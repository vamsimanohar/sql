/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.operator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.operator.SourceOperator;
import org.opensearch.sql.planner.distributed.page.Page;
import org.opensearch.sql.planner.distributed.page.PageBuilder;
import org.opensearch.sql.planner.distributed.split.Split;

/**
 * Source operator that reads documents directly from Lucene via {@link
 * IndexShard#acquireSearcher(String)}.
 *
 * <p>Uses Lucene's Weight/Scorer pattern to iterate only documents matching the filter query. When
 * no filter is provided, uses {@link MatchAllDocsQuery} to match all documents.
 *
 * <p>Reads {@code _source} JSON from stored fields and extracts requested field values.
 */
@Log4j2
public class LuceneScanOperator implements SourceOperator {

  private final IndexShard indexShard;
  private final List<String> fieldNames;
  private final int batchSize;
  private final OperatorContext context;
  private final Query luceneQuery;

  private Split split;
  private boolean noMoreSplits;
  private boolean finished;
  private Engine.Searcher engineSearcher;

  // Weight/Scorer state for filtered iteration
  private List<LeafReaderContext> leaves;
  private int currentLeafIndex;
  private StoredFields currentStoredFields;
  private Scorer currentScorer;
  private DocIdSetIterator currentDocIdIterator;
  private Bits currentLiveDocs;

  /**
   * Creates a LuceneScanOperator with a filter query merged into the scan.
   *
   * @param indexShard the shard to read from
   * @param fieldNames fields to extract from _source
   * @param batchSize rows per page batch
   * @param context operator context
   * @param luceneQuery the Lucene query for filtering (null means match all)
   */
  public LuceneScanOperator(
      IndexShard indexShard,
      List<String> fieldNames,
      int batchSize,
      OperatorContext context,
      Query luceneQuery) {
    this.indexShard = indexShard;
    this.fieldNames = fieldNames;
    this.batchSize = batchSize;
    this.context = context;
    this.luceneQuery = luceneQuery != null ? luceneQuery : new MatchAllDocsQuery();
    this.finished = false;
    this.currentLeafIndex = 0;
  }

  /** Backward-compatible constructor that matches all documents. */
  public LuceneScanOperator(
      IndexShard indexShard, List<String> fieldNames, int batchSize, OperatorContext context) {
    this(indexShard, fieldNames, batchSize, context, null);
  }

  @Override
  public void addSplit(Split split) {
    this.split = split;
  }

  @Override
  public void noMoreSplits() {
    this.noMoreSplits = true;
  }

  @Override
  public Page getOutput() {
    if (finished) {
      return null;
    }

    try {
      // Lazy initialization: acquire searcher and prepare Weight on first call
      if (engineSearcher == null) {
        engineSearcher = indexShard.acquireSearcher("distributed-pipeline");
        leaves = engineSearcher.getIndexReader().leaves();
        if (leaves.isEmpty()) {
          finished = true;
          return null;
        }
        advanceToLeaf(0);
      }

      PageBuilder builder = new PageBuilder(fieldNames.size());
      int rowsInBatch = 0;

      while (rowsInBatch < batchSize) {
        // Advance to next matching doc
        int docId = nextMatchingDoc();
        if (docId == DocIdSetIterator.NO_MORE_DOCS) {
          finished = true;
          return builder.isEmpty() ? null : builder.build();
        }

        // Read the document's _source
        org.apache.lucene.document.Document doc = currentStoredFields.document(docId);
        BytesRef sourceBytes = doc.getBinaryValue("_source");

        if (sourceBytes == null) {
          continue;
        }

        Map<String, Object> source =
            XContentHelper.convertToMap(new BytesArray(sourceBytes), false, XContentType.JSON).v2();

        builder.beginRow();
        for (int i = 0; i < fieldNames.size(); i++) {
          builder.setValue(i, getNestedValue(source, fieldNames.get(i)));
        }
        builder.endRow();
        rowsInBatch++;
      }

      return builder.isEmpty() ? null : builder.build();

    } catch (IOException e) {
      log.error("Error reading from Lucene shard: {}", indexShard.shardId(), e);
      finished = true;
      throw new RuntimeException("Failed to read from Lucene shard", e);
    }
  }

  /**
   * Returns the next matching live document ID using the Weight/Scorer pattern. Advances across
   * leaf readers (segments) as needed. Skips deleted/soft-deleted documents by checking the
   * segment's liveDocs bitset — Lucene's Scorer.iterator() does NOT filter deleted docs.
   */
  private int nextMatchingDoc() throws IOException {
    while (currentLeafIndex < leaves.size()) {
      if (currentDocIdIterator != null) {
        while (true) {
          int docId = currentDocIdIterator.nextDoc();
          if (docId == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          // Skip deleted/soft-deleted docs: liveDocs == null means all docs are live
          if (currentLiveDocs == null || currentLiveDocs.get(docId)) {
            return docId;
          }
        }
      }
      // Move to next leaf
      currentLeafIndex++;
      if (currentLeafIndex < leaves.size()) {
        advanceToLeaf(currentLeafIndex);
      }
    }
    return DocIdSetIterator.NO_MORE_DOCS;
  }

  /**
   * Advances to the specified leaf (segment) and creates a Scorer for it. The Scorer uses the
   * Lucene query to efficiently iterate only matching documents in that segment. Also captures the
   * segment's liveDocs bitset for filtering deleted/soft-deleted documents.
   */
  private void advanceToLeaf(int leafIndex) throws IOException {
    LeafReaderContext leafCtx = leaves.get(leafIndex);
    currentStoredFields = leafCtx.reader().storedFields();
    currentLiveDocs = leafCtx.reader().getLiveDocs();

    // Create Weight/Scorer for filtered iteration using the engine's IndexSearcher
    // (Engine.Searcher extends IndexSearcher with proper soft-delete handling)
    Query rewritten = engineSearcher.rewrite(luceneQuery);
    Weight weight = engineSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    currentScorer = weight.scorer(leafCtx);
    if (currentScorer != null) {
      currentDocIdIterator = currentScorer.iterator();
    } else {
      // No matching docs in this segment
      currentDocIdIterator = null;
    }
  }

  /**
   * Navigates a nested map using a dotted field path. For "machine.os1", navigates into
   * source["machine"]["os1"]. Handles arrays by extracting values from the first element. Falls
   * back to direct key lookup for non-dotted fields.
   */
  @SuppressWarnings("unchecked")
  private Object getNestedValue(Map<String, Object> source, String fieldName) {
    // Try direct key first (covers non-dotted names and flattened fields)
    Object direct = source.get(fieldName);
    if (direct != null) {
      return direct;
    }

    // Navigate dotted path: "machine.os1" → source["machine"]["os1"]
    if (fieldName.contains(".")) {
      String[] parts = fieldName.split("\\.");
      Object current = source;
      for (String part : parts) {
        if (current instanceof Map) {
          current = ((Map<String, Object>) current).get(part);
        } else if (current instanceof List<?> list) {
          // For array fields, extract from the first element
          if (!list.isEmpty() && list.get(0) instanceof Map) {
            current = ((Map<String, Object>) list.get(0)).get(part);
          } else {
            return null;
          }
        } else {
          return null;
        }
      }
      return current;
    }

    return null;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void finish() {
    finished = true;
  }

  @Override
  public OperatorContext getContext() {
    return context;
  }

  @Override
  public void close() {
    if (engineSearcher != null) {
      engineSearcher.close();
      engineSearcher = null;
    }
  }
}
