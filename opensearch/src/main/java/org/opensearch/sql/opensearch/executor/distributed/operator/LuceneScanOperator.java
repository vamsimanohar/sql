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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
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
          builder.setValue(i, source.get(fieldNames.get(i)));
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
   * Returns the next matching document ID using the Weight/Scorer pattern. Advances across leaf
   * readers (segments) as needed.
   */
  private int nextMatchingDoc() throws IOException {
    while (currentLeafIndex < leaves.size()) {
      if (currentDocIdIterator != null) {
        int docId = currentDocIdIterator.nextDoc();
        if (docId != DocIdSetIterator.NO_MORE_DOCS) {
          return docId;
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
   * Lucene query to efficiently iterate only matching documents in that segment.
   */
  private void advanceToLeaf(int leafIndex) throws IOException {
    LeafReaderContext leafCtx = leaves.get(leafIndex);
    currentStoredFields = leafCtx.reader().storedFields();

    // Create Weight/Scorer for filtered iteration
    IndexSearcher indexSearcher = new IndexSearcher(engineSearcher.getIndexReader());
    Query rewritten = indexSearcher.rewrite(luceneQuery);
    Weight weight = indexSearcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);

    currentScorer = weight.scorer(leafCtx);
    if (currentScorer != null) {
      currentDocIdIterator = currentScorer.iterator();
    } else {
      // No matching docs in this segment
      currentDocIdIterator = null;
    }
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
