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
 * IndexShard#acquireSearcher(String)}. Iterates all segments and documents, reading {@code _source}
 * JSON and extracting requested fields.
 *
 * <p>Phase 5B implementation: reads all fields from _source (no doc values optimization).
 */
@Log4j2
public class LuceneScanOperator implements SourceOperator {

  private final IndexShard indexShard;
  private final List<String> fieldNames;
  private final int batchSize;
  private final OperatorContext context;

  private Split split;
  private boolean noMoreSplits;
  private boolean finished;
  private Engine.Searcher engineSearcher;

  // Iteration state across segments
  private List<LeafReaderContext> leaves;
  private int currentLeafIndex;
  private int currentDocId;
  private int currentLeafMaxDoc;
  private StoredFields currentStoredFields;

  public LuceneScanOperator(
      IndexShard indexShard, List<String> fieldNames, int batchSize, OperatorContext context) {
    this.indexShard = indexShard;
    this.fieldNames = fieldNames;
    this.batchSize = batchSize;
    this.context = context;
    this.finished = false;
    this.currentLeafIndex = 0;
    this.currentDocId = 0;
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
      // Lazy initialization: acquire searcher on first getOutput()
      if (engineSearcher == null) {
        engineSearcher = indexShard.acquireSearcher("distributed-pipeline");
        leaves = engineSearcher.getIndexReader().leaves();
        if (!leaves.isEmpty()) {
          advanceToLeaf(0);
        } else {
          finished = true;
          return null;
        }
      }

      PageBuilder builder = new PageBuilder(fieldNames.size());
      int rowsInBatch = 0;

      while (rowsInBatch < batchSize) {
        // Check if we need to advance to the next leaf
        while (currentDocId >= currentLeafMaxDoc) {
          currentLeafIndex++;
          if (currentLeafIndex >= leaves.size()) {
            finished = true;
            return builder.isEmpty() ? null : builder.build();
          }
          advanceToLeaf(currentLeafIndex);
        }

        // Read the document's _source
        org.apache.lucene.document.Document doc = currentStoredFields.document(currentDocId);
        BytesRef sourceBytes = doc.getBinaryValue("_source");
        currentDocId++;

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

  private void advanceToLeaf(int leafIndex) throws IOException {
    LeafReaderContext leafCtx = leaves.get(leafIndex);
    currentStoredFields = leafCtx.reader().storedFields();
    currentDocId = 0;
    currentLeafMaxDoc = leafCtx.reader().maxDoc();
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
