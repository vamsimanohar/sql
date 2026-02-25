/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.split;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.planner.distributed.split.DataUnit;

/**
 * An OpenSearch-specific data unit representing a single shard of an index. Requires local Lucene
 * access (not remotely accessible) because the LuceneScanOperator reads directly from the shard's
 * IndexShard via {@code acquireSearcher}.
 */
public class OpenSearchDataUnit extends DataUnit {

  private final String indexName;
  private final int shardId;
  private final List<String> preferredNodes;
  private final long estimatedRows;
  private final long estimatedSizeBytes;

  public OpenSearchDataUnit(
      String indexName,
      int shardId,
      List<String> preferredNodes,
      long estimatedRows,
      long estimatedSizeBytes) {
    this.indexName = indexName;
    this.shardId = shardId;
    this.preferredNodes = Collections.unmodifiableList(preferredNodes);
    this.estimatedRows = estimatedRows;
    this.estimatedSizeBytes = estimatedSizeBytes;
  }

  @Override
  public String getDataUnitId() {
    return indexName + "/" + shardId;
  }

  @Override
  public List<String> getPreferredNodes() {
    return preferredNodes;
  }

  @Override
  public long getEstimatedRows() {
    return estimatedRows;
  }

  @Override
  public long getEstimatedSizeBytes() {
    return estimatedSizeBytes;
  }

  @Override
  public Map<String, String> getProperties() {
    return Map.of("indexName", indexName, "shardId", String.valueOf(shardId));
  }

  /**
   * OpenSearch shard data units require local Lucene access — they cannot be read remotely.
   *
   * @return false
   */
  @Override
  public boolean isRemotelyAccessible() {
    return false;
  }

  /** Returns the index name this data unit reads from. */
  public String getIndexName() {
    return indexName;
  }

  /** Returns the shard ID within the index. */
  public int getShardId() {
    return shardId;
  }

  @Override
  public String toString() {
    return "OpenSearchDataUnit{"
        + "index='"
        + indexName
        + "', shard="
        + shardId
        + ", nodes="
        + preferredNodes
        + ", ~rows="
        + estimatedRows
        + '}';
  }
}
