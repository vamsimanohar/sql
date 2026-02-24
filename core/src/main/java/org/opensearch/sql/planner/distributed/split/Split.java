/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.split;

import java.util.Collections;
import java.util.List;

/**
 * A unit of work (shard assignment) given to a SourceOperator. Each split represents a portion of
 * data to read — typically one OpenSearch shard. Includes preferred nodes for data locality and
 * estimated size for load balancing.
 */
public class Split {

  private final String indexName;
  private final int shardId;
  private final List<String> preferredNodes;
  private final long estimatedRows;

  public Split(String indexName, int shardId, List<String> preferredNodes, long estimatedRows) {
    this.indexName = indexName;
    this.shardId = shardId;
    this.preferredNodes = Collections.unmodifiableList(preferredNodes);
    this.estimatedRows = estimatedRows;
  }

  /** Returns the index name this split reads from. */
  public String getIndexName() {
    return indexName;
  }

  /** Returns the shard ID within the index. */
  public int getShardId() {
    return shardId;
  }

  /**
   * Returns the preferred nodes for this split (primary + replicas). Used for data locality and
   * load balancing.
   */
  public List<String> getPreferredNodes() {
    return preferredNodes;
  }

  /** Returns the estimated number of rows in this split. */
  public long getEstimatedRows() {
    return estimatedRows;
  }

  @Override
  public String toString() {
    return "Split{"
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
