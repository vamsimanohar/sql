/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.split;

import java.util.List;

/**
 * Generates {@link Split}s for a source operator. Implementations discover available shards from
 * cluster state and create splits with preferred node information.
 */
public interface SplitSource {

  /**
   * Returns the next batch of splits, or an empty list if no more splits are available. Each split
   * represents a unit of work (typically one shard).
   *
   * @return list of splits
   */
  List<Split> getNextBatch();

  /** Returns true if all splits have been generated. */
  boolean isFinished();
}
