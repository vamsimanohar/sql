/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.planner;

import java.util.List;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitSource;

/**
 * Provides context to the {@link PlanFragmenter} during plan fragmentation. Supplies information
 * about cluster topology, cost estimates, and data unit discovery needed to make fragmentation
 * decisions (e.g., broadcast vs. hash repartition, stage parallelism).
 */
public interface FragmentationContext {

  /** Returns the list of available data node IDs in the cluster. */
  List<String> getAvailableNodes();

  /** Returns the cost estimator for sizing stages and choosing exchange types. */
  CostEstimator getCostEstimator();

  /**
   * Returns a data unit source for the given table name. Used to discover shards and their
   * locations during fragmentation.
   *
   * @param tableName the table (index) name
   * @return the data unit source for shard discovery
   */
  DataUnitSource getDataUnitSource(String tableName);

  /** Returns the maximum number of tasks per stage (limits parallelism). */
  int getMaxTasksPerStage();

  /** Returns the node ID of the coordinator node. */
  String getCoordinatorNodeId();
}
