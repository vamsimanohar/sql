/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.planner;

import org.apache.calcite.rel.RelNode;

/**
 * Estimates the cost of executing a RelNode subtree. Used by the physical planner to make decisions
 * about stage boundaries, exchange types (broadcast vs. hash repartition), and operator placement.
 *
 * <p>Phase 5A defines the interface. Phase 5G implements it using Lucene statistics (doc count,
 * field cardinality, selectivity estimates).
 */
public interface CostEstimator {

  /**
   * Estimates the number of output rows for a RelNode.
   *
   * @param relNode the plan node to estimate
   * @return estimated row count
   */
  long estimateRowCount(RelNode relNode);

  /**
   * Estimates the output size in bytes for a RelNode.
   *
   * @param relNode the plan node to estimate
   * @return estimated size in bytes
   */
  long estimateSizeBytes(RelNode relNode);

  /**
   * Estimates the selectivity of a filter condition (0.0 to 1.0).
   *
   * @param relNode the filter node
   * @return selectivity ratio
   */
  double estimateSelectivity(RelNode relNode);
}
