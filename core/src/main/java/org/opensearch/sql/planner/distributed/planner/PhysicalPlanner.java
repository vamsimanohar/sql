/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.planner;

import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

/**
 * Converts a Calcite logical plan (RelNode) into a distributed execution plan (StagedPlan).
 * Implementations walk the RelNode tree, decide stage boundaries (where exchanges go), and build
 * operator pipelines for each stage.
 */
public interface PhysicalPlanner {

  /**
   * Plans a Calcite RelNode tree into a distributed StagedPlan.
   *
   * @param relNode the optimized Calcite logical plan
   * @return the distributed execution plan
   */
  StagedPlan plan(RelNode relNode);
}
