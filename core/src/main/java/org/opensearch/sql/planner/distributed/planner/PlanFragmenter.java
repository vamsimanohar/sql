/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.planner;

import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

/**
 * Fragments an optimized Calcite RelNode tree into a multi-stage distributed execution plan. Walks
 * the RelNode tree, identifies stage boundaries (where exchanges are needed), and creates {@link
 * SubPlan} fragments for each stage. Replaces the manual stage creation in the old {@code
 * DistributedQueryPlanner}.
 *
 * <p>Stage boundaries are inserted at:
 *
 * <ul>
 *   <li>Table scans (leaf stages)
 *   <li>Aggregations requiring repartition (hash exchange)
 *   <li>Joins requiring repartition or broadcast
 *   <li>Sort requiring gather exchange
 * </ul>
 */
public interface PlanFragmenter {

  /**
   * Fragments an optimized RelNode tree into a staged execution plan.
   *
   * @param optimizedPlan the Calcite-optimized RelNode tree
   * @param context fragmentation context providing cluster topology and cost estimates
   * @return the staged distributed execution plan
   */
  StagedPlan fragment(RelNode optimizedPlan, FragmentationContext context);
}
