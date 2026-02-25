/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.planner;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.planner.distributed.stage.PartitioningScheme;

/**
 * A fragment of the query plan that executes within a single stage. Contains a sub-plan (Calcite
 * RelNode tree) that can be sent to data nodes for local execution, enabling query pushdown.
 *
 * <p>The {@code root} RelNode represents the computation to execute locally on each data node. For
 * example, a scan stage's SubPlan might contain: Filter → TableScan, allowing the data node to
 * apply the filter during scanning rather than sending all data to the coordinator.
 */
public class SubPlan {

  private final String fragmentId;
  private final RelNode root;
  private final PartitioningScheme outputPartitioning;
  private final List<SubPlan> children;

  public SubPlan(
      String fragmentId,
      RelNode root,
      PartitioningScheme outputPartitioning,
      List<SubPlan> children) {
    this.fragmentId = fragmentId;
    this.root = root;
    this.outputPartitioning = outputPartitioning;
    this.children = Collections.unmodifiableList(children);
  }

  /** Returns the unique identifier for this plan fragment. */
  public String getFragmentId() {
    return fragmentId;
  }

  /** Returns the root of the sub-plan RelNode tree for data node execution. */
  public RelNode getRoot() {
    return root;
  }

  /** Returns the output partitioning scheme for this fragment. */
  public PartitioningScheme getOutputPartitioning() {
    return outputPartitioning;
  }

  /** Returns child sub-plans that feed data into this fragment. */
  public List<SubPlan> getChildren() {
    return children;
  }

  @Override
  public String toString() {
    return "SubPlan{"
        + "id='"
        + fragmentId
        + "', partitioning="
        + outputPartitioning.getExchangeType()
        + ", children="
        + children.size()
        + '}';
  }
}
