/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner.physical;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Intermediate representation of physical operators extracted from a Calcite RelNode tree. Used by
 * the physical planner to understand query structure before fragmenting into stages.
 *
 * <p>Contains the index name and ordered list of physical operators that represent the query
 * execution plan.
 */
@Getter
@RequiredArgsConstructor
public class PhysicalOperatorTree {

  /** Target index name for the query. */
  private final String indexName;

  /** Ordered list of physical operators representing the query plan. */
  private final List<PhysicalOperatorNode> operators;

  /** Returns the scan operator (should be first in the list for Phase 1B queries). */
  public ScanPhysicalOperator getScanOperator() {
    return operators.stream()
        .filter(op -> op instanceof ScanPhysicalOperator)
        .map(op -> (ScanPhysicalOperator) op)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No scan operator found in operator tree"));
  }

  /** Returns filter operators that can be pushed down to the scan. */
  public List<FilterPhysicalOperator> getFilterOperators() {
    return operators.stream()
        .filter(op -> op instanceof FilterPhysicalOperator)
        .map(op -> (FilterPhysicalOperator) op)
        .collect(Collectors.toList());
  }

  /** Returns projection operators. */
  public List<ProjectionPhysicalOperator> getProjectionOperators() {
    return operators.stream()
        .filter(op -> op instanceof ProjectionPhysicalOperator)
        .map(op -> (ProjectionPhysicalOperator) op)
        .collect(Collectors.toList());
  }

  /** Returns limit operators. */
  public List<LimitPhysicalOperator> getLimitOperators() {
    return operators.stream()
        .filter(op -> op instanceof LimitPhysicalOperator)
        .map(op -> (LimitPhysicalOperator) op)
        .collect(Collectors.toList());
  }

  /** Checks if this query can be executed in a single stage (scan + compatible operations). */
  public boolean isSingleStageCompatible() {
    // Phase 1B: All current operators can be combined in the leaf stage
    return operators.stream()
        .allMatch(
            op ->
                op instanceof ScanPhysicalOperator
                    || op instanceof FilterPhysicalOperator
                    || op instanceof ProjectionPhysicalOperator
                    || op instanceof LimitPhysicalOperator);
  }

  @Override
  public String toString() {
    return String.format(
        "PhysicalOperatorTree{index='%s', operators=[%s]}",
        indexName,
        operators.stream()
            .map(op -> op.getClass().getSimpleName())
            .collect(Collectors.joining(", ")));
  }
}
