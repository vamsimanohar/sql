/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner.physical;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rex.RexNode;

/**
 * Physical operator representing a filter (WHERE clause) operation.
 *
 * <p>Corresponds to Calcite LogicalFilter RelNode. Contains the filter condition as a RexNode that
 * can be analyzed and potentially pushed down to the scan operator.
 */
@Getter
@RequiredArgsConstructor
public class FilterPhysicalOperator implements PhysicalOperatorNode {

  /** Filter condition as Calcite RexNode. */
  private final RexNode condition;

  @Override
  public PhysicalOperatorType getOperatorType() {
    return PhysicalOperatorType.FILTER;
  }

  @Override
  public String describe() {
    return String.format("Filter(condition=%s)", condition.toString());
  }

  /** Returns true if this filter can be pushed down to Lucene (simple comparisons). */
  public boolean isPushdownCompatible() {
    // Phase 1B: Start with all filters being pushdown-compatible
    // Future phases can add more sophisticated analysis
    return true;
  }
}
