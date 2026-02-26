/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner.physical;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Physical operator representing a limit (row count restriction) operation.
 *
 * <p>Corresponds to Calcite LogicalSort RelNode with fetch clause only (no ordering). Contains the
 * maximum number of rows to return.
 */
@Getter
@RequiredArgsConstructor
public class LimitPhysicalOperator implements PhysicalOperatorNode {

  /** Maximum number of rows to return. */
  private final int limit;

  @Override
  public PhysicalOperatorType getOperatorType() {
    return PhysicalOperatorType.LIMIT;
  }

  @Override
  public String describe() {
    return String.format("Limit(rows=%d)", limit);
  }

  /**
   * Returns true if this limit can be pushed down to the scan operator. Phase 1B: Limits can be
   * pushed down for optimization.
   */
  public boolean isPushdownCompatible() {
    return true;
  }
}
