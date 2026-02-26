/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner.physical;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Physical operator representing a projection (field selection) operation.
 *
 * <p>Corresponds to Calcite LogicalProject RelNode. Contains the list of fields to project from the
 * input rows.
 */
@Getter
@RequiredArgsConstructor
public class ProjectionPhysicalOperator implements PhysicalOperatorNode {

  /** Field names to project from input rows. */
  private final List<String> projectedFields;

  @Override
  public PhysicalOperatorType getOperatorType() {
    return PhysicalOperatorType.PROJECTION;
  }

  @Override
  public String describe() {
    return String.format("Project(fields=[%s])", String.join(", ", projectedFields));
  }

  /**
   * Returns true if this projection can be pushed down to the scan operator. Phase 1B: Simple field
   * selection can be pushed down.
   */
  public boolean isPushdownCompatible() {
    // Phase 1B: All projections are simple field selections
    return true;
  }
}
