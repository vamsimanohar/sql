/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner.physical;

/**
 * Base interface for physical operator nodes in the intermediate representation.
 *
 * <p>Physical operators are extracted from Calcite RelNode trees and represent the planned
 * operations before fragmentation into distributed stages. Each physical operator will later be
 * converted to one or more runtime operators during pipeline construction.
 */
public interface PhysicalOperatorNode {

  /** Returns the type of this physical operator for planning and fragmentation decisions. */
  PhysicalOperatorType getOperatorType();

  /** Returns a string representation of this operator's configuration. */
  String describe();
}
