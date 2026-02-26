/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner.physical;

/**
 * Types of physical operators in the intermediate representation.
 *
 * <p>Used by the fragmenter to make intelligent decisions about stage boundaries and operator
 * placement.
 */
public enum PhysicalOperatorType {

  /** Table scan operator - reads from storage. */
  SCAN,

  /** Filter operator - applies predicates to rows. */
  FILTER,

  /** Projection operator - selects and transforms columns. */
  PROJECTION,

  /** Limit operator - limits number of rows. */
  LIMIT,

  /** Future: Aggregation operators. */
  // AGGREGATION,

  /** Future: Join operators. */
  // JOIN,

  /** Future: Sort operators. */
  // SORT,

  /** Future: Window function operators. */
  // WINDOW
}
