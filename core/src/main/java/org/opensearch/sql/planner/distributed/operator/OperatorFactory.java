/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.operator;

/**
 * Factory for creating {@link Operator} instances. Each factory creates operators for a specific
 * pipeline position (e.g., filter, project, aggregation). The pipeline uses factories so that
 * multiple operator instances can be created for parallel execution.
 */
public interface OperatorFactory {

  /**
   * Creates a new operator instance.
   *
   * @param context the runtime context for the operator
   * @return a new operator instance
   */
  Operator createOperator(OperatorContext context);

  /** Signals that no more operators will be created from this factory. */
  void noMoreOperators();
}
