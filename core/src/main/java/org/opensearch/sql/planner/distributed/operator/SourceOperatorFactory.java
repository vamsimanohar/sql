/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.operator;

/**
 * Factory for creating {@link SourceOperator} instances. Source operator factories are used at the
 * beginning of a pipeline to create operators that read from external storage.
 */
public interface SourceOperatorFactory {

  /**
   * Creates a new source operator instance.
   *
   * @param context the runtime context for the operator
   * @return a new source operator instance
   */
  SourceOperator createOperator(OperatorContext context);

  /** Signals that no more operators will be created from this factory. */
  void noMoreOperators();
}
