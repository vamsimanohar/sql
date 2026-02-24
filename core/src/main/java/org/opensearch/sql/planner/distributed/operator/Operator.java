/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.operator;

import org.opensearch.sql.planner.distributed.page.Page;

/**
 * Core operator interface using a push/pull model. Operators form a pipeline where data flows as
 * {@link Page} batches. Each operator declares whether it needs input ({@link #needsInput()}),
 * accepts input ({@link #addInput(Page)}), and produces output ({@link #getOutput()}).
 *
 * <p>Lifecycle:
 *
 * <ol>
 *   <li>Pipeline driver calls {@link #needsInput()} to check readiness
 *   <li>If ready, driver calls {@link #addInput(Page)} with upstream output
 *   <li>Driver calls {@link #getOutput()} to pull processed results
 *   <li>When upstream is done, driver calls {@link #finish()} to signal no more input
 *   <li>Operator produces remaining buffered output via {@link #getOutput()}
 *   <li>When {@link #isFinished()} returns true, operator is done
 *   <li>Driver calls {@link #close()} to release resources
 * </ol>
 */
public interface Operator extends AutoCloseable {

  /** Returns true if this operator is ready to accept input via {@link #addInput(Page)}. */
  boolean needsInput();

  /**
   * Provides a page of input data to this operator.
   *
   * @param page the input page (must not be null)
   * @throws IllegalStateException if {@link #needsInput()} returns false
   */
  void addInput(Page page);

  /**
   * Returns the next page of output, or null if no output is available yet. A null return does not
   * mean the operator is finished — call {@link #isFinished()} to check.
   */
  Page getOutput();

  /** Returns true if this operator has completed all processing and will produce no more output. */
  boolean isFinished();

  /** Signals that no more input will be provided. The operator should flush buffered results. */
  void finish();

  /** Returns the runtime context for this operator. */
  OperatorContext getContext();
}
