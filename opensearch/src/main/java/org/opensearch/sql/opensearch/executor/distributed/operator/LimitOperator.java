/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.operator;

import org.opensearch.sql.planner.distributed.operator.Operator;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.page.Page;

/**
 * Operator that limits the number of rows passing through the pipeline. Truncates pages when the
 * accumulated row count reaches the configured limit.
 */
public class LimitOperator implements Operator {

  private final int limit;
  private final OperatorContext context;

  private int accumulatedRows;
  private Page pendingOutput;
  private boolean inputFinished;

  public LimitOperator(int limit, OperatorContext context) {
    this.limit = limit;
    this.context = context;
    this.accumulatedRows = 0;
  }

  @Override
  public boolean needsInput() {
    return pendingOutput == null && accumulatedRows < limit && !inputFinished;
  }

  @Override
  public void addInput(Page page) {
    if (page == null || accumulatedRows >= limit) {
      return;
    }

    int remaining = limit - accumulatedRows;
    int pageRows = page.getPositionCount();

    if (pageRows <= remaining) {
      // Entire page fits within limit
      accumulatedRows += pageRows;
      pendingOutput = page;
    } else {
      // Truncate page to remaining rows
      pendingOutput = page.getRegion(0, remaining);
      accumulatedRows += remaining;
    }
  }

  @Override
  public Page getOutput() {
    Page output = pendingOutput;
    pendingOutput = null;
    return output;
  }

  @Override
  public boolean isFinished() {
    return accumulatedRows >= limit || (inputFinished && pendingOutput == null);
  }

  @Override
  public void finish() {
    inputFinished = true;
  }

  @Override
  public OperatorContext getContext() {
    return context;
  }

  @Override
  public void close() {
    // No resources to release
  }
}
