/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.operator;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runtime context available to operators during execution. Provides access to memory limits,
 * cancellation, and operator identity.
 */
public class OperatorContext {

  private final String operatorId;
  private final String stageId;
  private final long memoryLimitBytes;
  private final AtomicBoolean cancelled;

  public OperatorContext(String operatorId, String stageId, long memoryLimitBytes) {
    this.operatorId = operatorId;
    this.stageId = stageId;
    this.memoryLimitBytes = memoryLimitBytes;
    this.cancelled = new AtomicBoolean(false);
  }

  /** Returns the unique identifier for this operator instance. */
  public String getOperatorId() {
    return operatorId;
  }

  /** Returns the stage ID this operator belongs to. */
  public String getStageId() {
    return stageId;
  }

  /** Returns the memory limit in bytes for this operator. */
  public long getMemoryLimitBytes() {
    return memoryLimitBytes;
  }

  /** Returns true if the query has been cancelled. */
  public boolean isCancelled() {
    return cancelled.get();
  }

  /** Requests cancellation of the query. */
  public void cancel() {
    cancelled.set(true);
  }

  /** Creates a default context for testing. */
  public static OperatorContext createDefault(String operatorId) {
    return new OperatorContext(operatorId, "default-stage", Long.MAX_VALUE);
  }
}
