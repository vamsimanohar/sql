/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.pipeline;

import java.util.concurrent.atomic.AtomicBoolean;

/** Runtime state for a pipeline execution. Tracks status and provides cancellation. */
public class PipelineContext {

  /** Pipeline execution status. */
  public enum Status {
    CREATED,
    RUNNING,
    FINISHED,
    FAILED,
    CANCELLED
  }

  private volatile Status status;
  private final AtomicBoolean cancelled;
  private volatile String failureMessage;

  public PipelineContext() {
    this.status = Status.CREATED;
    this.cancelled = new AtomicBoolean(false);
  }

  public Status getStatus() {
    return status;
  }

  public void setRunning() {
    this.status = Status.RUNNING;
  }

  public void setFinished() {
    this.status = Status.FINISHED;
  }

  public void setFailed(String message) {
    this.status = Status.FAILED;
    this.failureMessage = message;
  }

  public void setCancelled() {
    this.status = Status.CANCELLED;
    this.cancelled.set(true);
  }

  public boolean isCancelled() {
    return cancelled.get();
  }

  public String getFailureMessage() {
    return failureMessage;
  }
}
