/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.execution;

import java.util.List;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;

/**
 * Represents the execution of a single task within a stage. Each task processes a subset of data
 * units on a specific node.
 */
public interface TaskExecution {

  /** Task execution states. */
  enum State {
    PLANNED,
    RUNNING,
    FLUSHING,
    FINISHED,
    FAILED,
    CANCELLED
  }

  /** Returns the unique identifier for this task. */
  String getTaskId();

  /** Returns the node ID where this task is executing. */
  String getNodeId();

  /** Returns the current execution state. */
  State getState();

  /** Returns the data units assigned to this task. */
  List<DataUnit> getAssignedDataUnits();

  /** Returns execution statistics for this task. */
  TaskStats getStats();

  /** Cancels this task. */
  void cancel();

  /** Statistics for a task execution. */
  interface TaskStats {

    /** Returns the number of rows processed by this task. */
    long getProcessedRows();

    /** Returns the number of bytes processed by this task. */
    long getProcessedBytes();

    /** Returns the number of output rows produced by this task. */
    long getOutputRows();

    /** Returns the elapsed execution time in milliseconds. */
    long getElapsedTimeMillis();
  }
}
