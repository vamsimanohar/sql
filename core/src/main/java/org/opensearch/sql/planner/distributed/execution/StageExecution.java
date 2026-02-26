/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.execution;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;

/**
 * Manages the execution of a single compute stage across multiple nodes. Tracks task executions,
 * handles data unit assignment, and monitors stage completion.
 */
public interface StageExecution {

  /** Stage execution states. */
  enum State {
    PLANNED,
    SCHEDULING,
    RUNNING,
    FINISHED,
    FAILED,
    CANCELLED
  }

  /** Returns the compute stage being executed. */
  ComputeStage getStage();

  /** Returns the current execution state. */
  State getState();

  /**
   * Adds data units to be processed by this stage.
   *
   * @param dataUnits the data units to add
   */
  void addDataUnits(List<DataUnit> dataUnits);

  /** Signals that no more data units will be added to this stage. */
  void noMoreDataUnits();

  /**
   * Returns task executions grouped by node ID.
   *
   * @return map from node ID to list of task executions on that node
   */
  Map<String, List<TaskExecution>> getTaskExecutions();

  /** Returns execution statistics for this stage. */
  StageStats getStats();

  /** Cancels all tasks in this stage. */
  void cancel();

  /**
   * Adds a listener to be notified when the stage state changes.
   *
   * @param listener the state change listener
   */
  void addStateChangeListener(StateChangeListener listener);

  /** Listener for stage state changes. */
  @FunctionalInterface
  interface StateChangeListener {

    /**
     * Called when the stage transitions to a new state.
     *
     * @param newState the new state
     */
    void onStateChange(State newState);
  }

  /** Statistics for a stage execution. */
  interface StageStats {

    /** Returns the total number of rows processed across all tasks. */
    long getTotalRows();

    /** Returns the total number of bytes processed across all tasks. */
    long getTotalBytes();

    /** Returns the number of completed tasks. */
    int getCompletedTasks();

    /** Returns the total number of tasks. */
    int getTotalTasks();
  }
}
