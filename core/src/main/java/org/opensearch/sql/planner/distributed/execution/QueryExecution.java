/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.execution;

import java.util.List;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

/**
 * Represents the execution of a complete distributed query. Manages the lifecycle of all stage
 * executions and provides query-level statistics.
 */
public interface QueryExecution {

  /** Query execution states. */
  enum State {
    PLANNING,
    STARTING,
    RUNNING,
    FINISHING,
    FINISHED,
    FAILED
  }

  /** Returns the unique query identifier. */
  String getQueryId();

  /** Returns the staged execution plan. */
  StagedPlan getPlan();

  /** Returns the current execution state. */
  State getState();

  /** Returns all stage executions for this query. */
  List<StageExecution> getStageExecutions();

  /** Returns execution statistics for this query. */
  QueryStats getStats();

  /** Cancels the query and all its stage executions. */
  void cancel();

  /** Statistics for a query execution. */
  interface QueryStats {

    /** Returns the total number of output rows. */
    long getTotalRows();

    /** Returns the total elapsed execution time in milliseconds. */
    long getElapsedTimeMillis();

    /** Returns the time spent planning in milliseconds. */
    long getPlanningTimeMillis();
  }
}
