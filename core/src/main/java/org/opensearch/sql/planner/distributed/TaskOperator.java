/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;

/**
 * Represents an operation that can be executed by a work unit on distributed data.
 *
 * <p>Task operators abstract the actual computation logic from the distribution framework:
 *
 * <ul>
 *   <li>Scan operators read data from storage (Lucene, Parquet, etc.)
 *   <li>Aggregation operators perform grouping and aggregation functions
 *   <li>Filter operators apply predicates to data
 *   <li>Join operators combine data from multiple sources
 * </ul>
 *
 * <p>Each operator encapsulates its configuration and can be serialized for transmission to remote
 * nodes during distributed execution.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public abstract class TaskOperator {

  /** Unique identifier for this operator */
  private String operatorId;

  /** Type of operation this operator performs */
  private OperatorType operatorType;

  /** Configuration parameters for the operator */
  private Map<String, Object> config;

  /** Enumeration of task operator types supported in distributed execution. */
  public enum OperatorType {
    /** Scan data from storage (Lucene indexes, Parquet files, etc.) */
    SCAN,

    /** Apply filter predicates to data */
    FILTER,

    /** Project specific columns/expressions */
    PROJECT,

    /** Perform partial aggregation (GROUP BY with aggregate functions) */
    PARTIAL_AGGREGATE,

    /** Merge partial aggregation results into final results */
    FINAL_AGGREGATE,

    /** Sort data by specified criteria */
    SORT,

    /** Limit result set size */
    LIMIT,

    /** Join data from multiple sources */
    JOIN,

    /** Evaluate expressions and add computed columns */
    EVAL,

    /** Remove duplicate records */
    DEDUPE,

    /** Apply window functions */
    WINDOW,

    /** Finalize results and collect on coordinator */
    FINALIZE
  }

  /**
   * Executes this operator with the given input context and produces results.
   *
   * @param context Execution context containing runtime information
   * @param input Input data for processing (null for scan operators)
   * @return Task result containing output data and metadata
   */
  public abstract TaskResult execute(TaskContext context, TaskInput input);

  /**
   * Returns the output schema produced by this operator.
   *
   * @return Map of column names to their data types
   */
  public abstract Map<String, ExprType> getOutputSchema();

  /**
   * Estimates the computational cost of executing this operator. Used for query optimization and
   * scheduling decisions.
   *
   * @param inputSize Estimated input data size in bytes
   * @return Estimated computational cost (arbitrary units)
   */
  public abstract double estimateCost(long inputSize);

  /** Input data and configuration for task operator execution. */
  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class TaskInput {
    /** Input data records (null for scan operators) */
    private Iterable<ExprValue> inputData;

    /** Filter conditions to apply during processing */
    private Map<String, Object> filterConditions;

    /** Column projections to include in output */
    private Map<String, String> projections;

    /** Grouping keys for aggregation operations */
    private Map<String, Object> groupingKeys;

    /** Aggregate functions to compute */
    private Map<String, String> aggregateFunctions;

    /** Sort criteria for ordering operations */
    private Map<String, String> sortCriteria;

    /** Limit for result size */
    private Integer limit;

    /** Additional operator-specific parameters */
    private Map<String, Object> parameters;
  }

  /** Output data and metadata from task operator execution. */
  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class TaskResult {
    /** Output data records */
    private Iterable<ExprValue> outputData;

    /** Number of output records */
    private long recordCount;

    /** Size of output data in bytes */
    private long dataSize;

    /** Execution statistics and metadata */
    private Map<String, Object> statistics;

    /** Whether this is partial results (for multi-stage aggregation) */
    private boolean isPartialResult;
  }

  /** Execution context providing runtime information to task operators. */
  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class TaskContext {
    /** Current node ID where this operator is executing */
    private String nodeId;

    /** Storage engine specific context (Lucene searcher, file system, etc.) */
    private Object storageContext;

    /** Query execution timeout */
    private long timeoutMs;

    /** Memory limit for this operation */
    private long memoryLimitBytes;

    /** Additional runtime properties */
    private Map<String, Object> properties;
  }
}
