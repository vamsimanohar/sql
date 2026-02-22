/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a stage in distributed query execution containing related work units.
 *
 * <p>Execution stages provide the framework for coordinating distributed query processing:
 *
 * <ul>
 *   <li>Group work units that can execute in parallel
 *   <li>Define dependencies between stages for proper ordering
 *   <li>Coordinate data exchange between stages
 *   <li>Track stage completion and progress
 * </ul>
 *
 * <p>Example multi-stage execution:
 *
 * <pre>
 * Stage 1 (SCAN): Parallel shard scanning across data nodes
 *    └─ WorkUnit per shard: Filter and project data
 *
 * Stage 2 (PROCESS): Partial aggregation per node
 *    └─ WorkUnit per node: GROUP BY with local aggregation
 *
 * Stage 3 (FINALIZE): Global aggregation on coordinator
 *    └─ Single WorkUnit: Merge partial results
 * </pre>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExecutionStage {

  /** Unique identifier for this execution stage */
  private String stageId;

  /** Type of stage indicating the primary operation */
  private StageType stageType;

  /** List of work units to be executed in this stage */
  private List<WorkUnit> workUnits;

  /** List of stage IDs that must complete before this stage can start */
  private List<String> dependencyStages;

  /** Current execution status of this stage */
  private StageStatus status;

  /** Configuration and metadata for the stage */
  private Map<String, Object> properties;

  /** Estimated parallelism level (number of concurrent work units) */
  private int estimatedParallelism;

  /** Data exchange strategy for collecting results from this stage */
  private DataExchangeType dataExchange;

  /** Enumeration of execution stage types in distributed processing. */
  public enum StageType {
    /** Initial stage: Direct data scanning from storage */
    SCAN,

    /** Intermediate stage: Data processing operations */
    PROCESS,

    /** Final stage: Result collection and finalization */
    FINALIZE
  }

  /** Enumeration of stage execution status. */
  public enum StageStatus {
    /** Stage is waiting for dependencies to complete */
    WAITING,

    /** Stage is ready to execute (dependencies satisfied) */
    READY,

    /** Stage is currently executing */
    RUNNING,

    /** Stage completed successfully */
    COMPLETED,

    /** Stage failed during execution */
    FAILED,

    /** Stage was cancelled */
    CANCELLED
  }

  /** Enumeration of data exchange strategies between stages. */
  public enum DataExchangeType {
    /** No data exchange - results remain on local nodes */
    NONE,

    /** Broadcast all results to all nodes */
    BROADCAST,

    /** Hash-based data redistribution for joins */
    HASH_REDISTRIBUTE,

    /** Collect all results to coordinator */
    GATHER
  }

  /**
   * Creates a scan stage for initial data access.
   *
   * @param stageId Unique stage identifier
   * @param workUnits List of scan work units
   * @return Configured scan stage
   */
  public static ExecutionStage createScanStage(String stageId, List<WorkUnit> workUnits) {
    return new ExecutionStage(
        stageId,
        StageType.SCAN,
        new ArrayList<>(workUnits),
        List.of(), // No dependencies for scan stage
        StageStatus.READY,
        Map.of(),
        workUnits.size(),
        DataExchangeType.NONE);
  }

  /**
   * Creates a processing stage for intermediate operations.
   *
   * @param stageId Unique stage identifier
   * @param workUnits List of processing work units
   * @param dependencyStages List of prerequisite stage IDs
   * @param dataExchange Data exchange strategy for this stage
   * @return Configured processing stage
   */
  public static ExecutionStage createProcessStage(
      String stageId,
      List<WorkUnit> workUnits,
      List<String> dependencyStages,
      DataExchangeType dataExchange) {
    return new ExecutionStage(
        stageId,
        StageType.PROCESS,
        new ArrayList<>(workUnits),
        new ArrayList<>(dependencyStages),
        StageStatus.WAITING,
        Map.of(),
        workUnits.size(),
        dataExchange);
  }

  /**
   * Creates a finalization stage for result collection.
   *
   * @param stageId Unique stage identifier
   * @param workUnit Single finalization work unit
   * @param dependencyStages List of prerequisite stage IDs
   * @return Configured finalization stage
   */
  public static ExecutionStage createFinalizeStage(
      String stageId, WorkUnit workUnit, List<String> dependencyStages) {
    return new ExecutionStage(
        stageId,
        StageType.FINALIZE,
        List.of(workUnit),
        new ArrayList<>(dependencyStages),
        StageStatus.WAITING,
        Map.of(),
        1, // Single work unit for finalization
        DataExchangeType.GATHER);
  }

  /**
   * Adds a work unit to this stage.
   *
   * @param workUnit Work unit to add
   */
  public void addWorkUnit(WorkUnit workUnit) {
    if (workUnits == null) {
      workUnits = new ArrayList<>();
    }
    workUnits.add(workUnit);
    estimatedParallelism = workUnits.size();
  }

  /**
   * Gets work units assigned to a specific node.
   *
   * @param nodeId Target node ID
   * @return List of work units assigned to the node
   */
  public List<WorkUnit> getWorkUnitsForNode(String nodeId) {
    if (workUnits == null) {
      return List.of();
    }
    return workUnits.stream()
        .filter(wu -> nodeId.equals(wu.getAssignedNodeId()))
        .collect(Collectors.toList());
  }

  /**
   * Gets all nodes involved in this stage execution.
   *
   * @return Set of node IDs participating in this stage
   */
  public Set<String> getInvolvedNodes() {
    if (workUnits == null) {
      return Set.of();
    }
    return workUnits.stream()
        .map(WorkUnit::getAssignedNodeId)
        .filter(nodeId -> nodeId != null)
        .collect(Collectors.toSet());
  }

  /**
   * Checks if this stage can execute (all dependencies satisfied).
   *
   * @param completedStages Set of completed stage IDs
   * @return true if stage can execute, false otherwise
   */
  public boolean canExecute(Set<String> completedStages) {
    return status == StageStatus.WAITING
        && (dependencyStages == null || completedStages.containsAll(dependencyStages));
  }

  /** Marks this stage as ready for execution. */
  public void markReady() {
    if (status == StageStatus.WAITING) {
      status = StageStatus.READY;
    }
  }

  /** Marks this stage as running. */
  public void markRunning() {
    if (status == StageStatus.READY) {
      status = StageStatus.RUNNING;
    }
  }

  /** Marks this stage as completed. */
  public void markCompleted() {
    if (status == StageStatus.RUNNING) {
      status = StageStatus.COMPLETED;
    }
  }

  /**
   * Marks this stage as failed.
   *
   * @param error Error information
   */
  public void markFailed(String error) {
    status = StageStatus.FAILED;
    if (properties == null) {
      properties = Map.of("error", error);
    } else {
      properties.put("error", error);
    }
  }

  /**
   * Gets the total estimated data size for this stage.
   *
   * @return Estimated data size in bytes
   */
  public long getEstimatedDataSize() {
    if (workUnits == null) {
      return 0;
    }
    return workUnits.stream()
        .mapToLong(
            wu -> {
              DataPartition partition = wu.getDataPartition();
              return partition != null ? partition.getEstimatedSizeBytes() : 0;
            })
        .sum();
  }

  /**
   * Calculates the stage completion progress.
   *
   * @param completedWorkUnits Set of completed work unit IDs
   * @return Completion percentage (0.0 to 1.0)
   */
  public double getProgress(Set<String> completedWorkUnits) {
    if (workUnits == null || workUnits.isEmpty()) {
      return status == StageStatus.COMPLETED ? 1.0 : 0.0;
    }

    long completedCount =
        workUnits.stream()
            .mapToLong(wu -> completedWorkUnits.contains(wu.getWorkUnitId()) ? 1 : 0)
            .sum();

    return (double) completedCount / workUnits.size();
  }
}
