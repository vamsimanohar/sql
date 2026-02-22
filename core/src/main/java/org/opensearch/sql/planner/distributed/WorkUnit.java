/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Represents a unit of parallelizable work that can be distributed across cluster nodes.
 * WorkUnits are the fundamental building blocks of distributed query execution.
 *
 * <p>Each WorkUnit contains:
 * <ul>
 *   <li>Unique identifier for tracking and coordination</li>
 *   <li>Work type indicating the operation (SCAN, PROCESS, FINALIZE)</li>
 *   <li>Data partition information specifying what data to process</li>
 *   <li>Task operator defining how to process the data</li>
 *   <li>Dependencies on other work units for ordering</li>
 *   <li>Target node assignment for data locality optimization</li>
 * </ul>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class WorkUnit {

  /** Unique identifier for this work unit */
  @EqualsAndHashCode.Include
  private String workUnitId;

  /** Type of work this unit performs */
  private WorkUnitType type;

  /** Information about the data partition this work unit processes */
  private DataPartition dataPartition;

  /** The operator that defines how to process the data */
  private TaskOperator taskOperator;

  /** List of work unit IDs that must complete before this one can start */
  private List<String> dependencies;

  /** Target node ID where this work unit should be executed (for data locality) */
  private String assignedNodeId;

  /** Additional properties for work unit execution */
  private Map<String, Object> properties;

  /**
   * Enumeration of work unit types in distributed execution.
   */
  public enum WorkUnitType {
    /**
     * Stage 1: Direct data scanning from storage (Lucene shards, Parquet files, etc.)
     * Assigned to nodes containing the target data for optimal locality.
     */
    SCAN,

    /**
     * Stage 2+: Intermediate processing operations (aggregation, filtering, joining)
     * Can be distributed across any available nodes in the cluster.
     */
    PROCESS,

    /**
     * Final stage: Global operations requiring all intermediate results
     * Typically executed on the coordinator node for result collection.
     */
    FINALIZE
  }

  /**
   * Convenience constructor for creating a scan work unit.
   *
   * @param workUnitId Unique identifier
   * @param dataPartition Data partition to scan
   * @param taskOperator Task operator for scanning
   * @param assignedNodeId Node containing the data
   * @return Configured scan work unit
   */
  public static WorkUnit createScanUnit(
      String workUnitId,
      DataPartition dataPartition,
      TaskOperator taskOperator,
      String assignedNodeId) {
    return new WorkUnit(
        workUnitId,
        WorkUnitType.SCAN,
        dataPartition,
        taskOperator,
        List.of(), // No dependencies for scan units
        assignedNodeId,
        Map.of());
  }

  /**
   * Convenience constructor for creating a process work unit.
   *
   * @param workUnitId Unique identifier
   * @param taskOperator Task operator for processing
   * @param dependencies List of prerequisite work unit IDs
   * @return Configured process work unit
   */
  public static WorkUnit createProcessUnit(
      String workUnitId,
      TaskOperator taskOperator,
      List<String> dependencies) {
    return new WorkUnit(
        workUnitId,
        WorkUnitType.PROCESS,
        null, // No specific data partition for processing
        taskOperator,
        dependencies,
        null, // Node assignment determined by scheduler
        Map.of());
  }

  /**
   * Convenience constructor for creating a finalize work unit.
   *
   * @param workUnitId Unique identifier
   * @param taskOperator Task operator for finalization
   * @param dependencies List of prerequisite work unit IDs
   * @return Configured finalize work unit
   */
  public static WorkUnit createFinalizeUnit(
      String workUnitId,
      TaskOperator taskOperator,
      List<String> dependencies) {
    return new WorkUnit(
        workUnitId,
        WorkUnitType.FINALIZE,
        null,
        taskOperator,
        dependencies,
        null, // Typically executed on coordinator
        Map.of());
  }

  /**
   * Checks if this work unit can be executed (all dependencies satisfied).
   *
   * @param completedWorkUnits Set of completed work unit IDs
   * @return true if all dependencies are satisfied, false otherwise
   */
  public boolean canExecute(List<String> completedWorkUnits) {
    return completedWorkUnits.containsAll(dependencies);
  }

  /**
   * Returns whether this work unit requires specific node assignment.
   * SCAN units typically require specific nodes for data locality.
   *
   * @return true if node assignment is required, false otherwise
   */
  public boolean requiresNodeAssignment() {
    return type == WorkUnitType.SCAN && assignedNodeId != null;
  }
}