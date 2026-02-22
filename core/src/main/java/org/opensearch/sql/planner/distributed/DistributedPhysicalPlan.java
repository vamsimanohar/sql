/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.planner.SerializablePlan;

/**
 * Represents a complete distributed execution plan consisting of multiple stages.
 *
 * <p>A distributed physical plan orchestrates the execution of a PPL query across multiple nodes in
 * the OpenSearch cluster. It provides:
 *
 * <ul>
 *   <li>Multi-stage execution coordination
 *   <li>Data locality optimization
 *   <li>Fault tolerance and progress tracking
 *   <li>Resource management across nodes
 * </ul>
 *
 * <p>Example execution flow:
 *
 * <pre>
 * DistributedPhysicalPlan:
 *   Stage 1 (SCAN): [WorkUnit-Shard1@Node1, WorkUnit-Shard2@Node2, ...]
 *       ↓
 *   Stage 2 (PROCESS): [WorkUnit-Agg@Node1, WorkUnit-Agg@Node2, ...]
 *       ↓
 *   Stage 3 (FINALIZE): [WorkUnit-FinalAgg@Coordinator]
 * </pre>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DistributedPhysicalPlan implements SerializablePlan {

  /** Unique identifier for this distributed plan */
  private String planId;

  /** Ordered list of execution stages */
  private List<ExecutionStage> executionStages;

  /** Expected output schema of the query */
  private Schema outputSchema;

  /** Total estimated execution cost */
  private double estimatedCost;

  /** Estimated memory requirement in bytes */
  private long estimatedMemoryBytes;

  /** Plan metadata and properties */
  private Map<String, Object> planMetadata;

  /** Current execution status of the plan */
  private PlanStatus status;

  /** Enumeration of distributed plan execution status. */
  public enum PlanStatus {
    /** Plan is created but not yet started */
    CREATED,

    /** Plan is currently executing */
    EXECUTING,

    /** Plan completed successfully */
    COMPLETED,

    /** Plan failed during execution */
    FAILED,

    /** Plan was cancelled */
    CANCELLED
  }

  /**
   * Creates a distributed physical plan with stages.
   *
   * @param planId Unique plan identifier
   * @param stages List of execution stages
   * @param outputSchema Expected output schema
   * @return Configured distributed plan
   */
  public static DistributedPhysicalPlan create(
      String planId, List<ExecutionStage> stages, Schema outputSchema) {

    double totalCost =
        stages.stream()
            .mapToDouble(
                stage ->
                    stage.getWorkUnits().stream()
                        .mapToDouble(
                            wu ->
                                wu.getTaskOperator()
                                    .estimateCost(
                                        wu.getDataPartition() != null
                                            ? wu.getDataPartition().getEstimatedSizeBytes()
                                            : 0))
                        .sum())
            .sum();

    long totalMemory = stages.stream().mapToLong(ExecutionStage::getEstimatedDataSize).sum();

    return new DistributedPhysicalPlan(
        planId,
        new ArrayList<>(stages),
        outputSchema,
        totalCost,
        totalMemory,
        Map.of(),
        PlanStatus.CREATED);
  }

  /**
   * Adds an execution stage to the plan.
   *
   * @param stage Execution stage to add
   */
  public void addStage(ExecutionStage stage) {
    if (executionStages == null) {
      executionStages = new ArrayList<>();
    }
    executionStages.add(stage);
  }

  /**
   * Gets the first stage that is ready to execute.
   *
   * @param completedStages Set of completed stage IDs
   * @return Next ready stage or null if none available
   */
  public ExecutionStage getNextReadyStage(Set<String> completedStages) {
    if (executionStages == null) {
      return null;
    }

    return executionStages.stream()
        .filter(stage -> stage.canExecute(completedStages))
        .findFirst()
        .orElse(null);
  }

  /**
   * Gets all stages that are ready to execute.
   *
   * @param completedStages Set of completed stage IDs
   * @return List of ready stages
   */
  public List<ExecutionStage> getReadyStages(Set<String> completedStages) {
    if (executionStages == null) {
      return List.of();
    }

    return executionStages.stream()
        .filter(stage -> stage.canExecute(completedStages))
        .collect(Collectors.toList());
  }

  /**
   * Gets a stage by its ID.
   *
   * @param stageId Stage identifier
   * @return Execution stage or null if not found
   */
  public ExecutionStage getStage(String stageId) {
    if (executionStages == null) {
      return null;
    }

    return executionStages.stream()
        .filter(stage -> stageId.equals(stage.getStageId()))
        .findFirst()
        .orElse(null);
  }

  /**
   * Gets all nodes involved in plan execution.
   *
   * @return Set of node IDs participating in this plan
   */
  public Set<String> getInvolvedNodes() {
    if (executionStages == null) {
      return Set.of();
    }

    return executionStages.stream()
        .flatMap(stage -> stage.getInvolvedNodes().stream())
        .collect(Collectors.toSet());
  }

  /**
   * Gets work units assigned to a specific node across all stages.
   *
   * @param nodeId Target node ID
   * @return List of work units assigned to the node
   */
  public List<WorkUnit> getWorkUnitsForNode(String nodeId) {
    if (executionStages == null) {
      return List.of();
    }

    return executionStages.stream()
        .flatMap(stage -> stage.getWorkUnitsForNode(nodeId).stream())
        .collect(Collectors.toList());
  }

  /**
   * Calculates overall plan execution progress.
   *
   * @param completedStages Set of completed stage IDs
   * @param completedWorkUnits Set of completed work unit IDs
   * @return Progress percentage (0.0 to 1.0)
   */
  public double getProgress(Set<String> completedStages, Set<String> completedWorkUnits) {
    if (executionStages == null || executionStages.isEmpty()) {
      return status == PlanStatus.COMPLETED ? 1.0 : 0.0;
    }

    double totalProgress =
        executionStages.stream()
            .mapToDouble(
                stage -> {
                  if (completedStages.contains(stage.getStageId())) {
                    return 1.0;
                  } else {
                    return stage.getProgress(completedWorkUnits);
                  }
                })
            .sum();

    return totalProgress / executionStages.size();
  }

  /**
   * Checks if the plan execution is complete.
   *
   * @param completedStages Set of completed stage IDs
   * @return true if all stages are completed, false otherwise
   */
  public boolean isComplete(Set<String> completedStages) {
    if (executionStages == null) {
      return true;
    }

    return executionStages.stream().allMatch(stage -> completedStages.contains(stage.getStageId()));
  }

  /** Marks the plan as executing. */
  public void markExecuting() {
    if (status == PlanStatus.CREATED) {
      status = PlanStatus.EXECUTING;
    }
  }

  /** Marks the plan as completed. */
  public void markCompleted() {
    if (status == PlanStatus.EXECUTING) {
      status = PlanStatus.COMPLETED;
    }
  }

  /**
   * Marks the plan as failed.
   *
   * @param error Error information
   */
  public void markFailed(String error) {
    status = PlanStatus.FAILED;
    if (planMetadata == null) {
      planMetadata = Map.of("error", error);
    } else {
      planMetadata.put("error", error);
    }
  }

  /**
   * Gets the final stage of the plan (typically FINALIZE type).
   *
   * @return Final execution stage or null if plan is empty
   */
  public ExecutionStage getFinalStage() {
    if (executionStages == null || executionStages.isEmpty()) {
      return null;
    }
    return executionStages.get(executionStages.size() - 1);
  }

  /**
   * Validates the plan structure and dependencies.
   *
   * @return List of validation errors (empty if valid)
   */
  public List<String> validate() {
    List<String> errors = new ArrayList<>();

    if (executionStages == null || executionStages.isEmpty()) {
      errors.add("Plan must contain at least one execution stage");
      return errors;
    }

    // Check for duplicate stage IDs
    Set<String> stageIds =
        executionStages.stream().map(ExecutionStage::getStageId).collect(Collectors.toSet());

    if (stageIds.size() != executionStages.size()) {
      errors.add("Plan contains duplicate stage IDs");
    }

    // Validate stage dependencies
    for (ExecutionStage stage : executionStages) {
      if (stage.getDependencyStages() != null) {
        for (String depStageId : stage.getDependencyStages()) {
          if (!stageIds.contains(depStageId)) {
            errors.add(
                "Stage " + stage.getStageId() + " depends on non-existent stage: " + depStageId);
          }
        }
      }
    }

    return errors;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DistributedPhysicalPlan{")
        .append("planId='")
        .append(planId)
        .append('\'')
        .append(", stages=")
        .append(executionStages != null ? executionStages.size() : 0)
        .append(", status=")
        .append(status)
        .append(", estimatedCost=")
        .append(estimatedCost)
        .append(", estimatedMemoryMB=")
        .append(estimatedMemoryBytes / (1024 * 1024))
        .append('}');
    return sb.toString();
  }

  /**
   * Implementation of Externalizable interface for serialization support. Required for cursor-based
   * pagination.
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(planId);
    out.writeObject(executionStages);
    out.writeObject(outputSchema);
    out.writeDouble(estimatedCost);
    out.writeLong(estimatedMemoryBytes);
    out.writeObject(planMetadata != null ? planMetadata : new HashMap<>());
    out.writeObject(status);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    planId = (String) in.readObject();
    executionStages = (List<ExecutionStage>) in.readObject();
    outputSchema = (Schema) in.readObject();
    estimatedCost = in.readDouble();
    estimatedMemoryBytes = in.readLong();
    planMetadata = (Map<String, Object>) in.readObject();
    status = (PlanStatus) in.readObject();
  }
}
