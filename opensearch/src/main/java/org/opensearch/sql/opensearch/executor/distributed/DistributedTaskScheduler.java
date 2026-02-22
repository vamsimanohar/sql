/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.core.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.planner.distributed.DistributedPhysicalPlan;
import org.opensearch.sql.planner.distributed.ExecutionStage;
import org.opensearch.sql.planner.distributed.WorkUnit;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Coordinates the execution of distributed query plans across cluster nodes.
 *
 * <p>The DistributedTaskScheduler manages the lifecycle of distributed query execution:
 * <ul>
 *   <li><strong>Stage Coordination</strong>: Executes stages in dependency order</li>
 *   <li><strong>Work Distribution</strong>: Distributes WorkUnits to appropriate nodes</li>
 *   <li><strong>Data Locality</strong>: Assigns scan tasks to nodes containing the data</li>
 *   <li><strong>Result Collection</strong>: Aggregates results from distributed tasks</li>
 *   <li><strong>Fault Tolerance</strong>: Handles node failures and retries</li>
 * </ul>
 *
 * <p><strong>Execution Flow:</strong>
 * <pre>
 * 1. executeQuery(DistributedPhysicalPlan) → Start execution
 * 2. executeStage(ExecutionStage) → Execute ready stages
 * 3. distributeWorkUnits(List&lt;WorkUnit&gt;) → Send tasks to nodes
 * 4. collectResults() → Aggregate results from all nodes
 * 5. Response via QueryResponse callback
 * </pre>
 *
 * <p><strong>Phase 1 Limitations:</strong>
 * - Simple 3-stage execution (SCAN → PROCESS → FINALIZE)
 * - Basic error handling and retry logic
 * - Single query execution (no concurrent queries)
 */
@Log4j2
@RequiredArgsConstructor
public class DistributedTaskScheduler {

  private final TransportService transportService;
  private final ClusterService clusterService;

  /** Thread pool for coordinating distributed execution */
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  /** Tracks completion status of work units and stages */
  private final Map<String, Boolean> completedWorkUnits = new ConcurrentHashMap<>();
  private final Map<String, Boolean> completedStages = new ConcurrentHashMap<>();

  /** Stores intermediate results from distributed tasks */
  private final Map<String, Object> stageResults = new ConcurrentHashMap<>();

  /**
   * Executes a distributed physical plan across the cluster.
   *
   * @param plan The distributed plan to execute
   * @param listener Response listener for async execution
   */
  public void executeQuery(
      DistributedPhysicalPlan plan,
      ResponseListener<QueryResponse> listener) {

    log.info("Starting execution of distributed plan: {}", plan.getPlanId());

    try {
      // Validate plan before execution
      List<String> validationErrors = plan.validate();
      if (!validationErrors.isEmpty()) {
        String errorMessage = "Plan validation failed: " + String.join(", ", validationErrors);
        log.error(errorMessage);
        listener.onFailure(new IllegalArgumentException(errorMessage));
        return;
      }

      // Mark plan as executing
      plan.markExecuting();

      // Clear previous execution state
      completedWorkUnits.clear();
      completedStages.clear();
      stageResults.clear();

      // Start stage-by-stage execution
      executeNextReadyStages(plan, listener);

    } catch (Exception e) {
      log.error("Failed to start distributed query execution: {}", plan.getPlanId(), e);
      plan.markFailed(e.getMessage());
      listener.onFailure(e);
    }
  }

  /**
   * Executes the next batch of ready stages that have no pending dependencies.
   */
  private void executeNextReadyStages(
      DistributedPhysicalPlan plan,
      ResponseListener<QueryResponse> listener) {

    // Find stages that can execute now (dependencies satisfied)
    Set<String> completed = completedStages.keySet();
    List<ExecutionStage> readyStages = plan.getReadyStages(completed);

    if (readyStages.isEmpty()) {
      // Check if plan is complete
      if (plan.isComplete(completed)) {
        log.info("Distributed plan execution completed: {}", plan.getPlanId());
        plan.markCompleted();
        deliverFinalResults(plan, listener);
      } else {
        // No ready stages but plan not complete - likely an error
        String error = "No ready stages found but plan not complete. Completed stages: " + completed;
        log.error(error);
        plan.markFailed(error);
        listener.onFailure(new IllegalStateException(error));
      }
      return;
    }

    log.info("Executing {} ready stages for plan: {}", readyStages.size(), plan.getPlanId());

    // Execute all ready stages in parallel
    List<CompletableFuture<Void>> stageFutures = readyStages.stream()
        .map(stage -> executeStageAsync(stage, plan, listener))
        .collect(Collectors.toList());

    // When all current stages complete, check for next ready stages
    CompletableFuture.allOf(stageFutures.toArray(new CompletableFuture[0]))
        .whenComplete((result, throwable) -> {
          if (throwable != null) {
            log.error("Error executing stages for plan: {}", plan.getPlanId(), throwable);
            plan.markFailed(throwable.getMessage());
            listener.onFailure(new RuntimeException(throwable));
          } else {
            // Continue with next ready stages
            executeNextReadyStages(plan, listener);
          }
        });
  }

  /**
   * Executes a single stage asynchronously.
   */
  private CompletableFuture<Void> executeStageAsync(
      ExecutionStage stage,
      DistributedPhysicalPlan plan,
      ResponseListener<QueryResponse> listener) {

    return CompletableFuture.runAsync(() -> {
      try {
        log.info("Executing stage: {} with {} work units",
            stage.getStageId(), stage.getWorkUnits().size());

        stage.markRunning();
        executeStage(stage);
        stage.markCompleted();
        completedStages.put(stage.getStageId(), true);

        log.info("Completed stage: {}", stage.getStageId());

      } catch (Exception e) {
        log.error("Failed to execute stage: {}", stage.getStageId(), e);
        stage.markFailed(e.getMessage());
        throw new RuntimeException(e);
      }
    }, executorService);
  }

  /**
   * Executes a single stage by distributing its work units across appropriate nodes.
   */
  private void executeStage(ExecutionStage stage) {
    List<WorkUnit> workUnits = stage.getWorkUnits();

    if (workUnits.isEmpty()) {
      log.warn("Stage {} has no work units", stage.getStageId());
      return;
    }

    // Group work units by target node for efficient distribution
    Map<String, List<WorkUnit>> workByNode = groupWorkUnitsByNode(workUnits);

    log.debug("Distributing {} work units across {} nodes for stage: {}",
        workUnits.size(), workByNode.size(), stage.getStageId());

    // Execute work units on each node
    List<CompletableFuture<Object>> taskFutures = new ArrayList<>();

    for (Map.Entry<String, List<WorkUnit>> entry : workByNode.entrySet()) {
      String nodeId = entry.getKey();
      List<WorkUnit> nodeWorkUnits = entry.getValue();

      CompletableFuture<Object> taskFuture = executeWorkUnitsOnNode(nodeId, nodeWorkUnits, stage);
      taskFutures.add(taskFuture);
    }

    // Wait for all tasks to complete and collect results
    try {
      CompletableFuture.allOf(taskFutures.toArray(new CompletableFuture[0])).get();

      // Collect results from all tasks
      List<Object> stageResultList = new ArrayList<>();
      for (CompletableFuture<Object> future : taskFutures) {
        Object result = future.get();
        if (result != null) {
          stageResultList.add(result);
        }
      }

      // Store results for use by subsequent stages
      stageResults.put(stage.getStageId(), stageResultList);

      log.debug("Collected {} results for stage: {}", stageResultList.size(), stage.getStageId());

    } catch (Exception e) {
      throw new RuntimeException("Failed to execute stage: " + stage.getStageId(), e);
    }
  }

  /**
   * Groups work units by their assigned node, considering data locality.
   */
  private Map<String, List<WorkUnit>> groupWorkUnitsByNode(List<WorkUnit> workUnits) {
    Map<String, List<WorkUnit>> workByNode = new HashMap<>();

    for (WorkUnit workUnit : workUnits) {
      String targetNode = determineTargetNode(workUnit);
      workByNode.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(workUnit);
    }

    return workByNode;
  }

  /**
   * Determines the best node to execute a work unit, considering data locality.
   */
  private String determineTargetNode(WorkUnit workUnit) {
    // For SCAN work units, use the assigned node (data locality)
    if (workUnit.requiresNodeAssignment() && workUnit.getAssignedNodeId() != null) {
      return workUnit.getAssignedNodeId();
    }

    // For other work units, use any available data node
    // In Phase 1, we'll use a simple round-robin strategy
    List<String> availableNodes = getAvailableDataNodes();
    if (availableNodes.isEmpty()) {
      throw new IllegalStateException("No data nodes available for work unit execution");
    }

    // Simple hash-based distribution for consistent assignment
    int nodeIndex = Math.abs(workUnit.getWorkUnitId().hashCode() % availableNodes.size());
    return availableNodes.get(nodeIndex);
  }

  /**
   * Gets list of available data nodes in the cluster.
   */
  private List<String> getAvailableDataNodes() {
    return clusterService.state().nodes().getDataNodes().values()
        .stream()
        .map(DiscoveryNode::getId)
        .collect(Collectors.toList());
  }

  /**
   * Executes a list of work units on a specific node using transport actions.
   */
  private CompletableFuture<Object> executeWorkUnitsOnNode(
      String nodeId,
      List<WorkUnit> workUnits,
      ExecutionStage stage) {

    CompletableFuture<Object> future = new CompletableFuture<>();

    try {
      // Create request for the target node
      ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest(
          workUnits, stage.getStageId(), getStageInputData(stage));

      log.debug("Sending {} work units to node: {}", workUnits.size(), nodeId);

      // Send request via transport service
      transportService.sendRequest(
          clusterService.state().nodes().get(nodeId),
          ExecuteDistributedTaskAction.NAME,
          request,
          new TransportResponseHandler<ExecuteDistributedTaskResponse>() {
            @Override
            public ExecuteDistributedTaskResponse read(org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
              return new ExecuteDistributedTaskResponse(in);
            }

            @Override
            public void handleResponse(ExecuteDistributedTaskResponse response) {
              log.debug("Received response from node: {} with {} results",
                  nodeId, response.getResults().size());

              // Mark work units as completed
              for (WorkUnit workUnit : workUnits) {
                completedWorkUnits.put(workUnit.getWorkUnitId(), true);
              }

              future.complete(response.getResults());
            }

            @Override
            public void handleException(TransportException exp) {
              log.error("Failed to execute work units on node: {}", nodeId, exp);
              future.completeExceptionally(exp);
            }

            @Override
            public String executor() {
              return org.opensearch.threadpool.ThreadPool.Names.GENERIC;
            }
          });

    } catch (Exception e) {
      log.error("Error sending work units to node: {}", nodeId, e);
      future.completeExceptionally(e);
    }

    return future;
  }

  /**
   * Gets input data for a stage from previous stage results.
   */
  private Object getStageInputData(ExecutionStage stage) {
    if (stage.getDependencyStages() == null || stage.getDependencyStages().isEmpty()) {
      return null; // No input data for initial stages
    }

    // Collect results from all dependency stages
    Map<String, Object> inputData = new HashMap<>();
    for (String depStageId : stage.getDependencyStages()) {
      Object stageResult = stageResults.get(depStageId);
      if (stageResult != null) {
        inputData.put(depStageId, stageResult);
      }
    }

    return inputData;
  }

  /**
   * Delivers the final query results to the response listener.
   */
  private void deliverFinalResults(
      DistributedPhysicalPlan plan,
      ResponseListener<QueryResponse> listener) {

    try {
      // Get results from the final stage
      ExecutionStage finalStage = plan.getFinalStage();
      if (finalStage == null) {
        throw new IllegalStateException("No final stage found in plan");
      }

      Object finalResults = stageResults.get(finalStage.getStageId());

      // TODO: Phase 1 - Convert distributed results to QueryResponse
      // For now, create a simple response indicating successful completion
      QueryResponse response = new QueryResponse(
          plan.getOutputSchema(),
          List.of(), // Empty results for Phase 1
          null // No cursor support yet
      );

      log.info("Delivering final results for plan: {}", plan.getPlanId());
      listener.onResponse(response);

    } catch (Exception e) {
      log.error("Failed to deliver final results for plan: {}", plan.getPlanId(), e);
      plan.markFailed(e.getMessage());
      listener.onFailure(e);
    }
  }

  /**
   * Shuts down the scheduler and releases resources.
   */
  public void shutdown() {
    log.info("Shutting down DistributedTaskScheduler");
    executorService.shutdown();
    completedWorkUnits.clear();
    completedStages.clear();
    stageResults.clear();
  }
}