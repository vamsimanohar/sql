/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.planner.distributed.TaskOperator;
import org.opensearch.sql.planner.distributed.WorkUnit;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action handler for executing distributed query tasks on data nodes.
 *
 * <p>This handler runs on each cluster node and processes ExecuteDistributedTaskRequest messages
 * from the coordinator. It executes the received WorkUnits locally and returns results via
 * ExecuteDistributedTaskResponse.
 *
 * <p><strong>Execution Process:</strong>
 *
 * <ol>
 *   <li>Receive WorkUnits from coordinator node
 *   <li>Execute each WorkUnit using its TaskOperator
 *   <li>Collect results and execution statistics
 *   <li>Return aggregated results to coordinator
 * </ol>
 *
 * <p><strong>Phase 1 Implementation:</strong> - Basic WorkUnit execution framework - Simple error
 * handling and logging - Placeholder task operator execution
 */
@Log4j2
public class TransportExecuteDistributedTaskAction
    extends HandledTransportAction<ExecuteDistributedTaskRequest, ExecuteDistributedTaskResponse> {

  public static final String NAME = "cluster:admin/opensearch/sql/distributed/execute";

  private final ClusterService clusterService;
  private final Client client;

  @Inject
  public TransportExecuteDistributedTaskAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      Client client) {
    super(
        ExecuteDistributedTaskAction.NAME,
        transportService,
        actionFilters,
        ExecuteDistributedTaskRequest::new);
    this.clusterService = clusterService;
    this.client = client;
  }

  @Override
  protected void doExecute(
      Task task,
      ExecuteDistributedTaskRequest request,
      ActionListener<ExecuteDistributedTaskResponse> listener) {

    String nodeId = clusterService.localNode().getId();

    try {
      // Phase 1C: SearchSourceBuilder-based execution (transport from coordinator)
      if (request.getSearchSourceBuilder() != null) {
        executeSearchRequest(request, nodeId, listener);
        return;
      }

      // Legacy path: WorkUnit-based execution
      log.info(
          "Executing {} work units on node: {} for stage: {}",
          request.getWorkUnitCount(),
          nodeId,
          request.getStageId());

      // Validate request
      if (!request.isValid()) {
        String error = "Invalid distributed task request: " + request;
        log.error(error);
        listener.onResponse(ExecuteDistributedTaskResponse.failure(nodeId, error));
        return;
      }

      // Execute work units and collect results
      List<Object> allResults = new ArrayList<>();
      Map<String, Object> executionStats = new HashMap<>();
      long startTime = System.currentTimeMillis();
      int successCount = 0;
      int errorCount = 0;

      for (WorkUnit workUnit : request.getWorkUnits()) {
        try {
          log.debug("Executing work unit: {} on node: {}", workUnit.getWorkUnitId(), nodeId);

          Object result = executeWorkUnit(workUnit, request.getInputData());
          if (result != null) {
            allResults.add(result);
          }
          successCount++;

        } catch (Exception e) {
          log.error(
              "Failed to execute work unit: {} on node: {}", workUnit.getWorkUnitId(), nodeId, e);
          errorCount++;

          // For Phase 1, we'll continue with other work units on error
          // TODO: Add configurable error handling strategy
        }
      }

      // Collect execution statistics
      long executionTime = System.currentTimeMillis() - startTime;
      executionStats.put("executionTimeMs", executionTime);
      executionStats.put("workUnitsExecuted", successCount + errorCount);
      executionStats.put("successCount", successCount);
      executionStats.put("errorCount", errorCount);
      executionStats.put("resultCount", allResults.size());
      executionStats.put("nodeId", nodeId);

      log.info(
          "Completed execution on node: {} - {} results, {} successes, {} errors in {}ms",
          nodeId,
          allResults.size(),
          successCount,
          errorCount,
          executionTime);

      // Return results to coordinator
      ExecuteDistributedTaskResponse response =
          ExecuteDistributedTaskResponse.success(nodeId, allResults, executionStats);
      listener.onResponse(response);

    } catch (Exception e) {
      log.error("Critical error executing distributed tasks on node: {}", nodeId, e);
      ExecuteDistributedTaskResponse errorResponse =
          ExecuteDistributedTaskResponse.failure(
              nodeId, "Critical execution error: " + e.getMessage());
      listener.onResponse(errorResponse);
    }
  }

  /**
   * Phase 1C: Executes a search request locally on this data node. The coordinator sends the
   * SearchSourceBuilder and shard IDs via transport; this node executes client.search() locally for
   * the assigned shards and returns the SearchResponse.
   *
   * @param request The request containing SearchSourceBuilder, index name, and shard IDs
   * @param nodeId The local node ID
   * @param listener Response listener for async execution
   */
  private void executeSearchRequest(
      ExecuteDistributedTaskRequest request,
      String nodeId,
      ActionListener<ExecuteDistributedTaskResponse> listener) {

    String shardPreference =
        request.getShardIds().stream().map(String::valueOf).collect(Collectors.joining(","));

    log.info(
        "[Phase 1C] Executing search on node: {} for index: {}, shards: [{}]",
        nodeId,
        request.getIndexName(),
        shardPreference);

    SearchRequest searchRequest = new SearchRequest(request.getIndexName());
    searchRequest.source(request.getSearchSourceBuilder());
    searchRequest.preference("_shards:" + shardPreference);

    client.search(
        searchRequest,
        new ActionListener<>() {
          @Override
          public void onResponse(SearchResponse searchResponse) {
            log.info(
                "[Phase 1C] Search completed on node: {} - {} hits, hasAggs={}",
                nodeId,
                searchResponse.getHits().getHits().length,
                searchResponse.getAggregations() != null);
            listener.onResponse(
                ExecuteDistributedTaskResponse.successWithSearch(nodeId, searchResponse));
          }

          @Override
          public void onFailure(Exception e) {
            log.error("[Phase 1C] Search failed on node: {}", nodeId, e);
            listener.onResponse(
                ExecuteDistributedTaskResponse.failure(
                    nodeId, "Phase 1C search failed: " + e.getMessage()));
          }
        });
  }

  /**
   * Executes a single WorkUnit on this node.
   *
   * @param workUnit The work unit to execute
   * @param inputData Input data from previous stages
   * @return Execution results
   */
  private Object executeWorkUnit(WorkUnit workUnit, Object inputData) {
    TaskOperator operator = workUnit.getTaskOperator();
    if (operator == null) {
      throw new IllegalStateException("WorkUnit has no TaskOperator: " + workUnit.getWorkUnitId());
    }

    try {
      // Create execution context for this node
      TaskOperator.TaskContext context =
          new TaskOperator.TaskContext(
              clusterService.localNode().getId(),
              null, // Storage context will be set by actual operators
              30000L, // 30 second timeout for Phase 1
              100 * 1024 * 1024L, // 100MB memory limit per task
              Map.of());

      // Create task input (simplified for Phase 1)
      TaskOperator.TaskInput input =
          new TaskOperator.TaskInput(
              null, // Input data iterator - will be populated by actual operators
              Map.of(), // Filter conditions
              Map.of(), // Projections
              Map.of(), // Grouping keys
              Map.of(), // Aggregate functions
              Map.of(), // Sort criteria
              null, // Limit
              inputData != null ? Map.of("inputData", inputData) : Map.of());

      // Execute the operator
      log.debug(
          "Executing operator: {} for work unit: {}",
          operator.getOperatorType(),
          workUnit.getWorkUnitId());

      TaskOperator.TaskResult result = operator.execute(context, input);

      log.debug(
          "Operator execution completed for work unit: {} - {} records, {} bytes",
          workUnit.getWorkUnitId(),
          result != null ? result.getRecordCount() : 0,
          result != null ? result.getDataSize() : 0);

      return result;

    } catch (Exception e) {
      throw new RuntimeException("Failed to execute WorkUnit: " + workUnit.getWorkUnitId(), e);
    }
  }
}
