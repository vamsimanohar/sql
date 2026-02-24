/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.planner.distributed.DistributedPhysicalPlan;
import org.opensearch.sql.planner.distributed.ExecutionStage;
import org.opensearch.sql.planner.distributed.WorkUnit;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Coordinates the execution of distributed query plans across cluster nodes.
 *
 * <p>When distributed execution is enabled, ALL queries go through the operator pipeline. There is
 * no fallback — errors propagate directly so they can be identified and fixed.
 *
 * <p><strong>Execution Flow:</strong>
 *
 * <pre>
 * 1. Coordinator: extract index, fields, limit from RelNode
 * 2. Coordinator: group shards by node, send OPERATOR_PIPELINE transport requests
 * 3. Data nodes: LuceneScanOperator reads _source directly from Lucene
 * 4. Data nodes: LimitOperator applies per-node limit
 * 5. Coordinator: merge rows from all nodes, apply final limit
 * 6. Coordinator: build QueryResponse with schema from RelNode
 * </pre>
 */
@Log4j2
public class DistributedTaskScheduler {

  private final TransportService transportService;
  private final ClusterService clusterService;
  private final Client client;

  public DistributedTaskScheduler(
      TransportService transportService, ClusterService clusterService, Client client) {
    this.transportService = transportService;
    this.clusterService = clusterService;
    this.client = client;
  }

  /**
   * Executes a distributed physical plan via the operator pipeline.
   *
   * @param plan The distributed plan to execute
   * @param listener Response listener for async execution
   */
  public void executeQuery(DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {

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

      plan.markExecuting();

      if (plan.getRelNode() == null) {
        throw new IllegalStateException("Distributed plan has no RelNode");
      }

      executeOperatorPipeline(plan, listener);

    } catch (Exception e) {
      log.error("Failed distributed query execution: {}", plan.getPlanId(), e);
      plan.markFailed(e.getMessage());
      listener.onFailure(e);
    }
  }

  /**
   * Executes query using operator pipeline with direct Lucene access. Extracts index name, field
   * names, and limit from the RelNode tree, groups shards by node, sends parallel transport
   * requests with OPERATOR_PIPELINE mode, and merges results.
   */
  private void executeOperatorPipeline(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    RelNode relNode = (RelNode) plan.getRelNode();

    log.info("[Distributed Engine] Executing via operator pipeline for plan: {}", plan.getPlanId());

    try {
      // Step 1: Extract field names from RelNode row type
      List<String> fieldNames =
          relNode.getRowType().getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toList());

      // Step 2: Extract limit from RelNode tree
      int queryLimit = extractLimit(relNode);

      // Step 3: Get shard partitions from the SCAN stage work units
      List<WorkUnit> scanWorkUnits = getScanStageWorkUnits(plan);
      String indexName = scanWorkUnits.getFirst().getDataPartition().getIndexName();

      log.info(
          "[Distributed Engine] index: {}, fields: {}, limit: {}, shards: {}",
          indexName,
          fieldNames,
          queryLimit,
          scanWorkUnits.size());

      // Step 4: Group work units by node
      Map<String, List<WorkUnit>> workByNode = new HashMap<>();
      for (WorkUnit wu : scanWorkUnits) {
        String nodeId = wu.getDataPartition().getNodeId();
        if (nodeId == null) {
          nodeId = wu.getAssignedNodeId();
        }
        if (nodeId == null) {
          throw new IllegalStateException(
              "Work unit has no node assignment: " + wu.getWorkUnitId());
        }
        workByNode.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(wu);
      }

      // Step 5: Send parallel transport requests to each node
      List<CompletableFuture<ExecuteDistributedTaskResponse>> futures = new ArrayList<>();

      for (Map.Entry<String, List<WorkUnit>> entry : workByNode.entrySet()) {
        String nodeId = entry.getKey();
        List<WorkUnit> nodeWorkUnits = entry.getValue();

        List<Integer> shardIds =
            nodeWorkUnits.stream()
                .map(wu -> Integer.parseInt(wu.getDataPartition().getShardId()))
                .collect(Collectors.toList());

        ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest();
        request.setExecutionMode("OPERATOR_PIPELINE");
        request.setIndexName(indexName);
        request.setShardIds(shardIds);
        request.setFieldNames(fieldNames);
        request.setQueryLimit(queryLimit);
        request.setStageId("operator-pipeline");

        DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);
        if (targetNode == null) {
          throw new IllegalStateException("Cannot resolve DiscoveryNode for nodeId: " + nodeId);
        }

        CompletableFuture<ExecuteDistributedTaskResponse> future = new CompletableFuture<>();
        futures.add(future);

        transportService.sendRequest(
            targetNode,
            TransportExecuteDistributedTaskAction.NAME,
            request,
            new TransportResponseHandler<ExecuteDistributedTaskResponse>() {
              @Override
              public ExecuteDistributedTaskResponse read(
                  org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
                return new ExecuteDistributedTaskResponse(in);
              }

              @Override
              public void handleResponse(ExecuteDistributedTaskResponse response) {
                if (response.isSuccessful()) {
                  future.complete(response);
                } else {
                  future.completeExceptionally(
                      new RuntimeException(
                          response.getErrorMessage() != null
                              ? response.getErrorMessage()
                              : "Operator pipeline failed on node: " + nodeId));
                }
              }

              @Override
              public void handleException(TransportException exp) {
                future.completeExceptionally(exp);
              }

              @Override
              public String executor() {
                return org.opensearch.threadpool.ThreadPool.Names.GENERIC;
              }
            });
      }

      // Step 6: Wait for all responses
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

      // Step 7: Merge rows from all nodes
      List<List<Object>> allRows = new ArrayList<>();
      for (CompletableFuture<ExecuteDistributedTaskResponse> future : futures) {
        ExecuteDistributedTaskResponse resp = future.get();
        if (resp.getPipelineRows() != null) {
          allRows.addAll(resp.getPipelineRows());
        }
      }

      // Apply final limit
      if (allRows.size() > queryLimit) {
        allRows = allRows.subList(0, queryLimit);
      }

      log.info(
          "[Distributed Engine] Merged {} rows from {} nodes", allRows.size(), workByNode.size());

      // Step 8: Build QueryResponse with schema from RelNode row type
      List<ExprValue> values = new ArrayList<>();
      for (List<Object> row : allRows) {
        Map<String, ExprValue> exprRow = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size() && i < row.size(); i++) {
          exprRow.put(fieldNames.get(i), ExprValueUtils.fromObjectValue(row.get(i)));
        }
        values.add(ExprTupleValue.fromExprValueMap(exprRow));
      }

      List<Column> columns = new ArrayList<>();
      for (RelDataTypeField field : relNode.getRowType().getFieldList()) {
        ExprType exprType;
        if (field.getType().getSqlTypeName() == SqlTypeName.ANY) {
          if (!values.isEmpty()) {
            ExprValue firstVal = values.getFirst().tupleValue().get(field.getName());
            exprType = firstVal != null ? firstVal.type() : ExprCoreType.UNDEFINED;
          } else {
            exprType = ExprCoreType.UNDEFINED;
          }
        } else {
          exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
        }
        columns.add(new Column(field.getName(), null, exprType));
      }

      Schema schema = new Schema(columns);
      QueryResponse queryResponse = new QueryResponse(schema, values, null);

      plan.markCompleted();
      log.info(
          "[Distributed Engine] Query completed with {} results for plan: {}",
          queryResponse.getResults().size(),
          plan.getPlanId());
      listener.onResponse(queryResponse);

    } catch (Exception e) {
      log.error(
          "[Distributed Engine] Operator pipeline execution failed for plan: {}",
          plan.getPlanId(),
          e);
      plan.markFailed(e.getMessage());
      listener.onFailure(new RuntimeException("Operator pipeline execution failed", e));
    }
  }

  /**
   * Extracts the query limit from the RelNode tree. Looks for Sort with fetch (head N) or
   * LogicalSystemLimit, returning whichever is smaller.
   */
  private int extractLimit(RelNode node) {
    int limit = 10000; // Default system limit
    if (node instanceof LogicalSystemLimit sysLimit) {
      if (sysLimit.fetch != null) {
        try {
          int sysVal =
              ((org.apache.calcite.rex.RexLiteral) sysLimit.fetch).getValueAs(Integer.class);
          limit = Math.min(limit, sysVal);
        } catch (Exception e) {
          // Not a literal, use default
        }
      }
      for (RelNode input : node.getInputs()) {
        limit = Math.min(limit, extractLimit(input));
      }
    } else if (node instanceof org.apache.calcite.rel.core.Sort sort) {
      if (sort.fetch != null) {
        try {
          int fetchVal = ((org.apache.calcite.rex.RexLiteral) sort.fetch).getValueAs(Integer.class);
          limit = Math.min(limit, fetchVal);
        } catch (Exception e) {
          // Not a literal, use default
        }
      }
    } else {
      for (RelNode input : node.getInputs()) {
        limit = Math.min(limit, extractLimit(input));
      }
    }
    return limit;
  }

  /**
   * Gets work units from the SCAN stage of the distributed plan.
   *
   * @param plan The distributed plan
   * @return List of scan work units
   */
  private List<WorkUnit> getScanStageWorkUnits(DistributedPhysicalPlan plan) {
    for (ExecutionStage stage : plan.getExecutionStages()) {
      if (stage.getStageType() == ExecutionStage.StageType.SCAN) {
        return stage.getWorkUnits();
      }
    }
    throw new IllegalStateException("No SCAN stage found in distributed plan");
  }

  /** Shuts down the scheduler and releases resources. */
  public void shutdown() {
    log.info("Shutting down DistributedTaskScheduler");
  }
}
