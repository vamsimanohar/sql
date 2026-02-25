/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.opensearch.executor.distributed.dataunit.LocalityAwareDataUnitAssignment;
import org.opensearch.sql.opensearch.executor.distributed.dataunit.OpenSearchDataUnit;
import org.opensearch.sql.opensearch.executor.distributed.planner.RelNodeAnalyzer;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitAssignment;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;
import org.opensearch.transport.TransportService;

/**
 * Orchestrates distributed query execution on the coordinator node.
 *
 * <ol>
 *   <li>Takes a StagedPlan and RelNode analysis
 *   <li>Assigns shards to nodes via DataUnitAssignment
 *   <li>Sends ExecuteDistributedTaskRequest to each data node
 *   <li>Collects responses asynchronously
 *   <li>Merges rows, applies coordinator-side limit, builds QueryResponse
 * </ol>
 */
public class DistributedQueryCoordinator {

  private static final Logger logger = LogManager.getLogger(DistributedQueryCoordinator.class);

  private final ClusterService clusterService;
  private final TransportService transportService;
  private final DataUnitAssignment dataUnitAssignment;

  public DistributedQueryCoordinator(
      ClusterService clusterService, TransportService transportService) {
    this.clusterService = clusterService;
    this.transportService = transportService;
    this.dataUnitAssignment = new LocalityAwareDataUnitAssignment();
  }

  /**
   * Executes a distributed query plan.
   *
   * @param stagedPlan the fragmented execution plan
   * @param analysis the RelNode analysis result
   * @param relNode the original RelNode (for schema extraction)
   * @param listener the response listener
   */
  public void execute(
      StagedPlan stagedPlan,
      RelNodeAnalyzer.AnalysisResult analysis,
      RelNode relNode,
      ResponseListener<QueryResponse> listener) {

    try {
      // Get leaf stage data units (shards)
      ComputeStage leafStage = stagedPlan.getLeafStages().get(0);
      List<DataUnit> dataUnits = leafStage.getDataUnits();

      // Assign shards to nodes
      List<String> availableNodes =
          clusterService.state().nodes().getDataNodes().values().stream()
              .map(DiscoveryNode::getId)
              .toList();
      Map<String, List<DataUnit>> nodeAssignments =
          dataUnitAssignment.assign(dataUnits, availableNodes);

      logger.info(
          "Distributed query: index={}, shards={}, nodes={}",
          analysis.getIndexName(),
          dataUnits.size(),
          nodeAssignments.size());

      // Send requests to each node
      int totalNodes = nodeAssignments.size();
      CountDownLatch latch = new CountDownLatch(totalNodes);
      AtomicBoolean failed = new AtomicBoolean(false);
      CopyOnWriteArrayList<ExecuteDistributedTaskResponse> responses = new CopyOnWriteArrayList<>();

      for (Map.Entry<String, List<DataUnit>> entry : nodeAssignments.entrySet()) {
        String nodeId = entry.getKey();
        List<DataUnit> nodeDUs = entry.getValue();

        ExecuteDistributedTaskRequest request =
            buildRequest(leafStage.getStageId(), analysis, nodeDUs);

        DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);
        if (targetNode == null) {
          latch.countDown();
          if (failed.compareAndSet(false, true)) {
            listener.onFailure(new IllegalStateException("Node not found in cluster: " + nodeId));
          }
          continue;
        }

        transportService.sendRequest(
            targetNode,
            ExecuteDistributedTaskAction.NAME,
            request,
            new org.opensearch.transport.TransportResponseHandler<
                ExecuteDistributedTaskResponse>() {
              @Override
              public ExecuteDistributedTaskResponse read(
                  org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
                return new ExecuteDistributedTaskResponse(in);
              }

              @Override
              public void handleResponse(ExecuteDistributedTaskResponse response) {
                if (!response.isSuccessful()) {
                  if (failed.compareAndSet(false, true)) {
                    listener.onFailure(
                        new RuntimeException(
                            "Node "
                                + response.getNodeId()
                                + " failed: "
                                + response.getErrorMessage()));
                  }
                } else {
                  responses.add(response);
                }
                latch.countDown();
              }

              @Override
              public void handleException(org.opensearch.transport.TransportException exp) {
                if (failed.compareAndSet(false, true)) {
                  listener.onFailure(exp);
                }
                latch.countDown();
              }

              @Override
              public String executor() {
                return org.opensearch.threadpool.ThreadPool.Names.GENERIC;
              }
            });
      }

      // Wait for all responses
      latch.await();

      if (failed.get()) {
        return; // Error already reported via listener.onFailure
      }

      // Merge results
      QueryResponse queryResponse = mergeResults(responses, analysis, relNode);
      listener.onResponse(queryResponse);

    } catch (Exception e) {
      logger.error("Distributed query execution failed", e);
      listener.onFailure(e);
    }
  }

  private ExecuteDistributedTaskRequest buildRequest(
      String stageId, RelNodeAnalyzer.AnalysisResult analysis, List<DataUnit> nodeDUs) {

    List<Integer> shardIds = new ArrayList<>();
    for (DataUnit du : nodeDUs) {
      if (du instanceof OpenSearchDataUnit) {
        shardIds.add(((OpenSearchDataUnit) du).getShardId());
      }
    }

    int limit = analysis.getQueryLimit() > 0 ? analysis.getQueryLimit() : 10000;

    return new ExecuteDistributedTaskRequest(
        stageId,
        analysis.getIndexName(),
        shardIds,
        "OPERATOR_PIPELINE",
        analysis.getFieldNames(),
        limit,
        analysis.getFilterConditions());
  }

  private QueryResponse mergeResults(
      List<ExecuteDistributedTaskResponse> responses,
      RelNodeAnalyzer.AnalysisResult analysis,
      RelNode relNode) {

    // Build schema from RelNode row type
    Schema schema = buildSchema(relNode);

    // Merge all rows from all nodes
    List<ExprValue> allRows = new ArrayList<>();
    for (ExecuteDistributedTaskResponse response : responses) {
      if (response.getPipelineFieldNames() != null && response.getPipelineRows() != null) {
        List<String> fieldNames = response.getPipelineFieldNames();
        for (List<Object> row : response.getPipelineRows()) {
          LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
          for (int i = 0; i < fieldNames.size() && i < row.size(); i++) {
            valueMap.put(fieldNames.get(i), ExprValueUtils.fromObjectValue(row.get(i)));
          }
          allRows.add(new ExprTupleValue(valueMap));
        }
      }
    }

    // Apply coordinator-side limit (data nodes each apply limit per-node, but total may exceed)
    int limit = analysis.getQueryLimit();
    if (limit > 0 && allRows.size() > limit) {
      allRows = allRows.subList(0, limit);
    }

    logger.info("Distributed query merged {} rows from {} nodes", allRows.size(), responses.size());

    return new QueryResponse(schema, allRows, Cursor.None);
  }

  private Schema buildSchema(RelNode relNode) {
    RelDataType rowType = relNode.getRowType();
    List<Schema.Column> columns = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      ExprType exprType;
      try {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
      } catch (IllegalArgumentException e) {
        exprType = org.opensearch.sql.data.type.ExprCoreType.STRING;
      }
      columns.add(new Schema.Column(field.getName(), null, exprType));
    }
    return new Schema(columns);
  }
}
