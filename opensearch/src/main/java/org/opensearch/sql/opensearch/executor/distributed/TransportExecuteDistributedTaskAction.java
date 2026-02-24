/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.IndicesService;
import org.opensearch.sql.opensearch.executor.distributed.pipeline.OperatorPipelineExecutor;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Transport action handler for executing distributed query tasks on data nodes.
 *
 * <p>This handler runs on each cluster node and processes ExecuteDistributedTaskRequest messages
 * from the coordinator. It executes the operator pipeline locally using direct Lucene access and
 * returns results via ExecuteDistributedTaskResponse.
 *
 * <p><strong>Execution Process:</strong>
 *
 * <ol>
 *   <li>Receive OPERATOR_PIPELINE request from coordinator node
 *   <li>Execute LuceneScanOperator + LimitOperator pipeline on assigned shards
 *   <li>Return rows to coordinator
 * </ol>
 */
@Log4j2
public class TransportExecuteDistributedTaskAction
    extends HandledTransportAction<ExecuteDistributedTaskRequest, ExecuteDistributedTaskResponse> {

  public static final String NAME = "cluster:admin/opensearch/sql/distributed/execute";

  private final ClusterService clusterService;
  private final Client client;
  private final IndicesService indicesService;

  @Inject
  public TransportExecuteDistributedTaskAction(
      TransportService transportService,
      ActionFilters actionFilters,
      ClusterService clusterService,
      Client client,
      IndicesService indicesService) {
    super(
        ExecuteDistributedTaskAction.NAME,
        transportService,
        actionFilters,
        ExecuteDistributedTaskRequest::new);
    this.clusterService = clusterService;
    this.client = client;
    this.indicesService = indicesService;
  }

  @Override
  protected void doExecute(
      Task task,
      ExecuteDistributedTaskRequest request,
      ActionListener<ExecuteDistributedTaskResponse> listener) {

    String nodeId = clusterService.localNode().getId();

    try {
      log.info(
          "[Operator Pipeline] Executing on node: {} for index: {}, shards: {}",
          nodeId,
          request.getIndexName(),
          request.getShardIds());

      OperatorPipelineExecutor.OperatorPipelineResult result =
          OperatorPipelineExecutor.execute(indicesService, request);

      log.info(
          "[Operator Pipeline] Completed on node: {} - {} rows", nodeId, result.getRows().size());

      listener.onResponse(
          ExecuteDistributedTaskResponse.successWithRows(
              nodeId, result.getFieldNames(), result.getRows()));
    } catch (Exception e) {
      log.error("[Operator Pipeline] Failed on node: {}", nodeId, e);
      listener.onResponse(
          ExecuteDistributedTaskResponse.failure(
              nodeId, "Operator pipeline failed: " + e.getMessage()));
    }
  }
}
