/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.locationtech.jts.geom.Point;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper.OpenSearchRelRunners;
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
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.planner.distributed.DataPartition;
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
 * <p>The DistributedTaskScheduler manages the lifecycle of distributed query execution:
 *
 * <ul>
 *   <li><strong>Stage Coordination</strong>: Executes stages in dependency order
 *   <li><strong>Work Distribution</strong>: Distributes WorkUnits to appropriate nodes
 *   <li><strong>Data Locality</strong>: Assigns scan tasks to nodes containing the data
 *   <li><strong>Result Collection</strong>: Aggregates results from distributed tasks
 *   <li><strong>Fault Tolerance</strong>: Handles node failures and retries
 * </ul>
 *
 * <p><strong>Execution Flow:</strong>
 *
 * <pre>
 * 1. executeQuery(DistributedPhysicalPlan) → Start execution
 * 2. Phase 1A: Local Calcite execution (if RelNode available)
 *    OR Future: executeStage(ExecutionStage) → Execute ready stages via transport
 * 3. Response via QueryResponse callback
 * </pre>
 *
 * <p><strong>Phase 1A:</strong> Executes queries locally via Calcite's built-in execution engine,
 * bypassing transport. This validates the distributed planning pipeline while producing correct
 * results using the same mechanism as the legacy engine.
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

      // Mark plan as executing
      plan.markExecuting();

      // Phase 2: Transport-based parallel execution with Phase 1B/1A fallback
      if (plan.getRelNode() != null) {
        try {
          executePerShardSearch(plan, listener);
        } catch (Exception e) {
          log.warn(
              "[Distributed Engine] Phase 2 transport failed, trying Phase 1B local for plan: {}",
              plan.getPlanId(),
              e);
          try {
            executePerShardLocal(plan, listener);
          } catch (Exception e2) {
            log.warn(
                "[Distributed Engine] Phase 1B local failed, falling back to Phase 1A Calcite"
                    + " for plan: {}",
                plan.getPlanId(),
                e2);
            executeLocalCalcite(plan, listener);
          }
        }
        return;
      }

      // Future: distributed execution via transport
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
   * Phase 1A: Executes the query locally via Calcite's built-in execution engine. This bypasses
   * transport entirely and executes the full query on the coordinator node using the same mechanism
   * as the legacy OpenSearchExecutionEngine.
   *
   * @param plan The distributed plan containing the RelNode and CalcitePlanContext
   * @param listener Response listener for the query results
   */
  private void executeLocalCalcite(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    try {
      RelNode relNode = (RelNode) plan.getRelNode();
      CalcitePlanContext context = (CalcitePlanContext) plan.getPlanContext();

      log.info(
          "[Distributed Engine] Phase 1A - executing locally via Calcite for plan: {}",
          plan.getPlanId());

      // Execute via Calcite (same mechanism as legacy engine)
      try (PreparedStatement statement = OpenSearchRelRunners.run(context, relNode)) {
        ResultSet resultSet = statement.executeQuery();
        QueryResponse response =
            buildQueryResponse(resultSet, relNode.getRowType(), context.sysLimit.querySizeLimit());

        plan.markCompleted();
        log.info(
            "[Distributed Engine] Phase 1A - query completed with {} results for plan: {}",
            response.getResults().size(),
            plan.getPlanId());
        listener.onResponse(response);
      }

    } catch (Exception e) {
      log.error(
          "[Distributed Engine] Phase 1A local execution failed for plan: {}", plan.getPlanId(), e);
      plan.markFailed(e.getMessage());
      listener.onFailure(e);
    }
  }

  /**
   * Phase 2: Executes query using transport-based parallel execution. Supports scan, filter,
   * projection, and aggregation queries. The coordinator groups shards by their assigned data
   * nodes, sends ExecuteDistributedTaskRequest (containing SearchSourceBuilder + shard IDs) to each
   * node via transportService.sendRequest(), and each data node executes client.search() locally
   * for its assigned shards. For aggregation queries, partial results are merged via
   * InternalAggregations.reduce().
   *
   * @param plan The distributed plan containing the RelNode and CalcitePlanContext
   * @param listener Response listener for the query results
   */
  private void executePerShardSearch(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    RelNode relNode = (RelNode) plan.getRelNode();
    CalcitePlanContext context = (CalcitePlanContext) plan.getPlanContext();

    log.info(
        "[Distributed Engine] Phase 2 - executing per-shard via transport for plan: {}",
        plan.getPlanId());

    PreparedStatement statement = null;
    try {
      // Step 1: Run Calcite optimization (VolcanoPlanner applies pushdown rules)
      statement = OpenSearchRelRunners.run(context, relNode);

      // Step 2: Read the optimized scan node from ThreadLocal
      Object scanObj = CalcitePlanContext.optimizedScanNode.get();
      CalcitePlanContext.optimizedScanNode.remove();

      if (!(scanObj instanceof AbstractCalciteIndexScan scan)) {
        // Scan node not available — query has post-aggregation operations (sort/limit)
        // that Calcite handles internally. Fall back to Phase 1A using the already-running
        // PreparedStatement instead of throwing (which would consume the connection).
        log.info(
            "[Distributed Engine] Phase 2 - scan node not available (likely sort/limit after agg),"
                + " using Phase 1A inline for plan: {}",
            plan.getPlanId());
        ResultSet resultSet = statement.executeQuery();
        QueryResponse queryResponse =
            buildQueryResponse(resultSet, relNode.getRowType(), context.sysLimit.querySizeLimit());
        statement.close();
        plan.markCompleted();
        listener.onResponse(queryResponse);
        return;
      }

      // Step 3: Extract SearchSourceBuilder from PushDownContext
      OpenSearchRequestBuilder requestBuilder = scan.getPushDownContext().createRequestBuilder();
      SearchSourceBuilder sourceBuilder = requestBuilder.getSourceBuilder();
      OpenSearchExprValueFactory exprValueFactory = requestBuilder.getExprValueFactory();
      boolean isAggregation = sourceBuilder.size() == 0 && sourceBuilder.aggregations() != null;

      org.opensearch.search.fetch.subphase.FetchSourceContext fetchSource =
          sourceBuilder.fetchSource();
      List<String> includes =
          fetchSource != null ? Arrays.asList(fetchSource.includes()) : List.of();

      // Step 4: Get shard partitions from the SCAN stage work units
      List<WorkUnit> scanWorkUnits = getScanStageWorkUnits(plan);
      String indexName = scanWorkUnits.getFirst().getDataPartition().getIndexName();

      log.info(
          "[Distributed Engine] Phase 2 - extracted SSB for index: {}, isAgg: {}, shards: {}",
          indexName,
          isAggregation,
          scanWorkUnits.size());

      // Step 5: Group work units by node for parallel transport execution
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

      log.info("[Distributed Engine] Phase 2 - sending requests to {} nodes", workByNode.size());

      // Step 6: Send parallel transport requests to each node
      List<CompletableFuture<SearchResponse>> futures = new ArrayList<>();

      for (Map.Entry<String, List<WorkUnit>> entry : workByNode.entrySet()) {
        String nodeId = entry.getKey();
        List<WorkUnit> nodeWorkUnits = entry.getValue();

        // Collect shard IDs for this node
        List<Integer> shardIds =
            nodeWorkUnits.stream()
                .map(wu -> Integer.parseInt(wu.getDataPartition().getShardId()))
                .collect(Collectors.toList());

        // Create transport request
        ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest();
        request.setSearchSourceBuilder(sourceBuilder);
        request.setIndexName(indexName);
        request.setShardIds(shardIds);
        request.setStageId("distributed-scan");

        // Resolve the DiscoveryNode for transport
        DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);
        if (targetNode == null) {
          throw new IllegalStateException("Cannot resolve DiscoveryNode for nodeId: " + nodeId);
        }

        CompletableFuture<SearchResponse> future = new CompletableFuture<>();
        futures.add(future);

        log.debug(
            "[Distributed Engine] Phase 2 - sending to node: {} for shards: {}", nodeId, shardIds);

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
                if (response.isSuccessful() && response.getSearchResponse() != null) {
                  log.debug(
                      "[Distributed Engine] Phase 2 - received response from node: {}", nodeId);
                  future.complete(response.getSearchResponse());
                } else {
                  String errorMsg =
                      response.getErrorMessage() != null
                          ? response.getErrorMessage()
                          : "No SearchResponse in response from node: " + nodeId;
                  future.completeExceptionally(new RuntimeException(errorMsg));
                }
              }

              @Override
              public void handleException(TransportException exp) {
                log.error(
                    "[Distributed Engine] Phase 2 - transport exception from node: {}",
                    nodeId,
                    exp);
                future.completeExceptionally(exp);
              }

              @Override
              public String executor() {
                return org.opensearch.threadpool.ThreadPool.Names.GENERIC;
              }
            });
      }

      // Step 7: Wait for all transport responses
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

      // Step 8: Collect SearchResponses from all nodes
      List<SearchResponse> shardResponses = new ArrayList<>();
      for (CompletableFuture<SearchResponse> future : futures) {
        shardResponses.add(future.get());
      }

      log.info(
          "[Distributed Engine] Phase 2 - merging results from {} node responses",
          shardResponses.size());

      // Step 9: Merge results and build QueryResponse
      QueryResponse queryResponse =
          mergeSearchResponses(
              shardResponses,
              exprValueFactory,
              isAggregation,
              includes,
              relNode.getRowType(),
              context.sysLimit.querySizeLimit());

      statement.close();

      plan.markCompleted();
      log.info(
          "[Distributed Engine] Phase 2 - query completed with {} results for plan: {}",
          queryResponse.getResults().size(),
          plan.getPlanId());
      listener.onResponse(queryResponse);

    } catch (Exception e) {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException ignored) {
          // ignore close errors
        }
      }
      throw new RuntimeException("Phase 2 transport-based execution failed", e);
    }
  }

  /**
   * Phase 1B fallback: Executes query using sequential per-shard OpenSearch search API calls on the
   * coordinator node. Calcite is used only for logical optimization (pushdown rules populate
   * PushDownContext). After optimization, the SearchSourceBuilder is extracted and executed
   * directly against individual shards via client.search().
   *
   * @param plan The distributed plan containing the RelNode and CalcitePlanContext
   * @param listener Response listener for the query results
   */
  private void executePerShardLocal(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    RelNode relNode = (RelNode) plan.getRelNode();
    CalcitePlanContext context = (CalcitePlanContext) plan.getPlanContext();

    log.info(
        "[Distributed Engine] Phase 1B - executing per-shard search locally for plan: {}",
        plan.getPlanId());

    PreparedStatement statement = null;
    try {
      // Step 1: Run Calcite optimization (VolcanoPlanner applies pushdown rules)
      statement = OpenSearchRelRunners.run(context, relNode);

      // Step 2: Read the optimized scan node from ThreadLocal
      Object scanObj = CalcitePlanContext.optimizedScanNode.get();
      CalcitePlanContext.optimizedScanNode.remove();

      if (!(scanObj instanceof AbstractCalciteIndexScan scan)) {
        throw new UnsupportedOperationException(
            "Optimized scan node is not an AbstractCalciteIndexScan: "
                + (scanObj != null ? scanObj.getClass().getName() : "null"));
      }

      // Step 3: Extract SearchSourceBuilder from PushDownContext
      OpenSearchRequestBuilder requestBuilder = scan.getPushDownContext().createRequestBuilder();
      SearchSourceBuilder sourceBuilder = requestBuilder.getSourceBuilder();
      OpenSearchExprValueFactory exprValueFactory = requestBuilder.getExprValueFactory();
      boolean isAggregation = sourceBuilder.size() == 0 && sourceBuilder.aggregations() != null;

      org.opensearch.search.fetch.subphase.FetchSourceContext fetchSource =
          sourceBuilder.fetchSource();
      List<String> includes =
          fetchSource != null ? Arrays.asList(fetchSource.includes()) : List.of();

      // Step 4: Get shard partitions from the SCAN stage work units
      List<WorkUnit> scanWorkUnits = getScanStageWorkUnits(plan);
      String indexName = scanWorkUnits.getFirst().getDataPartition().getIndexName();

      log.info(
          "[Distributed Engine] Phase 1B - extracted SSB for index: {}, isAgg: {}",
          indexName,
          isAggregation);

      // Step 5: Execute per-shard searches sequentially
      List<SearchResponse> shardResponses = new ArrayList<>();
      for (WorkUnit wu : scanWorkUnits) {
        DataPartition partition = wu.getDataPartition();
        String shardId = partition.getShardId();

        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(sourceBuilder);
        searchRequest.preference("_shards:" + shardId);

        log.debug(
            "[Distributed Engine] Phase 1B - executing shard search: {}/shard:{}",
            indexName,
            shardId);

        SearchResponse response = client.search(searchRequest).actionGet();
        shardResponses.add(response);

        log.debug(
            "[Distributed Engine] Phase 1B - shard {} returned {} hits, hasAggs={}",
            shardId,
            response.getHits().getHits().length,
            response.getAggregations() != null);
      }

      log.info(
          "[Distributed Engine] Phase 1B - merging results from {} shards", shardResponses.size());

      // Step 6: Merge results and build QueryResponse
      QueryResponse queryResponse =
          mergeSearchResponses(
              shardResponses,
              exprValueFactory,
              isAggregation,
              includes,
              relNode.getRowType(),
              context.sysLimit.querySizeLimit());

      statement.close();

      plan.markCompleted();
      log.info(
          "[Distributed Engine] Phase 1B - query completed with {} results for plan: {}",
          queryResponse.getResults().size(),
          plan.getPlanId());
      listener.onResponse(queryResponse);

    } catch (Exception e) {
      if (statement != null) {
        try {
          statement.close();
        } catch (SQLException ignored) {
          // ignore close errors
        }
      }
      throw new RuntimeException("Phase 1B per-shard search failed", e);
    }
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

  /**
   * Merges SearchResponse results from multiple shards into a single QueryResponse.
   *
   * <p>For non-aggregation queries: collects all SearchHits and applies size limit. For aggregation
   * queries: uses InternalAggregations.reduce() to merge partial aggregation results.
   *
   * @param shardResponses List of per-shard SearchResponse
   * @param exprValueFactory Factory for converting to ExprValue
   * @param isAggregation Whether this is an aggregation query
   * @param includes List of field names to include (controls metadata field filtering)
   * @param rowType RelDataType for schema construction
   * @param querySizeLimit Maximum number of rows to return
   * @return Merged QueryResponse
   */
  private QueryResponse mergeSearchResponses(
      List<SearchResponse> shardResponses,
      OpenSearchExprValueFactory exprValueFactory,
      boolean isAggregation,
      List<String> includes,
      RelDataType rowType,
      Integer querySizeLimit) {

    List<ExprValue> values = new ArrayList<>();

    if (isAggregation) {
      // Aggregation merge: use InternalAggregations.reduce()
      List<InternalAggregations> aggsList = new ArrayList<>();
      for (SearchResponse response : shardResponses) {
        if (response.getAggregations() != null) {
          aggsList.add((InternalAggregations) response.getAggregations());
        }
      }

      if (!aggsList.isEmpty()) {
        // Create a ReduceContext for final reduction
        InternalAggregation.ReduceContext reduceContext =
            InternalAggregation.ReduceContext.forFinalReduction(
                BigArrays.NON_RECYCLING_INSTANCE,
                null, // ScriptService not needed for basic agg reduce
                (s) -> {},
                PipelineAggregator.PipelineTree.EMPTY);

        InternalAggregations merged = InternalAggregations.reduce(aggsList, reduceContext);

        // Convert merged aggregations to ExprValue via the parser set during pushdown
        if (exprValueFactory.getParser() != null) {
          List<Map<String, Object>> parsedAggs = exprValueFactory.getParser().parse(merged);
          List<RelDataTypeField> schemaFields = rowType.getFieldList();
          for (Map<String, Object> entry : parsedAggs) {
            // Build unordered row from parsed aggregation
            Map<String, ExprValue> unorderedRow = new HashMap<>();
            for (Map.Entry<String, Object> kv : entry.entrySet()) {
              unorderedRow.put(
                  kv.getKey(), exprValueFactory.construct(kv.getKey(), kv.getValue(), true));
            }
            // Reorder row to match schema column order from RelDataType
            Map<String, ExprValue> orderedRow = new LinkedHashMap<>();
            for (RelDataTypeField field : schemaFields) {
              ExprValue val = unorderedRow.get(field.getName());
              if (val != null) {
                orderedRow.put(field.getName(), val);
              }
            }
            // Include any remaining keys not in schema (safety fallback)
            for (Map.Entry<String, ExprValue> kv : unorderedRow.entrySet()) {
              orderedRow.putIfAbsent(kv.getKey(), kv.getValue());
            }
            values.add(ExprTupleValue.fromExprValueMap(orderedRow));
          }
        }
      }
    } else {
      // Non-aggregation merge: collect all SearchHits
      for (SearchResponse response : shardResponses) {
        OpenSearchResponse osResponse =
            new OpenSearchResponse(response, exprValueFactory, includes, false);
        Iterator<ExprValue> hitIterator = osResponse.iterator();
        while (hitIterator.hasNext()
            && (querySizeLimit == null || values.size() < querySizeLimit)) {
          values.add(hitIterator.next());
        }

        if (querySizeLimit != null && values.size() >= querySizeLimit) {
          break;
        }
      }
    }

    // Build schema from RelDataType
    List<Column> columns = new ArrayList<>();
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (RelDataTypeField field : fields) {
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
    return new QueryResponse(schema, values, null);
  }

  /**
   * Converts a JDBC ResultSet into a QueryResponse with schema and ExprValue rows. Replicates the
   * logic from OpenSearchExecutionEngine.buildResultSet() to ensure identical output format.
   */
  private QueryResponse buildQueryResponse(
      ResultSet resultSet, RelDataType rowTypes, Integer querySizeLimit) throws SQLException {
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    List<RelDataType> fieldTypes =
        rowTypes.getFieldList().stream().map(RelDataTypeField::getType).toList();
    List<ExprValue> values = new ArrayList<>();

    // Iterate through the ResultSet and convert rows to ExprValue
    while (resultSet.next() && (querySizeLimit == null || values.size() < querySizeLimit)) {
      Map<String, ExprValue> row = new LinkedHashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnName(i);
        Object value = resultSet.getObject(columnName);
        Object converted = processValue(value);
        ExprValue exprValue = ExprValueUtils.fromObjectValue(converted);
        row.put(columnName, exprValue);
      }
      values.add(ExprTupleValue.fromExprValueMap(row));
    }

    // Build schema from ResultSet metadata and RelDataType
    List<Column> columns = new ArrayList<>(columnCount);
    for (int i = 1; i <= columnCount; ++i) {
      String columnName = metaData.getColumnName(i);
      RelDataType fieldType = fieldTypes.get(i - 1);
      ExprType exprType;
      if (fieldType.getSqlTypeName() == SqlTypeName.ANY) {
        if (!values.isEmpty()) {
          exprType = values.getFirst().tupleValue().get(columnName).type();
        } else {
          exprType = ExprCoreType.UNDEFINED;
        }
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(fieldType);
      }
      columns.add(new Column(columnName, null, exprType));
    }

    Schema schema = new Schema(columns);
    return new QueryResponse(schema, values, null);
  }

  /**
   * Process values recursively, handling geo points and nested maps/lists. Replicates logic from
   * OpenSearchExecutionEngine.processValue().
   */
  private static Object processValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Point) {
      Point point = (Point) value;
      return new OpenSearchExprGeoPointValue(point.getY(), point.getX());
    }
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      Map<String, Object> convertedMap = new HashMap<>();
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        convertedMap.put(entry.getKey(), processValue(entry.getValue()));
      }
      return convertedMap;
    }
    if (value instanceof List) {
      @SuppressWarnings("unchecked")
      List<Object> list = (List<Object>) value;
      List<Object> convertedList = new ArrayList<>();
      for (Object item : list) {
        convertedList.add(processValue(item));
      }
      return convertedList;
    }
    return value;
  }

  /** Executes the next batch of ready stages that have no pending dependencies. */
  private void executeNextReadyStages(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {

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
        String error =
            "No ready stages found but plan not complete. Completed stages: " + completed;
        log.error(error);
        plan.markFailed(error);
        listener.onFailure(new IllegalStateException(error));
      }
      return;
    }

    log.info("Executing {} ready stages for plan: {}", readyStages.size(), plan.getPlanId());

    // Execute all ready stages in parallel
    List<CompletableFuture<Void>> stageFutures =
        readyStages.stream()
            .map(stage -> executeStageAsync(stage, plan, listener))
            .collect(Collectors.toList());

    // When all current stages complete, check for next ready stages
    CompletableFuture.allOf(stageFutures.toArray(new CompletableFuture[0]))
        .whenComplete(
            (result, throwable) -> {
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

  /** Executes a single stage asynchronously. */
  private CompletableFuture<Void> executeStageAsync(
      ExecutionStage stage,
      DistributedPhysicalPlan plan,
      ResponseListener<QueryResponse> listener) {

    return CompletableFuture.runAsync(
        () -> {
          try {
            log.info(
                "Executing stage: {} with {} work units",
                stage.getStageId(),
                stage.getWorkUnits().size());

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
        },
        executorService);
  }

  /** Executes a single stage by distributing its work units across appropriate nodes. */
  private void executeStage(ExecutionStage stage) {
    List<WorkUnit> workUnits = stage.getWorkUnits();

    if (workUnits.isEmpty()) {
      log.warn("Stage {} has no work units", stage.getStageId());
      return;
    }

    // Group work units by target node for efficient distribution
    Map<String, List<WorkUnit>> workByNode = groupWorkUnitsByNode(workUnits);

    log.debug(
        "Distributing {} work units across {} nodes for stage: {}",
        workUnits.size(),
        workByNode.size(),
        stage.getStageId());

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

  /** Groups work units by their assigned node, considering data locality. */
  private Map<String, List<WorkUnit>> groupWorkUnitsByNode(List<WorkUnit> workUnits) {
    Map<String, List<WorkUnit>> workByNode = new HashMap<>();

    for (WorkUnit workUnit : workUnits) {
      String targetNode = determineTargetNode(workUnit);
      workByNode.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(workUnit);
    }

    return workByNode;
  }

  /** Determines the best node to execute a work unit, considering data locality. */
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

  /** Gets list of available data nodes in the cluster. */
  private List<String> getAvailableDataNodes() {
    return clusterService.state().nodes().getDataNodes().values().stream()
        .map(DiscoveryNode::getId)
        .collect(Collectors.toList());
  }

  /** Executes a list of work units on a specific node using transport actions. */
  private CompletableFuture<Object> executeWorkUnitsOnNode(
      String nodeId, List<WorkUnit> workUnits, ExecutionStage stage) {

    CompletableFuture<Object> future = new CompletableFuture<>();

    try {
      // Create request for the target node
      ExecuteDistributedTaskRequest request =
          new ExecuteDistributedTaskRequest(
              workUnits, stage.getStageId(), getStageInputData(stage));

      log.debug("Sending {} work units to node: {}", workUnits.size(), nodeId);

      // Send request via transport service
      transportService.sendRequest(
          clusterService.state().nodes().get(nodeId),
          ExecuteDistributedTaskAction.NAME,
          request,
          new TransportResponseHandler<ExecuteDistributedTaskResponse>() {
            @Override
            public ExecuteDistributedTaskResponse read(
                org.opensearch.core.common.io.stream.StreamInput in) throws java.io.IOException {
              return new ExecuteDistributedTaskResponse(in);
            }

            @Override
            public void handleResponse(ExecuteDistributedTaskResponse response) {
              log.debug(
                  "Received response from node: {} with {} results",
                  nodeId,
                  response.getResults().size());

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

  /** Gets input data for a stage from previous stage results. */
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

  /** Delivers the final query results to the response listener. */
  private void deliverFinalResults(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {

    try {
      // Get results from the final stage
      ExecutionStage finalStage = plan.getFinalStage();
      if (finalStage == null) {
        throw new IllegalStateException("No final stage found in plan");
      }

      Object finalResults = stageResults.get(finalStage.getStageId());

      // TODO: Phase 1B+ - Convert distributed results to QueryResponse with actual data
      // For now, create a simple response indicating successful completion
      QueryResponse response =
          new QueryResponse(
              plan.getOutputSchema(),
              List.of(), // Empty results until transport-based execution is implemented
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

  /** Shuts down the scheduler and releases resources. */
  public void shutdown() {
    log.info("Shutting down DistributedTaskScheduler");
    executorService.shutdown();
    completedWorkUnits.clear();
    completedStages.clear();
    stageResults.clear();
  }
}
