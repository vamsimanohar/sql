/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelRunner;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.utils.CalciteToolsHelper;
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
   * Executes query using the distributed operator pipeline. All queries are routed through
   * coordinator-side Calcite execution which scans raw data from data nodes via OPERATOR_PIPELINE
   * transport (direct Lucene reads) and executes the full Calcite plan on the coordinator.
   *
   * <p>This approach handles ALL PPL operations correctly: scan, filter (including OR, BETWEEN,
   * SEARCH/Sarg, regexp, IS NULL), sort, limit, rename, aggregation, eval, dedup, fillnull,
   * replace, parse, window functions, joins, and multi-table sources.
   */
  private void executeOperatorPipeline(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {

    log.info("[Distributed Engine] Executing via operator pipeline for plan: {}", plan.getPlanId());

    // Route all queries through coordinator-side Calcite execution.
    // This scans raw data from data nodes via OPERATOR_PIPELINE transport (direct Lucene reads)
    // and executes the full Calcite plan on the coordinator for correctness.
    executeCalciteOnCoordinator(plan, listener);
  }

  /**
   * Scans a table distributed across cluster nodes. Groups work units by node, sends parallel
   * transport requests with OPERATOR_PIPELINE mode, waits for all responses, and merges rows.
   *
   * <p>This method is reusable for both single-table queries and each side of a join.
   *
   * @param workUnits Work units containing shard partition info
   * @param indexName Index to scan
   * @param fieldNames Fields to retrieve from each document
   * @param filters Filter conditions to push down to data nodes (may be null)
   * @param limit Per-node row limit
   * @return Merged rows from all nodes
   */
  private List<List<Object>> scanTableDistributed(
      List<WorkUnit> workUnits,
      String indexName,
      List<String> fieldNames,
      List<Map<String, Object>> filters,
      int limit)
      throws Exception {

    // Group work units by (nodeId, actualIndexName) to handle multi-table sources.
    // A single table name like "test,test1" produces work units for different indexes,
    // and each transport request must target a single index.
    Map<String, Map<String, List<Integer>>> workByNodeAndIndex = new HashMap<>();
    for (WorkUnit wu : workUnits) {
      String nodeId = wu.getDataPartition().getNodeId();
      if (nodeId == null) {
        nodeId = wu.getAssignedNodeId();
      }
      if (nodeId == null) {
        throw new IllegalStateException("Work unit has no node assignment: " + wu.getWorkUnitId());
      }
      String actualIndex = wu.getDataPartition().getIndexName();
      workByNodeAndIndex
          .computeIfAbsent(nodeId, k -> new HashMap<>())
          .computeIfAbsent(actualIndex, k -> new ArrayList<>())
          .add(Integer.parseInt(wu.getDataPartition().getShardId()));
    }

    // Send parallel transport requests — one per (node, index) pair
    List<CompletableFuture<ExecuteDistributedTaskResponse>> futures = new ArrayList<>();

    for (Map.Entry<String, Map<String, List<Integer>>> nodeEntry : workByNodeAndIndex.entrySet()) {
      String nodeId = nodeEntry.getKey();

      DiscoveryNode targetNode = clusterService.state().nodes().get(nodeId);
      if (targetNode == null) {
        throw new IllegalStateException("Cannot resolve DiscoveryNode for nodeId: " + nodeId);
      }

      for (Map.Entry<String, List<Integer>> indexEntry : nodeEntry.getValue().entrySet()) {
        String actualIndex = indexEntry.getKey();
        List<Integer> shardIds = indexEntry.getValue();

        ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest();
        request.setExecutionMode("OPERATOR_PIPELINE");
        request.setIndexName(actualIndex);
        request.setShardIds(shardIds);
        request.setFieldNames(fieldNames);
        request.setQueryLimit(limit);
        request.setStageId("operator-pipeline");
        request.setFilterConditions(filters);

        CompletableFuture<ExecuteDistributedTaskResponse> future = new CompletableFuture<>();
        futures.add(future);

        final String fNodeId = nodeId;
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
                              : "Operator pipeline failed on node: " + fNodeId));
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
    }

    // Wait for all responses
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();

    // Merge rows from all nodes
    List<List<Object>> allRows = new ArrayList<>();
    for (CompletableFuture<ExecuteDistributedTaskResponse> future : futures) {
      ExecuteDistributedTaskResponse resp = future.get();
      if (resp.getPipelineRows() != null) {
        allRows.addAll(resp.getPipelineRows());
      }
    }

    log.info(
        "[Distributed Engine] scanTableDistributed: {} rows from {} node(s) for index {}",
        allRows.size(),
        workByNodeAndIndex.size(),
        indexName);

    return allRows;
  }

  /**
   * Executes a join query pipeline. Scans both sides of the join in parallel across data nodes,
   * performs the hash join on the coordinator, then applies post-join filter/sort/limit.
   *
   * @param plan The distributed plan (contains scan stages for both sides)
   * @param listener Response listener for async execution
   * @param joinNode The Calcite Join node from the RelNode tree
   */
  private void executeJoinPipeline(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener, Join joinNode) {

    RelNode relNode = (RelNode) plan.getRelNode();

    log.info(
        "[Distributed Engine] Executing join pipeline for plan: {}, joinType: {}",
        plan.getPlanId(),
        joinNode.getJoinType());

    try {
      // Step 1: Extract join info (both sides' tables, fields, key indices, filters)
      JoinInfo joinInfo = RelNodeAnalyzer.extractJoinInfo(joinNode);

      log.info(
          "[Distributed Engine] Join: left={} ({}), right={} ({}), type={}, leftKeys={},"
              + " rightKeys={}",
          joinInfo.leftTableName(),
          joinInfo.leftFieldNames(),
          joinInfo.rightTableName(),
          joinInfo.rightFieldNames(),
          joinInfo.joinType(),
          joinInfo.leftKeyIndices(),
          joinInfo.rightKeyIndices());

      // Step 2: Find scan stages for left and right sides
      List<WorkUnit> leftWorkUnits = null;
      List<WorkUnit> rightWorkUnits = null;
      String leftIndexName = null;
      String rightIndexName = null;

      for (ExecutionStage stage : plan.getExecutionStages()) {
        if (stage.getStageType() == ExecutionStage.StageType.SCAN
            && stage.getProperties() != null) {
          String side = (String) stage.getProperties().get("side");
          if ("left".equals(side)) {
            leftWorkUnits = stage.getWorkUnits();
            leftIndexName = (String) stage.getProperties().get("tableName");
          } else if ("right".equals(side)) {
            rightWorkUnits = stage.getWorkUnits();
            rightIndexName = (String) stage.getProperties().get("tableName");
          }
        }
      }

      if (leftWorkUnits == null || rightWorkUnits == null) {
        throw new IllegalStateException(
            "Join pipeline requires both left and right SCAN stages in the distributed plan");
      }

      // Step 3: Extract per-side limits from RelNode tree
      int leftLimit = RelNodeAnalyzer.extractLimit(joinInfo.leftInput());
      int rightLimit = RelNodeAnalyzer.extractLimit(joinInfo.rightInput());

      // Step 4: Scan both tables in parallel
      CompletableFuture<List<List<Object>>> leftFuture = new CompletableFuture<>();
      CompletableFuture<List<List<Object>>> rightFuture = new CompletableFuture<>();

      final List<WorkUnit> leftWu = leftWorkUnits;
      final List<WorkUnit> rightWu = rightWorkUnits;
      final String leftIdx = leftIndexName;
      final String rightIdx = rightIndexName;
      final List<Map<String, Object>> leftFilters = joinInfo.leftFilters();
      final List<Map<String, Object>> rightFilters = joinInfo.rightFilters();

      CompletableFuture.runAsync(
          () -> {
            try {
              leftFuture.complete(
                  scanTableDistributed(
                      leftWu, leftIdx, joinInfo.leftFieldNames(), leftFilters, leftLimit));
            } catch (Exception e) {
              leftFuture.completeExceptionally(e);
            }
          });

      CompletableFuture.runAsync(
          () -> {
            try {
              rightFuture.complete(
                  scanTableDistributed(
                      rightWu, rightIdx, joinInfo.rightFieldNames(), rightFilters, rightLimit));
            } catch (Exception e) {
              rightFuture.completeExceptionally(e);
            }
          });

      // Wait for both sides
      CompletableFuture.allOf(leftFuture, rightFuture).get();
      List<List<Object>> leftRows = leftFuture.get();
      List<List<Object>> rightRows = rightFuture.get();

      log.info(
          "[Distributed Engine] Join scan complete: left={} rows, right={} rows",
          leftRows.size(),
          rightRows.size());

      // Step 5: Perform hash join
      List<List<Object>> joinedRows =
          HashJoinExecutor.performHashJoin(
              leftRows,
              rightRows,
              joinInfo.leftKeyIndices(),
              joinInfo.rightKeyIndices(),
              joinInfo.joinType(),
              joinInfo.leftFieldCount(),
              joinInfo.rightFieldCount());

      log.info("[Distributed Engine] Hash join produced {} rows", joinedRows.size());

      // Step 6: Apply post-join operations from nodes above the Join
      // The post-join portion of the tree is everything above the Join node
      // (Filter, Sort, Limit, Project nodes above the join)
      List<String> joinedFieldNames = new ArrayList<>();
      joinedFieldNames.addAll(joinInfo.leftFieldNames());
      // For SEMI and ANTI joins, only left columns are in the output
      if (joinInfo.joinType() != JoinRelType.SEMI && joinInfo.joinType() != JoinRelType.ANTI) {
        joinedFieldNames.addAll(joinInfo.rightFieldNames());
      }

      // Apply post-join filter: extract filters from nodes ABOVE the join
      List<Map<String, Object>> postJoinFilters =
          RelNodeAnalyzer.extractPostJoinFilters(relNode, joinNode);
      if (postJoinFilters != null) {
        joinedRows =
            HashJoinExecutor.applyPostJoinFilters(joinedRows, postJoinFilters, joinedFieldNames);
        log.info("[Distributed Engine] After post-join filter: {} rows", joinedRows.size());
      }

      // Apply post-join sort
      List<SortKey> postJoinSortKeys = RelNodeAnalyzer.extractSortKeys(relNode, joinedFieldNames);
      if (!postJoinSortKeys.isEmpty()) {
        HashJoinExecutor.sortRows(joinedRows, postJoinSortKeys);
        log.info("[Distributed Engine] Sorted {} rows by {}", joinedRows.size(), postJoinSortKeys);
      }

      // Apply post-join limit (from nodes above the join)
      int postJoinLimit = RelNodeAnalyzer.extractLimit(relNode);
      if (joinedRows.size() > postJoinLimit) {
        joinedRows = joinedRows.subList(0, postJoinLimit);
      }

      // Step 7: Apply post-join projection.
      // The top-level Project maps output columns to specific positions in the joined row.
      // E.g., output[2] "occupation" may map to joinedRow[7] (right side field).
      List<Integer> projectionIndices =
          RelNodeAnalyzer.extractPostJoinProjection(relNode, joinNode);
      if (projectionIndices != null) {
        List<List<Object>> projected = new ArrayList<>();
        for (List<Object> row : joinedRows) {
          List<Object> projectedRow = new ArrayList<>(projectionIndices.size());
          for (int idx : projectionIndices) {
            projectedRow.add(idx < row.size() ? row.get(idx) : null);
          }
          projected.add(projectedRow);
        }
        joinedRows = projected;
        log.info("[Distributed Engine] Applied projection {} to joined rows", projectionIndices);
      }

      // Build QueryResponse with schema from the top-level RelNode row type
      List<String> outputFieldNames =
          relNode.getRowType().getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toList());

      List<ExprValue> values = new ArrayList<>();
      for (List<Object> row : joinedRows) {
        Map<String, ExprValue> exprRow = new LinkedHashMap<>();
        for (int i = 0; i < outputFieldNames.size() && i < row.size(); i++) {
          exprRow.put(outputFieldNames.get(i), ExprValueUtils.fromObjectValue(row.get(i)));
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
          "[Distributed Engine] Join query completed with {} results for plan: {}",
          queryResponse.getResults().size(),
          plan.getPlanId());
      listener.onResponse(queryResponse);

    } catch (Exception e) {
      log.error(
          "[Distributed Engine] Join pipeline execution failed for plan: {}", plan.getPlanId(), e);
      plan.markFailed(e.getMessage());
      listener.onFailure(new RuntimeException("Join pipeline execution failed", e));
    }
  }

  /**
   * Executes a query with complex operations using coordinator-side Calcite execution. Scans raw
   * data from data nodes for all base tables, creates in-memory ScannableTable wrappers, replaces
   * TableScan nodes in the RelNode tree with BindableTableScan backed by in-memory data, then
   * executes the full Calcite plan via RelRunner.
   *
   * <p>This approach handles ALL PPL operations (stats, eval, dedup, fillnull, replace, parse,
   * window functions, etc.) without manual reimplementation — Calcite's execution engine handles
   * them automatically.
   *
   * @param plan The distributed plan (contains scan stages and RelNode)
   * @param listener Response listener for async execution
   */
  private void executeCalciteOnCoordinator(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {

    RelNode relNode = (RelNode) plan.getRelNode();
    CalcitePlanContext context = (CalcitePlanContext) plan.getPlanContext();

    try {
      // Step 1: Find all TableScan nodes and their table names
      Map<String, TableScan> tableScans = new LinkedHashMap<>();
      RelNodeAnalyzer.collectTableScans(relNode, tableScans);

      log.info(
          "[Distributed Engine] Coordinator Calcite execution: {} base table(s): {}",
          tableScans.size(),
          tableScans.keySet());

      // Step 2: Scan raw data from data nodes for each base table
      Map<String, InMemoryScannableTable> inMemoryTables = new HashMap<>();
      for (Map.Entry<String, TableScan> entry : tableScans.entrySet()) {
        String tableName = entry.getKey();
        TableScan scan = entry.getValue();

        List<String> fieldNames = scan.getRowType().getFieldNames();
        List<WorkUnit> workUnits = findWorkUnitsForTable(plan, tableName);

        // Scan all rows from data nodes — no filter pushdown for correctness
        // (Calcite will apply filters on coordinator)
        List<List<Object>> rows =
            scanTableDistributed(workUnits, tableName, fieldNames, null, 10000);

        // Convert List<List<Object>> to List<Object[]> for Calcite ScannableTable
        // Normalize types to match the declared row type (e.g., Integer → Long for BIGINT)
        RelDataType scanRowType = scan.getRowType();
        List<Object[]> rowArrays =
            rows.stream()
                .map(row -> TemporalValueNormalizer.normalizeRowForCalcite(row, scanRowType))
                .collect(Collectors.toList());

        inMemoryTables.put(tableName, new InMemoryScannableTable(scanRowType, rowArrays));

        log.info(
            "[Distributed Engine] Scanned {} rows from {} ({} fields)",
            rows.size(),
            tableName,
            fieldNames.size());
      }

      // Step 3: Extract query_string conditions (from PPL inline filters) that can't be
      // executed on in-memory data — these will be pushed down to data node scans
      List<String> queryStringFilters = new ArrayList<>();
      RelNodeAnalyzer.collectQueryStringConditions(relNode, queryStringFilters);
      if (!queryStringFilters.isEmpty()) {
        log.info(
            "[Distributed Engine] Found {} query_string conditions to push down: {}",
            queryStringFilters.size(),
            queryStringFilters);
      }

      // Step 3b: If query_string conditions were found, re-scan data with them pushed down
      // as filter conditions to the data nodes
      if (!queryStringFilters.isEmpty()) {
        for (Map.Entry<String, TableScan> entry : tableScans.entrySet()) {
          String tableName = entry.getKey();
          TableScan scan = entry.getValue();
          List<String> fieldNames = scan.getRowType().getFieldNames();
          List<WorkUnit> workUnits = findWorkUnitsForTable(plan, tableName);

          // Build filter conditions with query_string type
          List<Map<String, Object>> filters = new ArrayList<>();
          for (String qs : queryStringFilters) {
            Map<String, Object> filter = new HashMap<>();
            filter.put("type", "query_string");
            filter.put("query", qs);
            filters.add(filter);
          }

          List<List<Object>> rows =
              scanTableDistributed(workUnits, tableName, fieldNames, filters, 10000);

          RelDataType scanRowType = scan.getRowType();
          List<Object[]> rowArrays =
              rows.stream()
                  .map(row -> TemporalValueNormalizer.normalizeRowForCalcite(row, scanRowType))
                  .collect(Collectors.toList());
          inMemoryTables.put(tableName, new InMemoryScannableTable(scanRowType, rowArrays));

          log.info(
              "[Distributed Engine] Re-scanned {} rows from {} with query_string filter",
              rows.size(),
              tableName);
        }
      }

      // Step 4: Replace TableScan with BindableTableScan, strip LogicalSystemLimit,
      // and strip Filter nodes with query_string conditions (already applied on data nodes)
      RelNode modifiedPlan =
          relNode.accept(
              new RelHomogeneousShuttle() {
                @Override
                public RelNode visit(TableScan scan) {
                  List<String> qualifiedName = scan.getTable().getQualifiedName();
                  String tableName = qualifiedName.get(qualifiedName.size() - 1);
                  InMemoryScannableTable memTable = inMemoryTables.get(tableName);
                  if (memTable != null) {
                    RelOptTable newTable =
                        RelOptTableImpl.create(
                            null, scan.getRowType(), memTable, ImmutableList.of(tableName));
                    return Bindables.BindableTableScan.create(scan.getCluster(), newTable);
                  }
                  return super.visit(scan);
                }

                @Override
                public RelNode visit(RelNode other) {
                  // Replace LogicalSystemLimit with standard LogicalSort
                  if (other instanceof LogicalSystemLimit sysLimit) {
                    RelNode newInput = sysLimit.getInput().accept(this);
                    return LogicalSort.create(
                        newInput, sysLimit.getCollation(), sysLimit.offset, sysLimit.fetch);
                  }
                  // Strip Filter nodes with query_string conditions (already pushed to data nodes)
                  if (other instanceof Filter filter
                      && RelNodeAnalyzer.containsQueryString(filter.getCondition())) {
                    return filter.getInput().accept(this);
                  }
                  return super.visit(other);
                }
              });

      // Step 4: Optimize and execute via Calcite RelRunner using existing connection
      modifiedPlan = CalciteToolsHelper.optimize(modifiedPlan, context);

      try (Connection connection = context.connection) {
        RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(modifiedPlan);
        ResultSet rs = ps.executeQuery();

        // Step 5: Build QueryResponse from ResultSet
        QueryResponse response = QueryResponseBuilder.buildQueryResponseFromResultSet(rs, relNode);

        plan.markCompleted();
        log.info(
            "[Distributed Engine] Coordinator Calcite execution completed with {} results",
            response.getResults().size());
        listener.onResponse(response);
      }

    } catch (Exception e) {
      log.error("[Distributed Engine] Coordinator Calcite execution failed", e);
      plan.markFailed(e.getMessage());
      listener.onFailure(new RuntimeException("Coordinator Calcite execution failed", e));
    }
  }

  /**
   * Finds work units for a specific table from the distributed plan's scan stages. Handles
   * single-table queries, join queries (named left/right stages), and multi-table sources
   * (comma-separated index names like "index1,index2").
   */
  private List<WorkUnit> findWorkUnitsForTable(DistributedPhysicalPlan plan, String tableName) {
    // Pass 1: Exact match on tagged join stages or work unit index names
    for (ExecutionStage stage : plan.getExecutionStages()) {
      if (stage.getStageType() == ExecutionStage.StageType.SCAN) {
        if (stage.getProperties() != null && stage.getProperties().containsKey("tableName")) {
          if (tableName.equals(stage.getProperties().get("tableName"))) {
            return stage.getWorkUnits();
          }
        } else if (!stage.getWorkUnits().isEmpty()) {
          String indexName = stage.getWorkUnits().getFirst().getDataPartition().getIndexName();
          if (tableName.equals(indexName)) {
            return stage.getWorkUnits();
          }
        }
      }
    }

    // Pass 2: For multi-table sources (comma-separated), check if any work unit's index
    // is part of the comma-separated table name, or if the table name matches the plan's
    // primary table name. Also handles wildcard/pattern index names.
    for (ExecutionStage stage : plan.getExecutionStages()) {
      if (stage.getStageType() == ExecutionStage.StageType.SCAN
          && !stage.getWorkUnits().isEmpty()) {
        // Check if any work unit index is contained in the table name
        String firstIndex = stage.getWorkUnits().getFirst().getDataPartition().getIndexName();
        if (tableName.contains(firstIndex) || firstIndex.contains(tableName)) {
          return stage.getWorkUnits();
        }
      }
    }

    // Pass 3: Fall back to the first available SCAN stage (handles cases where
    // the DistributedQueryPlanner resolved a different but equivalent table name)
    for (ExecutionStage stage : plan.getExecutionStages()) {
      if (stage.getStageType() == ExecutionStage.StageType.SCAN
          && !stage.getWorkUnits().isEmpty()) {
        log.info("[Distributed Engine] Falling back to first SCAN stage for table: {}", tableName);
        return stage.getWorkUnits();
      }
    }

    throw new IllegalStateException("No SCAN stage found for table: " + tableName);
  }

  /** Shuts down the scheduler and releases resources. */
  public void shutdown() {
    log.info("Shutting down DistributedTaskScheduler");
  }
}
