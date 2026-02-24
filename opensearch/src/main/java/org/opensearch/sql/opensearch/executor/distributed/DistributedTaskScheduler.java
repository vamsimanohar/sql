/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.DataContext;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
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
   * Executes query using operator pipeline with direct Lucene access. Extracts index name, field
   * names, and limit from the RelNode tree, groups shards by node, sends parallel transport
   * requests with OPERATOR_PIPELINE mode, and merges results.
   */
  private void executeOperatorPipeline(
      DistributedPhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    RelNode relNode = (RelNode) plan.getRelNode();

    log.info("[Distributed Engine] Executing via operator pipeline for plan: {}", plan.getPlanId());

    // Check for complex operations — dispatch to coordinator-side Calcite execution
    // This handles: stats, eval, dedup, fillnull, replace, window functions, etc.
    if (hasComplexOperations(relNode)) {
      log.info(
          "[Distributed Engine] Complex operations detected, using coordinator-side Calcite"
              + " execution");
      executeCalciteOnCoordinator(plan, listener);
      return;
    }

    // Check for join and dispatch to join pipeline
    Join joinNode = findJoinNode(relNode);
    if (joinNode != null) {
      executeJoinPipeline(plan, listener, joinNode);
      return;
    }

    try {
      // Step 1: Extract output field names and resolve physical (scan-level) field names.
      // A Project may rename fields (e.g., rename firstname as first_name), so we need
      // to send physical names to data nodes and map results back to output names.
      List<String> outputFieldNames =
          relNode.getRowType().getFieldList().stream()
              .map(RelDataTypeField::getName)
              .collect(Collectors.toList());

      // Resolve each output field to its physical scan field name
      List<FieldMapping> fieldMappings = resolveFieldMappings(relNode);
      List<String> physicalFieldNames =
          fieldMappings.stream().map(fm -> fm.physicalName).collect(Collectors.toList());

      // Determine which fields to request from data nodes (physical names)
      // and the mapping back to output names
      boolean hasRenames =
          !outputFieldNames.equals(physicalFieldNames)
              && fieldMappings.stream().allMatch(fm -> fm.physicalName != null);

      // Use physical field names for data node requests
      List<String> requestFieldNames = hasRenames ? physicalFieldNames : outputFieldNames;

      // Step 2: Extract limit from RelNode tree
      int queryLimit = extractLimit(relNode);

      // Step 3: Get shard partitions from the SCAN stage work units
      List<WorkUnit> scanWorkUnits = getScanStageWorkUnits(plan);
      String indexName = scanWorkUnits.getFirst().getDataPartition().getIndexName();

      // Step 3b: Extract filter conditions from RelNode tree
      List<Map<String, Object>> filterConditions = extractFilters(relNode);

      // Step 3c: Extract sort keys from RelNode tree (use physical names for sort resolution)
      List<SortKey> sortKeys = extractSortKeys(relNode, requestFieldNames);

      log.info(
          "[Distributed Engine] index: {}, outputFields: {}, requestFields: {}, limit: {},"
              + " shards: {}, filters: {}, sortKeys: {}",
          indexName,
          outputFieldNames,
          requestFieldNames,
          queryLimit,
          scanWorkUnits.size(),
          filterConditions != null ? filterConditions.size() : 0,
          sortKeys);

      // Step 4-7: Scan table distributed (group by node, transport, wait, merge)
      List<List<Object>> allRows =
          scanTableDistributed(
              scanWorkUnits, indexName, requestFieldNames, filterConditions, queryLimit);

      // Apply coordinator-side sort if sort keys present
      if (!sortKeys.isEmpty()) {
        sortRows(allRows, sortKeys);
        log.info("[Distributed Engine] Sorted {} rows by {}", allRows.size(), sortKeys);
      }

      // Apply final limit
      if (allRows.size() > queryLimit) {
        allRows = allRows.subList(0, queryLimit);
      }

      log.info("[Distributed Engine] Total {} rows after sort/limit", allRows.size());

      // Step 8: Build QueryResponse with schema from RelNode row type.
      // Use output field names (may differ from physical names due to rename).
      List<ExprValue> values = new ArrayList<>();
      for (List<Object> row : allRows) {
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

    // Group work units by node
    Map<String, List<WorkUnit>> workByNode = new HashMap<>();
    for (WorkUnit wu : workUnits) {
      String nodeId = wu.getDataPartition().getNodeId();
      if (nodeId == null) {
        nodeId = wu.getAssignedNodeId();
      }
      if (nodeId == null) {
        throw new IllegalStateException("Work unit has no node assignment: " + wu.getWorkUnitId());
      }
      workByNode.computeIfAbsent(nodeId, k -> new ArrayList<>()).add(wu);
    }

    // Send parallel transport requests to each node
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
      request.setQueryLimit(limit);
      request.setStageId("operator-pipeline");
      request.setFilterConditions(filters);

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
        "[Distributed Engine] scanTableDistributed: {} rows from {} nodes for index {}",
        allRows.size(),
        workByNode.size(),
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
      JoinInfo joinInfo = extractJoinInfo(joinNode);

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
      int leftLimit = extractLimit(joinInfo.leftInput());
      int rightLimit = extractLimit(joinInfo.rightInput());

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
          performHashJoin(
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
      List<Map<String, Object>> postJoinFilters = extractPostJoinFilters(relNode, joinNode);
      if (postJoinFilters != null) {
        joinedRows = applyPostJoinFilters(joinedRows, postJoinFilters, joinedFieldNames);
        log.info("[Distributed Engine] After post-join filter: {} rows", joinedRows.size());
      }

      // Apply post-join sort
      List<SortKey> postJoinSortKeys = extractSortKeys(relNode, joinedFieldNames);
      if (!postJoinSortKeys.isEmpty()) {
        sortRows(joinedRows, postJoinSortKeys);
        log.info("[Distributed Engine] Sorted {} rows by {}", joinedRows.size(), postJoinSortKeys);
      }

      // Apply post-join limit (from nodes above the join)
      int postJoinLimit = extractLimit(relNode);
      if (joinedRows.size() > postJoinLimit) {
        joinedRows = joinedRows.subList(0, postJoinLimit);
      }

      // Step 7: Apply post-join projection.
      // The top-level Project maps output columns to specific positions in the joined row.
      // E.g., output[2] "occupation" may map to joinedRow[7] (right side field).
      List<Integer> projectionIndices = extractPostJoinProjection(relNode, joinNode);
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
   * Extracts column index mappings from Project nodes above the Join. Returns a list of source
   * column indices that the Project selects from the joined row, or null if no Project is found
   * above the join.
   */
  private List<Integer> extractPostJoinProjection(RelNode node, Join joinNode) {
    if (node == joinNode) {
      return null;
    }
    if (node instanceof Project project) {
      List<Integer> indices = new ArrayList<>();
      for (RexNode expr : project.getProjects()) {
        if (expr instanceof RexInputRef ref) {
          indices.add(ref.getIndex());
        } else {
          // Non-simple projection (computed column) — fall back to identity
          return null;
        }
      }
      return indices;
    }
    // Look through Sort/Limit nodes to find Project
    for (RelNode input : node.getInputs()) {
      List<Integer> result = extractPostJoinProjection(input, joinNode);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  /**
   * Extracts filter conditions from nodes ABOVE the join (post-join filters). Walks only up to the
   * join node and collects filters from the portion of the tree above it.
   */
  private List<Map<String, Object>> extractPostJoinFilters(RelNode root, Join joinNode) {
    List<Map<String, Object>> conditions = new ArrayList<>();
    collectPostJoinFilters(root, joinNode, conditions);
    return conditions.isEmpty() ? null : conditions;
  }

  private void collectPostJoinFilters(
      RelNode node, Join joinNode, List<Map<String, Object>> conditions) {
    // Stop recursion when we reach the join node
    if (node == joinNode) {
      return;
    }
    if (node instanceof Filter filter) {
      RexNode condition = filter.getCondition();
      RelDataType inputRowType = filter.getInput().getRowType();
      convertRexToConditions(condition, inputRowType, conditions);
    }
    for (RelNode input : node.getInputs()) {
      collectPostJoinFilters(input, joinNode, conditions);
    }
  }

  /**
   * Applies post-join filter conditions on the coordinator. Evaluates each row against the filter
   * conditions and returns only matching rows.
   */
  private List<List<Object>> applyPostJoinFilters(
      List<List<Object>> rows, List<Map<String, Object>> filters, List<String> fieldNames) {
    List<List<Object>> filtered = new ArrayList<>();
    for (List<Object> row : rows) {
      if (matchesFilters(row, filters, fieldNames)) {
        filtered.add(row);
      }
    }
    return filtered;
  }

  /** Evaluates whether a row matches all filter conditions. */
  @SuppressWarnings("unchecked")
  private boolean matchesFilters(
      List<Object> row, List<Map<String, Object>> filters, List<String> fieldNames) {
    for (Map<String, Object> filter : filters) {
      String field = (String) filter.get("field");
      String op = (String) filter.get("op");
      Object filterValue = filter.get("value");

      int fieldIndex = fieldNames.indexOf(field);
      if (fieldIndex < 0 || fieldIndex >= row.size()) {
        return false;
      }

      Object rowValue = row.get(fieldIndex);
      if (rowValue == null) {
        return false;
      }

      int cmp;
      if (rowValue instanceof Comparable && filterValue instanceof Comparable) {
        try {
          cmp = ((Comparable<Object>) rowValue).compareTo(filterValue);
        } catch (ClassCastException e) {
          // Type mismatch: try numeric comparison
          if (rowValue instanceof Number && filterValue instanceof Number) {
            cmp =
                Double.compare(
                    ((Number) rowValue).doubleValue(), ((Number) filterValue).doubleValue());
          } else {
            cmp = rowValue.toString().compareTo(filterValue.toString());
          }
        }
      } else {
        cmp = rowValue.toString().compareTo(filterValue.toString());
      }

      boolean passes =
          switch (op) {
            case "EQ" -> cmp == 0;
            case "NEQ" -> cmp != 0;
            case "GT" -> cmp > 0;
            case "GTE" -> cmp >= 0;
            case "LT" -> cmp < 0;
            case "LTE" -> cmp <= 0;
            default -> true;
          };

      if (!passes) {
        return false;
      }
    }
    return true;
  }

  /**
   * Resolves output field names to physical (scan-level) field names by walking through Project
   * nodes. A Project may rename fields (e.g., rename firstname as first_name), so the output name
   * differs from the physical index field name.
   *
   * <p>Returns a list of FieldMapping(outputName, physicalName) for each output column. If a field
   * is computed (RexCall, not a simple field reference), physicalName will be null.
   */
  private List<FieldMapping> resolveFieldMappings(RelNode node) {
    List<String> outputNames = node.getRowType().getFieldNames();

    // Walk down through Projects to resolve to scan-level field names
    Map<Integer, String> indexToPhysical = resolveToScanFields(node);

    List<FieldMapping> mappings = new ArrayList<>();
    for (int i = 0; i < outputNames.size(); i++) {
      String physical = indexToPhysical.getOrDefault(i, outputNames.get(i));
      mappings.add(new FieldMapping(outputNames.get(i), physical));
    }
    return mappings;
  }

  /**
   * Recursively resolves output column indices to physical scan field names. Returns a map from
   * output column index → physical field name.
   */
  private Map<Integer, String> resolveToScanFields(RelNode node) {
    if (node instanceof TableScan) {
      // Base case: scan fields are the physical field names
      List<String> scanFields = node.getRowType().getFieldNames();
      Map<Integer, String> result = new HashMap<>();
      for (int i = 0; i < scanFields.size(); i++) {
        result.put(i, scanFields.get(i));
      }
      return result;
    }

    if (node instanceof Project project) {
      // Resolve input fields first
      Map<Integer, String> inputPhysical = resolveToScanFields(project.getInput());

      // Map each Project output to its input field
      Map<Integer, String> result = new HashMap<>();
      List<RexNode> projects = project.getProjects();
      for (int i = 0; i < projects.size(); i++) {
        RexNode expr = projects.get(i);
        if (expr instanceof RexInputRef ref) {
          String physical = inputPhysical.get(ref.getIndex());
          if (physical != null) {
            result.put(i, physical);
          }
        }
        // RexCall (computed expression): physicalName stays null (not in result map)
      }
      return result;
    }

    // Sort, Filter, SystemLimit: pass through to input
    if (!node.getInputs().isEmpty()) {
      return resolveToScanFields(node.getInputs().getFirst());
    }

    return new HashMap<>();
  }

  /** Maps an output field name to its physical scan-level field name. */
  private record FieldMapping(String outputName, String physicalName) {}

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

  /**
   * Extracts filter conditions from the RelNode tree. Walks the tree to find Filter nodes and
   * converts their RexNode conditions to serializable filter condition maps.
   *
   * @param node the root RelNode
   * @return list of filter condition maps, or null if no filters
   */
  private List<Map<String, Object>> extractFilters(RelNode node) {
    List<Map<String, Object>> conditions = new ArrayList<>();
    collectFilters(node, conditions);
    return conditions.isEmpty() ? null : conditions;
  }

  private void collectFilters(RelNode node, List<Map<String, Object>> conditions) {
    if (node instanceof Filter filter) {
      RexNode condition = filter.getCondition();
      RelDataType inputRowType = filter.getInput().getRowType();
      convertRexToConditions(condition, inputRowType, conditions);
    }
    // Recurse into children
    for (RelNode input : node.getInputs()) {
      collectFilters(input, conditions);
    }
  }

  /**
   * Converts a Calcite RexNode expression to filter condition maps. Handles comparison operators
   * (=, !=, >, >=, <, <=) and boolean AND/OR.
   */
  private void convertRexToConditions(
      RexNode rexNode, RelDataType rowType, List<Map<String, Object>> conditions) {
    if (!(rexNode instanceof RexCall call)) {
      return;
    }

    switch (call.getKind()) {
      case AND -> {
        // Flatten AND into multiple conditions (all ANDed)
        for (RexNode operand : call.getOperands()) {
          convertRexToConditions(operand, rowType, conditions);
        }
      }
      case EQUALS -> addComparisonCondition(call, rowType, "EQ", conditions);
      case NOT_EQUALS -> addComparisonCondition(call, rowType, "NEQ", conditions);
      case GREATER_THAN -> addComparisonCondition(call, rowType, "GT", conditions);
      case GREATER_THAN_OR_EQUAL -> addComparisonCondition(call, rowType, "GTE", conditions);
      case LESS_THAN -> addComparisonCondition(call, rowType, "LT", conditions);
      case LESS_THAN_OR_EQUAL -> addComparisonCondition(call, rowType, "LTE", conditions);
      default ->
          log.warn(
              "[Distributed Engine] Unsupported filter operator: {}, condition: {}",
              call.getKind(),
              call);
    }
  }

  private void addComparisonCondition(
      RexCall call, RelDataType rowType, String op, List<Map<String, Object>> conditions) {
    if (call.getOperands().size() != 2) {
      return;
    }
    String field = resolveFieldName(call.getOperands().get(0), rowType);
    Object value = resolveLiteralValue(call.getOperands().get(1));

    // Handle reversed operands: literal <op> field
    if (field == null && value == null) {
      return;
    }
    if (field == null) {
      field = resolveFieldName(call.getOperands().get(1), rowType);
      value = resolveLiteralValue(call.getOperands().get(0));
      // Reverse the operator
      op = reverseOp(op);
    }
    if (field == null || value == null) {
      return;
    }

    Map<String, Object> condition = new HashMap<>();
    condition.put("field", field);
    condition.put("op", op);
    condition.put("value", value);
    conditions.add(condition);

    log.debug("[Distributed Engine] Extracted filter: {} {} {}", field, op, value);
  }

  private String resolveFieldName(RexNode node, RelDataType rowType) {
    if (node instanceof RexInputRef ref) {
      List<String> fieldNames = rowType.getFieldNames();
      if (ref.getIndex() < fieldNames.size()) {
        return fieldNames.get(ref.getIndex());
      }
    }
    // Handle CAST(field AS type)
    if (node instanceof RexCall cast && cast.getKind() == SqlKind.CAST) {
      return resolveFieldName(cast.getOperands().get(0), rowType);
    }
    return null;
  }

  private Object resolveLiteralValue(RexNode node) {
    if (node instanceof RexLiteral literal) {
      // getValue2() returns native Java types: String, Integer, Long, etc.
      return literal.getValue2();
    }
    // Handle CAST(literal AS type)
    if (node instanceof RexCall cast && cast.getKind() == SqlKind.CAST) {
      return resolveLiteralValue(cast.getOperands().get(0));
    }
    return null;
  }

  private String reverseOp(String op) {
    return switch (op) {
      case "GT" -> "LT";
      case "GTE" -> "LTE";
      case "LT" -> "GT";
      case "LTE" -> "GTE";
      default -> op;
    };
  }

  /**
   * Extracts sort keys from the RelNode tree. Walks the tree to find Sort nodes (excluding
   * LogicalSystemLimit) and extracts field index + direction for each sort key.
   *
   * @param node the root RelNode
   * @param fieldNames the field names from the row type (used to resolve field indices)
   * @return list of sort keys, or empty if no sort
   */
  private List<SortKey> extractSortKeys(RelNode node, List<String> fieldNames) {
    List<SortKey> keys = new ArrayList<>();
    collectSortKeys(node, fieldNames, keys);
    return keys;
  }

  private void collectSortKeys(RelNode node, List<String> fieldNames, List<SortKey> keys) {
    if (node instanceof Sort sort && !(node instanceof LogicalSystemLimit)) {
      RelCollation collation = sort.getCollation();
      if (collation != null && !collation.getFieldCollations().isEmpty()) {
        // Use the sort node's input row type to resolve field names
        List<String> sortFieldNames = sort.getInput().getRowType().getFieldNames();
        for (RelFieldCollation fc : collation.getFieldCollations()) {
          int fieldIndex = fc.getFieldIndex();
          String fieldName =
              fieldIndex < sortFieldNames.size() ? sortFieldNames.get(fieldIndex) : null;
          if (fieldName != null) {
            // Map sort field name to output field index
            int outputIndex = fieldNames.indexOf(fieldName);
            if (outputIndex >= 0) {
              boolean descending =
                  fc.getDirection() == RelFieldCollation.Direction.DESCENDING
                      || fc.getDirection() == RelFieldCollation.Direction.STRICTLY_DESCENDING;
              boolean nullsLast =
                  fc.nullDirection == RelFieldCollation.NullDirection.LAST
                      || (fc.nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED
                          && descending);
              keys.add(new SortKey(fieldName, outputIndex, descending, nullsLast));
            }
          }
        }
      }
    }
    for (RelNode input : node.getInputs()) {
      collectSortKeys(input, fieldNames, keys);
    }
  }

  /**
   * Sorts merged rows on the coordinator using the extracted sort keys. Uses a Comparator chain
   * that handles null values and ascending/descending direction.
   */
  @SuppressWarnings("unchecked")
  private void sortRows(List<List<Object>> rows, List<SortKey> sortKeys) {
    if (sortKeys.isEmpty() || rows.size() <= 1) {
      return;
    }

    Comparator<List<Object>> comparator =
        (row1, row2) -> {
          for (SortKey key : sortKeys) {
            Object v1 = key.fieldIndex < row1.size() ? row1.get(key.fieldIndex) : null;
            Object v2 = key.fieldIndex < row2.size() ? row2.get(key.fieldIndex) : null;

            // Handle nulls
            if (v1 == null && v2 == null) {
              continue;
            }
            if (v1 == null) {
              return key.nullsLast ? 1 : -1;
            }
            if (v2 == null) {
              return key.nullsLast ? -1 : 1;
            }

            // Compare values
            int cmp;
            if (v1 instanceof Comparable && v2 instanceof Comparable) {
              try {
                cmp = ((Comparable<Object>) v1).compareTo(v2);
              } catch (ClassCastException e) {
                // Different types: compare by string representation
                cmp = v1.toString().compareTo(v2.toString());
              }
            } else {
              cmp = v1.toString().compareTo(v2.toString());
            }

            if (cmp != 0) {
              return key.descending ? -cmp : cmp;
            }
          }
          return 0;
        };

    rows.sort(comparator);
  }

  /** Represents a sort key with field name, position, direction, and null ordering. */
  private record SortKey(String fieldName, int fieldIndex, boolean descending, boolean nullsLast) {}

  // ========== Coordinator-side Calcite Execution ==========

  /**
   * Checks whether the RelNode tree contains complex operations that require coordinator-side
   * Calcite execution. Complex operations include: Aggregate (stats), computed expressions (eval),
   * window functions (dedup, streamstats).
   *
   * <p>Simple operations (scan, filter, sort, limit, rename/project with simple field refs) are
   * handled by the fast operator pipeline path.
   */
  private boolean hasComplexOperations(RelNode node) {
    if (node instanceof Aggregate) {
      return true;
    }
    if (node instanceof Project project) {
      for (RexNode expr : project.getProjects()) {
        if (expr instanceof RexOver || expr instanceof RexCall) {
          return true;
        }
      }
    }
    for (RelNode input : node.getInputs()) {
      if (hasComplexOperations(input)) {
        return true;
      }
    }
    return false;
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
      collectTableScans(relNode, tableScans);

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
                .map(row -> normalizeRowForCalcite(row, scanRowType))
                .collect(Collectors.toList());

        inMemoryTables.put(tableName, new InMemoryScannableTable(scanRowType, rowArrays));

        log.info(
            "[Distributed Engine] Scanned {} rows from {} ({} fields)",
            rows.size(),
            tableName,
            fieldNames.size());
      }

      // Step 3: Replace TableScan nodes with BindableTableScan backed by in-memory data
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
              });

      // Step 4: Optimize and execute via Calcite RelRunner using existing connection
      modifiedPlan = CalciteToolsHelper.optimize(modifiedPlan, context);

      try (Connection connection = context.connection) {
        RelRunner runner = connection.unwrap(RelRunner.class);
        PreparedStatement ps = runner.prepareStatement(modifiedPlan);
        ResultSet rs = ps.executeQuery();

        // Step 5: Build QueryResponse from ResultSet
        QueryResponse response = buildQueryResponseFromResultSet(rs, relNode);

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
   * Collects all TableScan nodes from the RelNode tree, mapping table name → TableScan. Handles
   * both single-table and join queries.
   */
  private void collectTableScans(RelNode node, Map<String, TableScan> scans) {
    if (node instanceof TableScan scan) {
      List<String> qualifiedName = scan.getTable().getQualifiedName();
      String tableName = qualifiedName.get(qualifiedName.size() - 1);
      scans.put(tableName, scan);
    }
    for (RelNode input : node.getInputs()) {
      collectTableScans(input, scans);
    }
  }

  /**
   * Finds work units for a specific table from the distributed plan's scan stages. Handles both
   * single-table queries (unnamed scan stage) and join queries (named left/right stages).
   */
  private List<WorkUnit> findWorkUnitsForTable(DistributedPhysicalPlan plan, String tableName) {
    for (ExecutionStage stage : plan.getExecutionStages()) {
      if (stage.getStageType() == ExecutionStage.StageType.SCAN) {
        // Check tagged join scan stages
        if (stage.getProperties() != null && stage.getProperties().containsKey("tableName")) {
          if (tableName.equals(stage.getProperties().get("tableName"))) {
            return stage.getWorkUnits();
          }
        } else {
          // Single-table scan stage — check if work unit index matches
          if (!stage.getWorkUnits().isEmpty()) {
            String indexName = stage.getWorkUnits().getFirst().getDataPartition().getIndexName();
            if (tableName.equals(indexName)) {
              return stage.getWorkUnits();
            }
          }
        }
      }
    }
    throw new IllegalStateException("No SCAN stage found for table: " + tableName);
  }

  /**
   * Builds a QueryResponse from a JDBC ResultSet. Reads all rows and maps them to ExprValue tuples
   * using the original RelNode's output field names for column naming.
   */
  private QueryResponse buildQueryResponseFromResultSet(ResultSet rs, RelNode originalRelNode)
      throws Exception {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    List<String> outputFieldNames = originalRelNode.getRowType().getFieldNames();

    // Read all rows
    List<ExprValue> values = new ArrayList<>();
    while (rs.next()) {
      Map<String, ExprValue> exprRow = new LinkedHashMap<>();
      for (int i = 0; i < columnCount && i < outputFieldNames.size(); i++) {
        Object val = rs.getObject(i + 1); // JDBC is 1-indexed
        exprRow.put(outputFieldNames.get(i), ExprValueUtils.fromObjectValue(val));
      }
      values.add(ExprTupleValue.fromExprValueMap(exprRow));
    }

    // Build schema from original RelNode row type
    List<Column> columns = new ArrayList<>();
    for (RelDataTypeField field : originalRelNode.getRowType().getFieldList()) {
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
   * Normalizes a row's values to match the declared Calcite row type. OpenSearch data nodes may
   * return Integer for fields declared as BIGINT (Long), or Float for DOUBLE fields. Calcite's
   * execution engine expects exact type matches, so we convert here.
   */
  private Object[] normalizeRowForCalcite(List<Object> row, RelDataType rowType) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    Object[] result = new Object[row.size()];
    for (int i = 0; i < row.size(); i++) {
      Object val = row.get(i);
      if (val != null && i < fields.size()) {
        SqlTypeName sqlType = fields.get(i).getType().getSqlTypeName();
        val = coerceToCalciteType(val, sqlType);
      }
      result[i] = val;
    }
    return result;
  }

  /**
   * Coerces a value to the expected Calcite Java type for the given SQL type. Handles: BIGINT →
   * Long, DOUBLE → Double, INTEGER → Integer, FLOAT → Float, SMALLINT → Short.
   */
  private Object coerceToCalciteType(Object val, SqlTypeName sqlType) {
    if (val == null) {
      return null;
    }
    return switch (sqlType) {
      case BIGINT -> {
        if (val instanceof Number n) {
          yield n.longValue();
        }
        yield val;
      }
      case INTEGER -> {
        if (val instanceof Number n) {
          yield n.intValue();
        }
        yield val;
      }
      case DOUBLE -> {
        if (val instanceof Number n) {
          yield n.doubleValue();
        }
        yield val;
      }
      case FLOAT, REAL -> {
        if (val instanceof Number n) {
          yield n.floatValue();
        }
        yield val;
      }
      case SMALLINT -> {
        if (val instanceof Number n) {
          yield n.shortValue();
        }
        yield val;
      }
      case TINYINT -> {
        if (val instanceof Number n) {
          yield n.byteValue();
        }
        yield val;
      }
      default -> val;
    };
  }

  /**
   * In-memory Calcite ScannableTable that wraps pre-fetched rows from distributed data node scans.
   * Used by coordinator-side Calcite execution to replace OpenSearch-backed TableScan nodes with
   * in-memory data.
   */
  private static class InMemoryScannableTable extends AbstractTable implements ScannableTable {
    private final RelDataType rowType;
    private final List<Object[]> rows;

    InMemoryScannableTable(RelDataType rowType, List<Object[]> rows) {
      this.rowType = rowType;
      this.rows = rows;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return rowType;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
      return Linq4j.asEnumerable(rows);
    }
  }

  // ========== Join Support ==========

  /**
   * Walks the RelNode tree to find the first Join node. Returns null if no join is present. Skips
   * through Sort, Filter, Project, and LogicalSystemLimit nodes.
   */
  Join findJoinNode(RelNode node) {
    if (node instanceof Join join) {
      return join;
    }
    for (RelNode input : node.getInputs()) {
      Join found = findJoinNode(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Holds extracted information about a join: both sides' table names, field names, equi-join key
   * indices, join type, pre-join filters, and field counts.
   */
  record JoinInfo(
      RelNode leftInput,
      RelNode rightInput,
      String leftTableName,
      String rightTableName,
      List<String> leftFieldNames,
      List<String> rightFieldNames,
      List<Integer> leftKeyIndices,
      List<Integer> rightKeyIndices,
      JoinRelType joinType,
      int leftFieldCount,
      int rightFieldCount,
      List<Map<String, Object>> leftFilters,
      List<Map<String, Object>> rightFilters) {}

  /**
   * Extracts join info from a Join node. Parses the join condition to get equi-join key indices,
   * extracts per-side table names, field names, and pre-join filters. Handles field offset: right
   * key index = rawIndex - leftFieldCount.
   */
  JoinInfo extractJoinInfo(Join joinNode) {
    RelNode leftInput = joinNode.getLeft();
    RelNode rightInput = joinNode.getRight();

    int leftFieldCount = leftInput.getRowType().getFieldCount();
    int rightFieldCount = rightInput.getRowType().getFieldCount();

    List<String> leftFieldNames =
        leftInput.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .collect(Collectors.toList());

    List<String> rightFieldNames =
        rightInput.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .collect(Collectors.toList());

    String leftTableName = findTableName(leftInput);
    String rightTableName = findTableName(rightInput);

    // Parse join condition to extract equi-join key indices
    List<Integer> leftKeyIndices = new ArrayList<>();
    List<Integer> rightKeyIndices = new ArrayList<>();
    extractJoinKeys(joinNode.getCondition(), leftFieldCount, leftKeyIndices, rightKeyIndices);

    // Extract pre-join filters from each side
    List<Map<String, Object>> leftFilters = extractFilters(leftInput);
    List<Map<String, Object>> rightFilters = extractFilters(rightInput);

    return new JoinInfo(
        leftInput,
        rightInput,
        leftTableName,
        rightTableName,
        leftFieldNames,
        rightFieldNames,
        leftKeyIndices,
        rightKeyIndices,
        joinNode.getJoinType(),
        leftFieldCount,
        rightFieldCount,
        leftFilters,
        rightFilters);
  }

  /**
   * Finds the table name by walking down the RelNode tree to the TableScan. Traverses through
   * Filter, Project, Sort, and LogicalSystemLimit nodes.
   */
  private String findTableName(RelNode node) {
    if (node instanceof TableScan tableScan) {
      List<String> qualifiedName = tableScan.getTable().getQualifiedName();
      return qualifiedName.get(qualifiedName.size() - 1);
    }
    for (RelNode input : node.getInputs()) {
      String name = findTableName(input);
      if (name != null) {
        return name;
      }
    }
    return null;
  }

  /**
   * Extracts equi-join key indices from a RexNode join condition. For condition like {@code =($0,
   * $11)} where left has 11 fields: left key index = 0, right key index = 11 - 11 = 0
   * (offset-adjusted). Handles AND conditions by recursing into operands.
   */
  private void extractJoinKeys(
      RexNode condition, int leftFieldCount, List<Integer> leftKeys, List<Integer> rightKeys) {
    if (!(condition instanceof RexCall call)) {
      return;
    }

    switch (call.getKind()) {
      case AND -> {
        for (RexNode operand : call.getOperands()) {
          extractJoinKeys(operand, leftFieldCount, leftKeys, rightKeys);
        }
      }
      case EQUALS -> {
        if (call.getOperands().size() == 2) {
          RexNode left = call.getOperands().get(0);
          RexNode right = call.getOperands().get(1);
          if (left instanceof RexInputRef leftRef && right instanceof RexInputRef rightRef) {
            int leftIdx = leftRef.getIndex();
            int rightIdx = rightRef.getIndex();
            // Determine which is left side and which is right side
            if (leftIdx < leftFieldCount && rightIdx >= leftFieldCount) {
              leftKeys.add(leftIdx);
              rightKeys.add(rightIdx - leftFieldCount);
            } else if (rightIdx < leftFieldCount && leftIdx >= leftFieldCount) {
              leftKeys.add(rightIdx);
              rightKeys.add(leftIdx - leftFieldCount);
            }
          }
        }
      }
      default -> log.debug("[Distributed Engine] Non-equi join condition: {}", call.getKind());
    }
  }

  /**
   * Performs a hash join between left and right row sets. Builds a hash table on the right side
   * (build side) and probes with the left side (probe side).
   *
   * <p>Supports: INNER, LEFT, RIGHT, SEMI, ANTI join types. NULL keys never match (SQL semantics).
   *
   * @param leftRows Rows from the left (probe) side
   * @param rightRows Rows from the right (build) side
   * @param leftKeyIndices Column indices in left rows for join keys
   * @param rightKeyIndices Column indices in right rows for join keys
   * @param joinType The type of join to perform
   * @param leftFieldCount Number of fields in the left row type
   * @param rightFieldCount Number of fields in the right row type
   * @return Joined rows
   */
  List<List<Object>> performHashJoin(
      List<List<Object>> leftRows,
      List<List<Object>> rightRows,
      List<Integer> leftKeyIndices,
      List<Integer> rightKeyIndices,
      JoinRelType joinType,
      int leftFieldCount,
      int rightFieldCount) {

    // Build hash table from right side (build side)
    Map<Object, List<List<Object>>> hashTable = buildHashTable(rightRows, rightKeyIndices);

    List<List<Object>> result = new ArrayList<>();
    // Track which right rows were matched (for RIGHT and FULL joins)
    Set<Integer> matchedRightIndices = new HashSet<>();

    // Probe with left side
    for (List<Object> leftRow : leftRows) {
      Object leftKey = extractJoinKey(leftRow, leftKeyIndices);

      // NULL keys never match in SQL semantics
      if (leftKey == null) {
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
          result.add(combineRowsWithNullRight(leftRow, rightFieldCount));
        } else if (joinType == JoinRelType.ANTI) {
          result.add(new ArrayList<>(leftRow));
        }
        continue;
      }

      List<List<Object>> matchingRightRows = hashTable.get(leftKey);
      boolean hasMatch = matchingRightRows != null && !matchingRightRows.isEmpty();

      switch (joinType) {
        case INNER -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
              trackMatchedRightRows(rightRows, rightRow, matchedRightIndices);
            }
          }
        }
        case LEFT -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
            }
          } else {
            result.add(combineRowsWithNullRight(leftRow, rightFieldCount));
          }
        }
        case RIGHT -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
              trackMatchedRightRows(rightRows, rightRow, matchedRightIndices);
            }
          }
        }
        case FULL -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
              trackMatchedRightRows(rightRows, rightRow, matchedRightIndices);
            }
          } else {
            result.add(combineRowsWithNullRight(leftRow, rightFieldCount));
          }
        }
        case SEMI -> {
          if (hasMatch) {
            result.add(new ArrayList<>(leftRow));
          }
        }
        case ANTI -> {
          if (!hasMatch) {
            result.add(new ArrayList<>(leftRow));
          }
        }
        default -> throw new UnsupportedOperationException("Unsupported join type: " + joinType);
      }
    }

    // For RIGHT and FULL joins: emit unmatched right rows
    if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
      for (int i = 0; i < rightRows.size(); i++) {
        if (!matchedRightIndices.contains(i)) {
          result.add(combineRowsWithNullLeft(leftFieldCount, rightRows.get(i)));
        }
      }
    }

    return result;
  }

  /**
   * Builds a hash table from the given rows using the specified key indices. Rows with null keys
   * are stored under a special sentinel but will never be matched during probe.
   */
  private Map<Object, List<List<Object>>> buildHashTable(
      List<List<Object>> rows, List<Integer> keyIndices) {
    Map<Object, List<List<Object>>> hashTable = new HashMap<>();
    for (List<Object> row : rows) {
      Object key = extractJoinKey(row, keyIndices);
      if (key != null) {
        hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }
    }
    return hashTable;
  }

  /**
   * Extracts the join key from a row. For single-column keys, returns the normalized value. For
   * composite keys (multiple columns), returns a List of normalized values. Returns null if any key
   * column is null.
   */
  Object extractJoinKey(List<Object> row, List<Integer> keyIndices) {
    if (keyIndices.size() == 1) {
      int idx = keyIndices.get(0);
      Object val = idx < row.size() ? row.get(idx) : null;
      return normalizeJoinKeyValue(val);
    }

    // Composite key: all parts must be non-null
    List<Object> compositeKey = new ArrayList<>(keyIndices.size());
    for (int idx : keyIndices) {
      Object val = idx < row.size() ? row.get(idx) : null;
      if (val == null) {
        return null;
      }
      compositeKey.add(normalizeJoinKeyValue(val));
    }
    return compositeKey;
  }

  /**
   * Normalizes a join key value for consistent hash/equals behavior. Converts all integer numeric
   * types (Integer, Long, Short, Byte) to Long for consistent comparison, since OpenSearch may
   * return the same field as different numeric types.
   */
  private Object normalizeJoinKeyValue(Object val) {
    if (val == null) {
      return null;
    }
    // Normalize integer numeric types to Long for consistent equals/hashCode
    if (val instanceof Integer || val instanceof Short || val instanceof Byte) {
      return ((Number) val).longValue();
    }
    if (val instanceof Float) {
      return ((Float) val).doubleValue();
    }
    return val;
  }

  /** Combines a left row and right row into a single joined row (left + right). */
  private List<Object> combineRows(List<Object> leftRow, List<Object> rightRow) {
    List<Object> combined = new ArrayList<>(leftRow.size() + rightRow.size());
    combined.addAll(leftRow);
    combined.addAll(rightRow);
    return combined;
  }

  /** Creates a joined row with left data and nulls for right side (used in LEFT/FULL joins). */
  private List<Object> combineRowsWithNullRight(List<Object> leftRow, int rightFieldCount) {
    List<Object> combined = new ArrayList<>(leftRow.size() + rightFieldCount);
    combined.addAll(leftRow);
    combined.addAll(Collections.nCopies(rightFieldCount, null));
    return combined;
  }

  /** Creates a joined row with nulls for left side and right data (used in RIGHT/FULL joins). */
  private List<Object> combineRowsWithNullLeft(int leftFieldCount, List<Object> rightRow) {
    List<Object> combined = new ArrayList<>(leftFieldCount + rightRow.size());
    combined.addAll(Collections.nCopies(leftFieldCount, null));
    combined.addAll(rightRow);
    return combined;
  }

  /**
   * Tracks the index of a matched right row for RIGHT/FULL join. Finds the row by reference in the
   * original list.
   */
  private void trackMatchedRightRows(
      List<List<Object>> rightRows, List<Object> matchedRow, Set<Integer> matchedIndices) {
    for (int i = 0; i < rightRows.size(); i++) {
      if (rightRows.get(i) == matchedRow) {
        matchedIndices.add(i);
      }
    }
  }

  /** Shuts down the scheduler and releases resources. */
  public void shutdown() {
    log.info("Shutting down DistributedTaskScheduler");
  }
}
