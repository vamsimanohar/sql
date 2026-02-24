/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
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
        request.setFieldNames(requestFieldNames);
        request.setQueryLimit(queryLimit);
        request.setStageId("operator-pipeline");
        request.setFilterConditions(filterConditions);

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

      // Apply coordinator-side sort if sort keys present
      if (!sortKeys.isEmpty()) {
        sortRows(allRows, sortKeys);
        log.info("[Distributed Engine] Sorted {} rows by {}", allRows.size(), sortKeys);
      }

      // Apply final limit
      if (allRows.size() > queryLimit) {
        allRows = allRows.subList(0, queryLimit);
      }

      log.info(
          "[Distributed Engine] Merged {} rows from {} nodes", allRows.size(), workByNode.size());

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

  /** Shuts down the scheduler and releases resources. */
  public void shutdown() {
    log.info("Shutting down DistributedTaskScheduler");
  }
}
