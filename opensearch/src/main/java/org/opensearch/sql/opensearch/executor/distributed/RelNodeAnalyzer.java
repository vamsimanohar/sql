/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;

/**
 * Walks RelNode trees to extract metadata: filters, sort keys, limits, table scans, projections,
 * query_string conditions, join nodes. All methods are pure tree-walking functions — static with no
 * state.
 */
@Log4j2
public final class RelNodeAnalyzer {

  private RelNodeAnalyzer() {}

  // ========== Filter extraction ==========

  /**
   * Extracts filter conditions from the RelNode tree. Walks the tree to find Filter nodes and
   * converts their RexNode conditions to serializable filter condition maps.
   */
  public static List<Map<String, Object>> extractFilters(RelNode node) {
    List<Map<String, Object>> conditions = new ArrayList<>();
    collectFilters(node, conditions);
    return conditions.isEmpty() ? null : conditions;
  }

  private static void collectFilters(RelNode node, List<Map<String, Object>> conditions) {
    if (node instanceof Filter filter) {
      RexNode condition = filter.getCondition();
      RelDataType inputRowType = filter.getInput().getRowType();
      convertRexToConditions(condition, inputRowType, conditions);
    }
    for (RelNode input : node.getInputs()) {
      collectFilters(input, conditions);
    }
  }

  /**
   * Converts a Calcite RexNode expression to filter condition maps. Handles comparison operators
   * (=, !=, >, >=, <, <=) and boolean AND/OR.
   */
  static void convertRexToConditions(
      RexNode rexNode, RelDataType rowType, List<Map<String, Object>> conditions) {
    if (!(rexNode instanceof RexCall call)) {
      return;
    }

    switch (call.getKind()) {
      case AND -> {
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

  private static void addComparisonCondition(
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

  static String resolveFieldName(RexNode node, RelDataType rowType) {
    if (node instanceof RexInputRef ref) {
      List<String> fieldNames = rowType.getFieldNames();
      if (ref.getIndex() < fieldNames.size()) {
        return fieldNames.get(ref.getIndex());
      }
    }
    if (node instanceof RexCall cast && cast.getKind() == SqlKind.CAST) {
      return resolveFieldName(cast.getOperands().get(0), rowType);
    }
    return null;
  }

  static Object resolveLiteralValue(RexNode node) {
    if (node instanceof RexLiteral literal) {
      return literal.getValue2();
    }
    if (node instanceof RexCall cast && cast.getKind() == SqlKind.CAST) {
      return resolveLiteralValue(cast.getOperands().get(0));
    }
    return null;
  }

  static String reverseOp(String op) {
    return switch (op) {
      case "GT" -> "LT";
      case "GTE" -> "LTE";
      case "LT" -> "GT";
      case "LTE" -> "GTE";
      default -> op;
    };
  }

  // ========== Sort key extraction ==========

  /**
   * Extracts sort keys from the RelNode tree. Walks the tree to find Sort nodes (excluding
   * LogicalSystemLimit) and extracts field index + direction for each sort key.
   */
  public static List<SortKey> extractSortKeys(RelNode node, List<String> fieldNames) {
    List<SortKey> keys = new ArrayList<>();
    collectSortKeys(node, fieldNames, keys);
    return keys;
  }

  private static void collectSortKeys(RelNode node, List<String> fieldNames, List<SortKey> keys) {
    if (node instanceof Sort sort && !(node instanceof LogicalSystemLimit)) {
      RelCollation collation = sort.getCollation();
      if (collation != null && !collation.getFieldCollations().isEmpty()) {
        List<String> sortFieldNames = sort.getInput().getRowType().getFieldNames();
        for (RelFieldCollation fc : collation.getFieldCollations()) {
          int fieldIndex = fc.getFieldIndex();
          String fieldName =
              fieldIndex < sortFieldNames.size() ? sortFieldNames.get(fieldIndex) : null;
          if (fieldName != null) {
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

  // ========== Limit extraction ==========

  /**
   * Extracts the query limit from the RelNode tree. Looks for Sort with fetch (head N) or
   * LogicalSystemLimit, returning whichever is smaller.
   */
  public static int extractLimit(RelNode node) {
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
    } else if (node instanceof Sort sort) {
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

  // ========== Field mapping resolution ==========

  /**
   * Resolves output field names to physical (scan-level) field names by walking through Project
   * nodes. Returns a list of FieldMapping(outputName, physicalName) for each output column.
   */
  public static List<FieldMapping> resolveFieldMappings(RelNode node) {
    List<String> outputNames = node.getRowType().getFieldNames();
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
   * output column index to physical field name.
   */
  static Map<Integer, String> resolveToScanFields(RelNode node) {
    if (node instanceof TableScan) {
      List<String> scanFields = node.getRowType().getFieldNames();
      Map<Integer, String> result = new HashMap<>();
      for (int i = 0; i < scanFields.size(); i++) {
        result.put(i, scanFields.get(i));
      }
      return result;
    }

    if (node instanceof Project project) {
      Map<Integer, String> inputPhysical = resolveToScanFields(project.getInput());
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
      }
      return result;
    }

    if (!node.getInputs().isEmpty()) {
      return resolveToScanFields(node.getInputs().getFirst());
    }

    return new HashMap<>();
  }

  // ========== Complex operations check ==========

  /**
   * Checks whether the RelNode tree contains complex operations that require coordinator-side
   * Calcite execution (Aggregate, computed expressions, window functions).
   */
  public static boolean hasComplexOperations(RelNode node) {
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

  // ========== Table scan collection ==========

  /**
   * Collects all TableScan nodes from the RelNode tree, mapping table name to TableScan. Handles
   * both single-table and join queries.
   */
  public static void collectTableScans(RelNode node, Map<String, TableScan> scans) {
    if (node instanceof TableScan scan) {
      List<String> qualifiedName = scan.getTable().getQualifiedName();
      String tableName = qualifiedName.get(qualifiedName.size() - 1);
      scans.put(tableName, scan);
    }
    for (RelNode input : node.getInputs()) {
      collectTableScans(input, scans);
    }
  }

  // ========== Query string extraction ==========

  /**
   * Walks the RelNode tree to find query_string conditions in Filter nodes. Extracts the query text
   * for pushdown to data nodes.
   */
  public static void collectQueryStringConditions(RelNode node, List<String> queryStrings) {
    if (node instanceof Filter filter) {
      extractQueryStringFromRex(filter.getCondition(), queryStrings);
    }
    for (RelNode input : node.getInputs()) {
      collectQueryStringConditions(input, queryStrings);
    }
  }

  /** Extracts query_string text from a RexNode condition. */
  static void extractQueryStringFromRex(RexNode rex, List<String> queryStrings) {
    if (rex instanceof RexCall call && call.getOperator().getName().equals("query_string")) {
      if (!call.getOperands().isEmpty() && call.getOperands().get(0) instanceof RexCall mapCall) {
        if (mapCall.getOperands().size() >= 2
            && mapCall.getOperands().get(1) instanceof RexLiteral lit) {
          String queryText = lit.getValueAs(String.class);
          if (queryText != null) {
            queryStrings.add(queryText);
          }
        }
      }
    }
  }

  /** Checks if a RexNode contains a query_string function call. */
  public static boolean containsQueryString(RexNode rex) {
    if (rex instanceof RexCall call) {
      if (call.getOperator().getName().equals("query_string")) {
        return true;
      }
      for (RexNode operand : call.getOperands()) {
        if (containsQueryString(operand)) {
          return true;
        }
      }
    }
    return false;
  }

  // ========== Join-related analysis ==========

  /**
   * Walks the RelNode tree to find the first Join node. Returns null if no join is present. Skips
   * through Sort, Filter, Project, and LogicalSystemLimit nodes.
   */
  public static Join findJoinNode(RelNode node) {
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
   * Finds the table name by walking down the RelNode tree to the TableScan. Traverses through
   * Filter, Project, Sort, and LogicalSystemLimit nodes.
   */
  public static String findTableName(RelNode node) {
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
   * Extracts column index mappings from Project nodes above the Join. Returns a list of source
   * column indices that the Project selects from the joined row, or null if no Project is found
   * above the join.
   */
  public static List<Integer> extractPostJoinProjection(RelNode node, Join joinNode) {
    if (node == joinNode) {
      return null;
    }
    if (node instanceof Project project) {
      List<Integer> indices = new ArrayList<>();
      for (RexNode expr : project.getProjects()) {
        if (expr instanceof RexInputRef ref) {
          indices.add(ref.getIndex());
        } else {
          return null;
        }
      }
      return indices;
    }
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
  public static List<Map<String, Object>> extractPostJoinFilters(RelNode root, Join joinNode) {
    List<Map<String, Object>> conditions = new ArrayList<>();
    collectPostJoinFilters(root, joinNode, conditions);
    return conditions.isEmpty() ? null : conditions;
  }

  private static void collectPostJoinFilters(
      RelNode node, Join joinNode, List<Map<String, Object>> conditions) {
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

  // ========== Join info extraction ==========

  /**
   * Extracts join info from a Join node. Parses the join condition to get equi-join key indices,
   * extracts per-side table names, field names, and pre-join filters.
   */
  public static JoinInfo extractJoinInfo(Join joinNode) {
    RelNode leftInput = joinNode.getLeft();
    RelNode rightInput = joinNode.getRight();

    int leftFieldCount = leftInput.getRowType().getFieldCount();
    int rightFieldCount = rightInput.getRowType().getFieldCount();

    List<String> leftFieldNames =
        leftInput.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .collect(java.util.stream.Collectors.toList());

    List<String> rightFieldNames =
        rightInput.getRowType().getFieldList().stream()
            .map(RelDataTypeField::getName)
            .collect(java.util.stream.Collectors.toList());

    String leftTableName = findTableName(leftInput);
    String rightTableName = findTableName(rightInput);

    List<Integer> leftKeyIndices = new ArrayList<>();
    List<Integer> rightKeyIndices = new ArrayList<>();
    extractJoinKeys(joinNode.getCondition(), leftFieldCount, leftKeyIndices, rightKeyIndices);

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
   * Extracts equi-join key indices from a RexNode join condition. Handles AND conditions by
   * recursing into operands.
   */
  static void extractJoinKeys(
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
}
