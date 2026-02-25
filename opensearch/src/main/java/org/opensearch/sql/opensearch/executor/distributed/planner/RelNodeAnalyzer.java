/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Extracts query metadata from a Calcite RelNode tree. Walks the tree to find the index name, field
 * names, query limit, and filter conditions.
 *
 * <p>Supported RelNode patterns:
 *
 * <ul>
 *   <li>{@link AbstractCalciteIndexScan} - index name and field names
 *   <li>{@link LogicalSort} with fetch - query limit
 *   <li>{@link LogicalFilter} - filter conditions (simple comparisons and AND)
 *   <li>{@link LogicalProject} - projected field names
 * </ul>
 */
public class RelNodeAnalyzer {

  /** Result of analyzing a RelNode tree. */
  public static class AnalysisResult {
    private final String indexName;
    private final List<String> fieldNames;
    private final int queryLimit;
    private final List<Map<String, Object>> filterConditions;

    public AnalysisResult(
        String indexName,
        List<String> fieldNames,
        int queryLimit,
        List<Map<String, Object>> filterConditions) {
      this.indexName = indexName;
      this.fieldNames = fieldNames;
      this.queryLimit = queryLimit;
      this.filterConditions = filterConditions;
    }

    public String getIndexName() {
      return indexName;
    }

    public List<String> getFieldNames() {
      return fieldNames;
    }

    /** Returns the query limit, or -1 if no limit was specified. */
    public int getQueryLimit() {
      return queryLimit;
    }

    /** Returns filter conditions, or null if no filters. */
    public List<Map<String, Object>> getFilterConditions() {
      return filterConditions;
    }
  }

  /**
   * Analyzes a RelNode tree and extracts query metadata.
   *
   * @param relNode the root of the RelNode tree
   * @return the analysis result
   * @throws UnsupportedOperationException if the tree contains unsupported operations
   */
  public static AnalysisResult analyze(RelNode relNode) {
    String indexName = null;
    List<String> fieldNames = null;
    int queryLimit = -1;
    List<Map<String, Object>> filterConditions = null;

    // Walk tree from root to leaf
    RelNode current = relNode;
    List<String> projectedFields = null;

    while (current != null) {
      if (current instanceof LogicalSort) {
        LogicalSort sort = (LogicalSort) current;
        if (sort.fetch != null) {
          queryLimit = extractLimit(sort.fetch);
        }
        current = sort.getInput();
      } else if (current instanceof LogicalProject) {
        LogicalProject project = (LogicalProject) current;
        projectedFields = extractProjectedFields(project);
        current = project.getInput();
      } else if (current instanceof LogicalFilter) {
        LogicalFilter filter = (LogicalFilter) current;
        filterConditions = extractFilterConditions(filter.getCondition(), filter.getInput());
        current = filter.getInput();
      } else if (current instanceof AbstractCalciteIndexScan) {
        AbstractCalciteIndexScan scan = (AbstractCalciteIndexScan) current;
        indexName = extractIndexName(scan);
        fieldNames = extractFieldNames(scan);
        current = null;
      } else if (current instanceof TableScan) {
        // Generic table scan — extract from table qualified name
        TableScan scan = (TableScan) current;
        List<String> qualifiedName = scan.getTable().getQualifiedName();
        indexName = qualifiedName.get(qualifiedName.size() - 1);
        fieldNames = new ArrayList<>();
        for (RelDataTypeField field : scan.getRowType().getFieldList()) {
          fieldNames.add(field.getName());
        }
        current = null;
      } else if (current.getInputs().size() == 1) {
        // Single-input node we don't recognize — skip through
        current = current.getInput(0);
      } else if (current.getInputs().isEmpty()) {
        // Leaf node we don't recognize
        throw new UnsupportedOperationException(
            "Unsupported leaf node type: " + current.getClass().getSimpleName());
      } else {
        throw new UnsupportedOperationException(
            "Multi-input nodes (joins) not supported: " + current.getClass().getSimpleName());
      }
    }

    if (indexName == null) {
      throw new IllegalStateException("Could not extract index name from RelNode tree");
    }

    // Use projected fields if available, otherwise use scan fields
    if (projectedFields != null) {
      fieldNames = projectedFields;
    }

    return new AnalysisResult(indexName, fieldNames, queryLimit, filterConditions);
  }

  private static String extractIndexName(AbstractCalciteIndexScan scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    return qualifiedName.get(qualifiedName.size() - 1);
  }

  private static List<String> extractFieldNames(AbstractCalciteIndexScan scan) {
    List<String> names = new ArrayList<>();
    for (RelDataTypeField field : scan.getRowType().getFieldList()) {
      names.add(field.getName());
    }
    return names;
  }

  private static int extractLimit(RexNode fetch) {
    if (fetch instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) fetch;
      return ((Number) literal.getValue()).intValue();
    }
    throw new UnsupportedOperationException("Non-literal LIMIT not supported: " + fetch);
  }

  private static List<String> extractProjectedFields(LogicalProject project) {
    List<String> names = new ArrayList<>();
    List<RelDataTypeField> inputFields = project.getInput().getRowType().getFieldList();

    for (int i = 0; i < project.getProjects().size(); i++) {
      RexNode expr = project.getProjects().get(i);
      if (expr instanceof RexInputRef) {
        RexInputRef ref = (RexInputRef) expr;
        names.add(inputFields.get(ref.getIndex()).getName());
      } else {
        // Use the output field name for non-simple projections
        names.add(project.getRowType().getFieldList().get(i).getName());
      }
    }
    return names;
  }

  /**
   * Extracts filter conditions from a RexNode condition expression. Produces a list of condition
   * maps compatible with {@link
   * org.opensearch.sql.opensearch.executor.distributed.ExecuteDistributedTaskRequest}.
   */
  private static List<Map<String, Object>> extractFilterConditions(
      RexNode condition, RelNode input) {
    List<Map<String, Object>> conditions = new ArrayList<>();
    extractConditionsRecursive(condition, input, conditions);
    return conditions;
  }

  private static void extractConditionsRecursive(
      RexNode node, RelNode input, List<Map<String, Object>> conditions) {
    if (node instanceof RexCall) {
      RexCall call = (RexCall) node;
      SqlKind kind = call.getKind();

      if (kind == SqlKind.AND) {
        // Flatten AND — recurse into each operand
        for (RexNode operand : call.getOperands()) {
          extractConditionsRecursive(operand, input, conditions);
        }
      } else if (isComparisonOp(kind)) {
        Map<String, Object> condition = extractComparison(call, input);
        if (condition != null) {
          conditions.add(condition);
        }
      } else if (kind == SqlKind.OR || kind == SqlKind.NOT) {
        throw new UnsupportedOperationException("OR/NOT filters not yet supported");
      }
    }
  }

  private static boolean isComparisonOp(SqlKind kind) {
    return kind == SqlKind.EQUALS
        || kind == SqlKind.NOT_EQUALS
        || kind == SqlKind.GREATER_THAN
        || kind == SqlKind.GREATER_THAN_OR_EQUAL
        || kind == SqlKind.LESS_THAN
        || kind == SqlKind.LESS_THAN_OR_EQUAL;
  }

  private static Map<String, Object> extractComparison(RexCall call, RelNode input) {
    List<RexNode> operands = call.getOperands();
    if (operands.size() != 2) {
      return null;
    }

    RexNode left = operands.get(0);
    RexNode right = operands.get(1);

    // Normalize: field ref on left, literal on right
    String fieldName;
    Object value;
    SqlKind op = call.getKind();

    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      fieldName = resolveFieldName((RexInputRef) left, input);
      value = extractLiteralValue((RexLiteral) right);
    } else if (right instanceof RexInputRef && left instanceof RexLiteral) {
      // Swap: literal op field → field reverseOp literal
      fieldName = resolveFieldName((RexInputRef) right, input);
      value = extractLiteralValue((RexLiteral) left);
      op = reverseComparison(op);
    } else {
      return null;
    }

    Map<String, Object> condition = new HashMap<>();
    condition.put("field", fieldName);
    condition.put("op", sqlKindToOpString(op));
    condition.put("value", value);
    return condition;
  }

  private static String resolveFieldName(RexInputRef ref, RelNode input) {
    return input.getRowType().getFieldList().get(ref.getIndex()).getName();
  }

  private static Object extractLiteralValue(RexLiteral literal) {
    Comparable<?> value = literal.getValue();
    if (value instanceof org.apache.calcite.util.NlsString) {
      return ((org.apache.calcite.util.NlsString) value).getValue();
    }
    if (value instanceof java.math.BigDecimal) {
      java.math.BigDecimal bd = (java.math.BigDecimal) value;
      // Return integer if it has no fractional part
      if (bd.scale() <= 0 || bd.stripTrailingZeros().scale() <= 0) {
        try {
          return bd.intValueExact();
        } catch (ArithmeticException e) {
          try {
            return bd.longValueExact();
          } catch (ArithmeticException e2) {
            return bd.doubleValue();
          }
        }
      }
      return bd.doubleValue();
    }
    return value;
  }

  private static String sqlKindToOpString(SqlKind kind) {
    switch (kind) {
      case EQUALS:
        return "EQ";
      case NOT_EQUALS:
        return "NEQ";
      case GREATER_THAN:
        return "GT";
      case GREATER_THAN_OR_EQUAL:
        return "GTE";
      case LESS_THAN:
        return "LT";
      case LESS_THAN_OR_EQUAL:
        return "LTE";
      default:
        throw new UnsupportedOperationException("Unsupported comparison: " + kind);
    }
  }

  private static SqlKind reverseComparison(SqlKind kind) {
    switch (kind) {
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      default:
        return kind;
    }
  }
}
