/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/**
 * Analyzes a Calcite RelNode tree and produces a {@link RelNodeAnalysis} for distributed planning.
 *
 * <p>Walks the RelNode tree to extract table name, filter conditions, projections, aggregation
 * info, sort/limit info, and join metadata.
 */
@Log4j2
public class DistributedPlanAnalyzer {

  /** Analyzes a RelNode tree and returns the analysis result. */
  public RelNodeAnalysis analyze(RelNode relNode, CalcitePlanContext context) {
    RelNodeAnalysis analysis = new RelNodeAnalysis();

    // Walk the RelNode tree and extract information
    analyzeNode(relNode, analysis, context);

    // Determine if the plan is distributable
    boolean distributable = analysis.getTableName() != null;
    String reason = distributable ? null : "No table found in RelNode tree";

    analysis.setDistributable(distributable);
    analysis.setReason(reason);

    // Create output schema
    Schema outputSchema = createOutputSchema(analysis);
    analysis.setOutputSchema(outputSchema);

    return analysis;
  }

  private void analyzeNode(RelNode node, RelNodeAnalysis analysis, CalcitePlanContext context) {
    if (node instanceof Join join) {
      analyzeJoin(join, analysis);
    } else if (node instanceof TableScan) {
      analyzeTableScan((TableScan) node, analysis);
    } else if (node instanceof Filter) {
      analyzeFilter((Filter) node, analysis);
    } else if (node instanceof Project) {
      analyzeProject((Project) node, analysis);
    } else if (node instanceof Aggregate) {
      analyzeAggregate((Aggregate) node, analysis);
    } else if (node instanceof Sort) {
      analyzeSort((Sort) node, analysis);
    }

    // Store RelNode information for later use
    analysis.getRelNodeInfo().put(node.getClass().getSimpleName(), node.getDigest());

    // Recursively analyze inputs
    for (RelNode input : node.getInputs()) {
      analyzeNode(input, analysis, context);
    }
  }

  private void analyzeJoin(Join join, RelNodeAnalysis analysis) {
    analysis.setHasJoin(true);

    String leftTable = findTableName(join.getLeft());
    if (leftTable != null) {
      analysis.setLeftTableName(leftTable);
      if (analysis.getTableName() == null) {
        analysis.setTableName(leftTable);
      }
    }

    String rightTable = findTableName(join.getRight());
    if (rightTable != null) {
      analysis.setRightTableName(rightTable);
    }

    log.debug("Found join: type={}, left={}, right={}", join.getJoinType(), leftTable, rightTable);
  }

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

  private void analyzeTableScan(TableScan tableScan, RelNodeAnalysis analysis) {
    List<String> qualifiedName = tableScan.getTable().getQualifiedName();
    String tableName = qualifiedName.get(qualifiedName.size() - 1);
    analysis.setTableName(tableName);
    log.debug("Found table scan: {}", tableName);
  }

  private void analyzeFilter(Filter filter, RelNodeAnalysis analysis) {
    String condition = filter.getCondition().toString();
    analysis.addFilterCondition(condition);
    log.debug("Found filter: {}", condition);
  }

  private void analyzeProject(Project project, RelNodeAnalysis analysis) {
    project
        .getProjects()
        .forEach(
            expr -> {
              String exprStr = expr.toString();
              analysis.addProjection(exprStr, exprStr);
            });
    log.debug("Found projection with {} expressions", project.getProjects().size());
  }

  private void analyzeAggregate(Aggregate aggregate, RelNodeAnalysis analysis) {
    analysis.setHasAggregation(true);

    aggregate
        .getGroupSet()
        .forEach(
            groupIndex -> {
              String fieldName = "field_" + groupIndex;
              analysis.addGroupByField(fieldName);
            });

    aggregate
        .getAggCallList()
        .forEach(
            aggCall -> {
              String aggName = aggCall.getAggregation().getName();
              String aggExpr = aggCall.toString();
              analysis.addAggregation(aggName, aggExpr);
            });

    log.debug(
        "Found aggregation with {} groups and {} agg calls",
        aggregate.getGroupCount(),
        aggregate.getAggCallList().size());
  }

  private void analyzeSort(Sort sort, RelNodeAnalysis analysis) {
    if (sort.getCollation() != null) {
      sort.getCollation()
          .getFieldCollations()
          .forEach(
              field -> {
                String fieldName = "field_" + field.getFieldIndex();
                analysis.addSortField(fieldName);
              });
    }

    if (sort.fetch != null) {
      analysis.setLimit(100); // Simplified for Phase 2
    }

    log.debug("Found sort with collation: {}", sort.getCollation());
  }

  private Schema createOutputSchema(RelNodeAnalysis analysis) {
    List<Column> columns = new ArrayList<>();

    if (analysis.hasAggregation()) {
      analysis.getGroupByFields().forEach(field -> columns.add(new Column(field, null, null)));
      analysis.getAggregations().forEach((name, func) -> columns.add(new Column(name, null, null)));
    } else {
      if (analysis.getProjections().isEmpty()) {
        columns.add(new Column("*", null, null));
      } else {
        analysis
            .getProjections()
            .forEach((alias, expr) -> columns.add(new Column(alias, null, null)));
      }
    }

    return new Schema(columns);
  }
}
