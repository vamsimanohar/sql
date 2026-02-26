/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.FilterPhysicalOperator;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.LimitPhysicalOperator;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.PhysicalOperatorNode;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.PhysicalOperatorTree;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.ProjectionPhysicalOperator;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.ScanPhysicalOperator;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;
import org.opensearch.sql.planner.distributed.planner.PhysicalPlanner;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

/**
 * Physical planner that converts Calcite RelNode trees to distributed execution plans using proper
 * visitor pattern traversal instead of ad-hoc pattern matching.
 *
 * <p>Replaces the ad-hoc RelNodeAnalyzer + SimplePlanFragmenter approach with intelligent
 * multi-stage planning that can handle complex query shapes.
 *
 * <p>Phase 1B Support:
 *
 * <ul>
 *   <li>Table scans with pushed-down filters
 *   <li>Field projection
 *   <li>Limit operations
 *   <li>Single-table queries only
 * </ul>
 */
@Log4j2
@RequiredArgsConstructor
public class CalciteDistributedPhysicalPlanner implements PhysicalPlanner {

  private final FragmentationContext fragmentationContext;

  @Override
  public StagedPlan plan(RelNode relNode) {
    log.debug("Planning RelNode tree: {}", relNode.explain());

    // Step 1: Convert RelNode tree to physical operator tree using visitor pattern
    PlanningVisitor visitor = new PlanningVisitor();
    visitor.go(relNode);
    PhysicalOperatorTree operatorTree = visitor.buildOperatorTree();

    log.debug("Built physical operator tree: {}", operatorTree);

    // Step 2: Fragment operator tree into distributed stages
    IntelligentPlanFragmenter fragmenter =
        new IntelligentPlanFragmenter(fragmentationContext.getCostEstimator());
    StagedPlan stagedPlan = fragmenter.fragment(operatorTree, fragmentationContext);

    log.info("Generated staged plan with {} stages", stagedPlan.getStageCount());
    return stagedPlan;
  }

  /**
   * Visitor that traverses RelNode tree and builds corresponding physical operators. Uses proper
   * Calcite visitor pattern instead of ad-hoc pattern matching.
   */
  private static class PlanningVisitor extends RelVisitor {

    private final List<PhysicalOperatorNode> operators = new ArrayList<>();
    private String indexName;

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
      if (node instanceof TableScan) {
        visitTableScan((TableScan) node);
      } else if (node instanceof LogicalFilter) {
        visitLogicalFilter((LogicalFilter) node);
      } else if (node instanceof LogicalProject) {
        visitLogicalProject((LogicalProject) node);
      } else if (node instanceof LogicalSort) {
        visitLogicalSort((LogicalSort) node);
      } else {
        throw new UnsupportedOperationException(
            "Unsupported RelNode type for Phase 1B: "
                + node.getClass().getSimpleName()
                + ". Supported: TableScan, LogicalFilter, LogicalProject, LogicalSort (fetch"
                + " only).");
      }

      super.visit(node, ordinal, parent);
    }

    private void visitTableScan(TableScan tableScan) {
      // Extract index name from table
      RelOptTable table = tableScan.getTable();
      this.indexName = extractIndexName(table);

      // Extract field names from row type
      List<String> fieldNames = tableScan.getRowType().getFieldNames();

      log.debug("Found table scan: index={}, fields={}", indexName, fieldNames);

      // Create scan physical operator
      ScanPhysicalOperator scanOp = new ScanPhysicalOperator(indexName, fieldNames);
      operators.add(scanOp);
    }

    private void visitLogicalFilter(LogicalFilter logicalFilter) {
      RexNode condition = logicalFilter.getCondition();

      log.debug("Found filter: {}", condition);

      // Create filter physical operator (will be merged into scan during fragmentation)
      FilterPhysicalOperator filterOp = new FilterPhysicalOperator(condition);
      operators.add(filterOp);
    }

    private void visitLogicalProject(LogicalProject logicalProject) {
      // Extract projected field names
      List<String> projectedFields = logicalProject.getRowType().getFieldNames();

      log.debug("Found projection: fields={}", projectedFields);

      // Create projection physical operator
      ProjectionPhysicalOperator projectionOp = new ProjectionPhysicalOperator(projectedFields);
      operators.add(projectionOp);
    }

    private void visitLogicalSort(LogicalSort logicalSort) {
      // Phase 1B: Only support LIMIT (fetch), not ORDER BY
      if (logicalSort.getCollation() != null
          && !logicalSort.getCollation().getFieldCollations().isEmpty()) {
        throw new UnsupportedOperationException(
            "ORDER BY not supported in Phase 1B. Only LIMIT (fetch) is supported.");
      }

      RexNode fetch = logicalSort.fetch;
      if (fetch == null) {
        throw new UnsupportedOperationException(
            "LogicalSort without fetch clause not supported. Use LIMIT for row limiting.");
      }

      // Extract limit value
      int limit = extractLimitValue(fetch);

      log.debug("Found limit: {}", limit);

      // Create limit physical operator
      LimitPhysicalOperator limitOp = new LimitPhysicalOperator(limit);
      operators.add(limitOp);
    }

    public PhysicalOperatorTree buildOperatorTree() {
      if (indexName == null) {
        throw new IllegalStateException("No table scan found in RelNode tree");
      }

      return new PhysicalOperatorTree(indexName, operators);
    }

    private String extractIndexName(RelOptTable table) {
      // Extract index name from Calcite table
      List<String> qualifiedName = table.getQualifiedName();
      if (qualifiedName.isEmpty()) {
        throw new IllegalArgumentException("Table has empty qualified name");
      }
      // Use last part as index name
      return qualifiedName.get(qualifiedName.size() - 1);
    }

    private int extractLimitValue(RexNode fetch) {
      // Extract literal limit value from RexNode
      if (fetch.isA(org.apache.calcite.sql.SqlKind.LITERAL)) {
        org.apache.calcite.rex.RexLiteral literal = (org.apache.calcite.rex.RexLiteral) fetch;
        return literal.getValueAs(Integer.class);
      }

      throw new UnsupportedOperationException(
          "Dynamic LIMIT values not supported. Only literal integer limits are allowed.");
    }
  }
}
