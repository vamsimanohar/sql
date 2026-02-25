/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.opensearch.executor.distributed.planner.RelNodeAnalyzer.AnalysisResult;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RelNodeAnalyzerTest {

  private RelDataTypeFactory typeFactory;
  private RexBuilder rexBuilder;
  private RelOptCluster cluster;
  private RelTraitSet traitSet;
  private RelDataType rowType;

  @BeforeEach
  void setUp() {
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    cluster = RelOptCluster.create(planner, rexBuilder);
    traitSet = cluster.traitSet();
    rowType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR, 256)
            .add("age", SqlTypeName.INTEGER)
            .add("balance", SqlTypeName.DOUBLE)
            .build();
  }

  @Test
  void should_extract_index_name_and_fields_from_scan() {
    RelNode scan = createMockScan("accounts", rowType);

    AnalysisResult result = RelNodeAnalyzer.analyze(scan);

    assertEquals("accounts", result.getIndexName());
    assertEquals(List.of("name", "age", "balance"), result.getFieldNames());
    assertEquals(-1, result.getQueryLimit());
    assertNull(result.getFilterConditions());
  }

  @Test
  void should_extract_limit_from_sort() {
    RelNode scan = createMockScan("accounts", rowType);
    RexNode fetch = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10));
    LogicalSort sort = LogicalSort.create(scan, RelCollations.EMPTY, null, fetch);

    AnalysisResult result = RelNodeAnalyzer.analyze(sort);

    assertEquals("accounts", result.getIndexName());
    assertEquals(10, result.getQueryLimit());
  }

  @Test
  void should_extract_equality_filter() {
    RelNode scan = createMockScan("accounts", rowType);
    // age = 30
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode literal30 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(30));
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ageRef, literal30);
    LogicalFilter filter = LogicalFilter.create(scan, condition);

    AnalysisResult result = RelNodeAnalyzer.analyze(filter);

    assertEquals("accounts", result.getIndexName());
    assertNotNull(result.getFilterConditions());
    assertEquals(1, result.getFilterConditions().size());
    Map<String, Object> cond = result.getFilterConditions().get(0);
    assertEquals("age", cond.get("field"));
    assertEquals("EQ", cond.get("op"));
    assertEquals(30, cond.get("value"));
  }

  @Test
  void should_extract_greater_than_filter() {
    RelNode scan = createMockScan("accounts", rowType);
    // age > 30
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode literal30 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(30));
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal30);
    LogicalFilter filter = LogicalFilter.create(scan, condition);

    AnalysisResult result = RelNodeAnalyzer.analyze(filter);

    assertNotNull(result.getFilterConditions());
    assertEquals(1, result.getFilterConditions().size());
    assertEquals("GT", result.getFilterConditions().get(0).get("op"));
  }

  @Test
  void should_extract_and_filter_conditions() {
    RelNode scan = createMockScan("accounts", rowType);
    // age > 30 AND balance < 10000.0
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode literal30 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(30));
    RexNode ageCond = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal30);

    RexNode balRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.DOUBLE), 2);
    RexNode literal10000 = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(10000.0));
    RexNode balCond = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, balRef, literal10000);

    RexNode andCond = rexBuilder.makeCall(SqlStdOperatorTable.AND, ageCond, balCond);
    LogicalFilter filter = LogicalFilter.create(scan, andCond);

    AnalysisResult result = RelNodeAnalyzer.analyze(filter);

    assertNotNull(result.getFilterConditions());
    assertEquals(2, result.getFilterConditions().size());
    assertEquals("age", result.getFilterConditions().get(0).get("field"));
    assertEquals("GT", result.getFilterConditions().get(0).get("op"));
    assertEquals("balance", result.getFilterConditions().get(1).get("field"));
    assertEquals("LT", result.getFilterConditions().get(1).get("op"));
  }

  @Test
  void should_extract_filter_and_limit() {
    RelNode scan = createMockScan("accounts", rowType);
    // age > 30
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode literal30 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(30));
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal30);
    LogicalFilter filter = LogicalFilter.create(scan, condition);

    // head 10
    RexNode fetch = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10));
    LogicalSort sort = LogicalSort.create(filter, RelCollations.EMPTY, null, fetch);

    AnalysisResult result = RelNodeAnalyzer.analyze(sort);

    assertEquals("accounts", result.getIndexName());
    assertEquals(10, result.getQueryLimit());
    assertNotNull(result.getFilterConditions());
    assertEquals(1, result.getFilterConditions().size());
    assertEquals("GT", result.getFilterConditions().get(0).get("op"));
  }

  @Test
  void should_extract_projected_fields() {
    RelNode scan = createMockScan("accounts", rowType);
    // Project only name (index 0) and age (index 1)
    List<RexNode> projects =
        List.of(
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR, 256), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1));
    RelDataType projectedType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR, 256)
            .add("age", SqlTypeName.INTEGER)
            .build();
    LogicalProject project = LogicalProject.create(scan, List.of(), projects, projectedType);

    AnalysisResult result = RelNodeAnalyzer.analyze(project);

    assertEquals("accounts", result.getIndexName());
    assertEquals(List.of("name", "age"), result.getFieldNames());
  }

  @Test
  void should_throw_for_or_filter() {
    RelNode scan = createMockScan("accounts", rowType);
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode literal30 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(30));
    RexNode cond1 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ageRef, literal30);
    RexNode cond2 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, ageRef, literal30);
    RexNode orCond = rexBuilder.makeCall(SqlStdOperatorTable.OR, cond1, cond2);
    LogicalFilter filter = LogicalFilter.create(scan, orCond);

    assertThrows(UnsupportedOperationException.class, () -> RelNodeAnalyzer.analyze(filter));
  }

  @Test
  void should_reject_aggregation() {
    RelNode scan = createMockScan("accounts", rowType);
    // stats count() by age → LogicalAggregate
    LogicalAggregate aggregate =
        LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(1), null, List.of());

    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> RelNodeAnalyzer.analyze(aggregate));
    assert ex.getMessage().contains("Aggregation");
  }

  @Test
  void should_reject_sort_with_collation() {
    RelNode scan = createMockScan("accounts", rowType);
    // sort age → LogicalSort with collation on field 1 (age)
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));
    LogicalSort sort = LogicalSort.create(scan, collation, null, null);

    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> RelNodeAnalyzer.analyze(sort));
    assert ex.getMessage().contains("Sort");
  }

  @Test
  void should_reject_sort_with_collation_and_limit() {
    RelNode scan = createMockScan("accounts", rowType);
    // sort age | head 5 → LogicalSort with collation AND fetch
    RelCollation collation =
        RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.ASCENDING));
    RexNode fetch = rexBuilder.makeExactLiteral(BigDecimal.valueOf(5));
    LogicalSort sort = LogicalSort.create(scan, collation, null, fetch);

    UnsupportedOperationException ex =
        assertThrows(UnsupportedOperationException.class, () -> RelNodeAnalyzer.analyze(sort));
    assert ex.getMessage().contains("Sort");
  }

  @Test
  void should_handle_reversed_comparison() {
    RelNode scan = createMockScan("accounts", rowType);
    // 30 < age  →  age > 30
    RexNode literal30 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(30));
    RexNode ageRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, literal30, ageRef);
    LogicalFilter filter = LogicalFilter.create(scan, condition);

    AnalysisResult result = RelNodeAnalyzer.analyze(filter);

    assertNotNull(result.getFilterConditions());
    assertEquals("age", result.getFilterConditions().get(0).get("field"));
    assertEquals("GT", result.getFilterConditions().get(0).get("op"));
    assertEquals(30, result.getFilterConditions().get(0).get("value"));
  }

  /**
   * Creates a mock AbstractCalciteIndexScan that returns the given index name and row type. Uses
   * Mockito to avoid the complex setup required for a real scan node.
   */
  private RelNode createMockScan(String indexName, RelDataType scanRowType) {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class);
    RelOptTable table = mock(RelOptTable.class);
    when(table.getQualifiedName()).thenReturn(List.of(indexName));
    when(scan.getTable()).thenReturn(table);
    when(scan.getRowType()).thenReturn(scanRowType);
    when(scan.getInputs()).thenReturn(List.of());
    when(scan.getTraitSet()).thenReturn(traitSet);
    when(scan.getCluster()).thenReturn(cluster);
    return scan;
  }
}
