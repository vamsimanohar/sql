/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.opensearch.executor.distributed.dataunit.OpenSearchDataUnit;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitSource;
import org.opensearch.sql.planner.distributed.planner.CostEstimator;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;
import org.opensearch.sql.planner.distributed.stage.ExchangeType;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SimplePlanFragmenterTest {

  private SimplePlanFragmenter fragmenter;
  @Mock private FragmentationContext context;
  @Mock private DataUnitSource dataUnitSource;
  @Mock private CostEstimator costEstimator;

  private RelDataTypeFactory typeFactory;
  private RexBuilder rexBuilder;
  private RelOptCluster cluster;
  private RelTraitSet traitSet;

  @BeforeEach
  void setUp() {
    fragmenter = new SimplePlanFragmenter();
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    cluster = RelOptCluster.create(planner, rexBuilder);
    traitSet = cluster.traitSet();
  }

  @Test
  void should_create_two_stage_plan_for_scan() {
    RelDataType rowType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR, 256)
            .add("age", SqlTypeName.INTEGER)
            .build();
    RelNode scan = createMockScan("accounts", rowType);

    List<DataUnit> shards =
        List.of(
            new OpenSearchDataUnit("accounts", 0, List.of("node-1"), -1, -1),
            new OpenSearchDataUnit("accounts", 1, List.of("node-2"), -1, -1));

    when(context.getDataUnitSource("accounts")).thenReturn(dataUnitSource);
    when(dataUnitSource.getNextBatch()).thenReturn(shards);
    when(context.getCostEstimator()).thenReturn(costEstimator);
    when(costEstimator.estimateRowCount(scan)).thenReturn(-1L);

    StagedPlan plan = fragmenter.fragment(scan, context);

    assertNotNull(plan);
    assertNotNull(plan.getPlanId());
    assertTrue(plan.getPlanId().startsWith("plan-"));
    assertEquals(2, plan.getStageCount());
    assertTrue(plan.validate().isEmpty());

    // Leaf stage
    ComputeStage leaf = plan.getLeafStages().get(0);
    assertEquals("0", leaf.getStageId());
    assertTrue(leaf.isLeaf());
    assertEquals(ExchangeType.GATHER, leaf.getOutputPartitioning().getExchangeType());
    assertEquals(2, leaf.getDataUnits().size());
    assertNotNull(leaf.getPlanFragment());

    // Root stage
    ComputeStage root = plan.getRootStage();
    assertEquals("1", root.getStageId());
    assertFalse(root.isLeaf());
    assertEquals(ExchangeType.NONE, root.getOutputPartitioning().getExchangeType());
    assertEquals(List.of("0"), root.getSourceStageIds());
    assertTrue(root.getDataUnits().isEmpty());
  }

  @Test
  void should_include_all_shards_in_leaf_stage() {
    RelDataType rowType = typeFactory.builder().add("field1", SqlTypeName.VARCHAR, 256).build();
    RelNode scan = createMockScan("logs", rowType);

    List<DataUnit> shards =
        List.of(
            new OpenSearchDataUnit("logs", 0, List.of("n1"), -1, -1),
            new OpenSearchDataUnit("logs", 1, List.of("n2"), -1, -1),
            new OpenSearchDataUnit("logs", 2, List.of("n3"), -1, -1),
            new OpenSearchDataUnit("logs", 3, List.of("n1"), -1, -1),
            new OpenSearchDataUnit("logs", 4, List.of("n2"), -1, -1));

    when(context.getDataUnitSource("logs")).thenReturn(dataUnitSource);
    when(dataUnitSource.getNextBatch()).thenReturn(shards);
    when(context.getCostEstimator()).thenReturn(costEstimator);
    when(costEstimator.estimateRowCount(scan)).thenReturn(-1L);

    StagedPlan plan = fragmenter.fragment(scan, context);

    assertEquals(5, plan.getLeafStages().get(0).getDataUnits().size());
  }

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
