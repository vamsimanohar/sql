/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitSource;
import org.opensearch.sql.planner.distributed.planner.CostEstimator;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

/** Unit tests for CalciteDistributedPhysicalPlanner. */
@ExtendWith(MockitoExtension.class)
class CalciteDistributedPhysicalPlannerTest {

  @Mock private FragmentationContext fragmentationContext;
  @Mock private CostEstimator costEstimator;
  @Mock private DataUnitSource dataUnitSource;
  @Mock private DataUnit dataUnit1;
  @Mock private DataUnit dataUnit2;
  @Mock private TableScan tableScan;
  @Mock private RelOptTable relOptTable;
  @Mock private RelDataType relDataType;

  private CalciteDistributedPhysicalPlanner planner;

  @BeforeEach
  void setUp() {
    lenient().when(fragmentationContext.getCostEstimator()).thenReturn(costEstimator);
    lenient().when(fragmentationContext.getDataUnitSource("test_index")).thenReturn(dataUnitSource);
    lenient().when(dataUnitSource.getNextBatch()).thenReturn(Arrays.asList(dataUnit1, dataUnit2));
    lenient().when(costEstimator.estimateRowCount(any())).thenReturn(1000L);
    lenient().when(costEstimator.estimateSizeBytes(any())).thenReturn(50000L);

    planner = new CalciteDistributedPhysicalPlanner(fragmentationContext);
  }

  @Test
  void testPlanSimpleTableScan() {
    // Setup table scan mock
    when(tableScan.getTable()).thenReturn(relOptTable);
    when(relOptTable.getQualifiedName()).thenReturn(Arrays.asList("test_index"));
    when(tableScan.getRowType()).thenReturn(relDataType);
    when(relDataType.getFieldNames()).thenReturn(Arrays.asList("field1", "field2"));

    // Test planning
    StagedPlan result = planner.plan(tableScan);

    // Verify plan structure
    assertNotNull(result);
    assertTrue(result.getPlanId().startsWith("plan-"));
    assertEquals(2, result.getStageCount());

    // Verify leaf stage
    ComputeStage leafStage = result.getLeafStages().get(0);
    assertTrue(leafStage.isLeaf());
    assertEquals("0", leafStage.getStageId());
    assertEquals(2, leafStage.getDataUnits().size());

    // Verify root stage
    ComputeStage rootStage = result.getRootStage();
    assertFalse(rootStage.isLeaf());
    assertEquals("1", rootStage.getStageId());
    assertEquals(0, rootStage.getDataUnits().size());
    assertEquals(Arrays.asList("0"), rootStage.getSourceStageIds());

    // Verify data unit source was called
    verify(dataUnitSource).getNextBatch();
    verify(dataUnitSource).close();
  }

  @Test
  void testPlanWithUnsupportedOperator() {
    // Setup unsupported RelNode
    RelNode unsupportedNode = mock(RelNode.class);

    // Test planning should throw exception
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class, () -> planner.plan(unsupportedNode));

    assertTrue(exception.getMessage().contains("Unsupported RelNode type for Phase 1B"));
  }

  @Test
  void testFragmentationContextIntegration() {
    // Test that planner properly uses fragmentation context
    when(tableScan.getTable()).thenReturn(relOptTable);
    when(relOptTable.getQualifiedName()).thenReturn(Arrays.asList("test_index"));
    when(tableScan.getRowType()).thenReturn(relDataType);
    when(relDataType.getFieldNames()).thenReturn(Arrays.asList("field1"));

    planner.plan(tableScan);

    // Verify fragmentation context was used
    verify(fragmentationContext).getCostEstimator();
    verify(fragmentationContext).getDataUnitSource("test_index");
  }
}
