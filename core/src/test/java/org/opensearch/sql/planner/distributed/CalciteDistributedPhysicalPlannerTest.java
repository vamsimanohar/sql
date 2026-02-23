/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.sql.calcite.CalcitePlanContext;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CalciteDistributedPhysicalPlannerTest {

  @Mock private CalciteDistributedPhysicalPlanner.PartitionDiscovery partitionDiscovery;
  @Mock private CalcitePlanContext planContext;
  @Mock private TableScan tableScan;
  @Mock private LogicalFilter filter;
  @Mock private LogicalProject project;
  @Mock private LogicalAggregate aggregate;
  @Mock private LogicalSort sort;

  private CalciteDistributedPhysicalPlanner planner;

  @BeforeEach
  void setUp() {
    planner = new CalciteDistributedPhysicalPlanner(partitionDiscovery);

    // Setup mock partition discovery
    DataPartition mockPartition =
        DataPartition.createLucenePartition("shard-1", "index-1", "node-1", 1024L);
    when(partitionDiscovery.discoverPartitions(any())).thenReturn(List.of(mockPartition));
  }

  @Test
  void should_create_distributed_plan_from_table_scan() {
    // Given - Setup table scan mock to return qualified name
    when(tableScan.getTable())
        .thenReturn(
            new org.apache.calcite.plan.RelOptAbstractTable(null, "test-table", null) {
              @Override
              public List<String> getQualifiedName() {
                return List.of("default", "index-1");
              }

              @Override
              public org.apache.calcite.rel.type.RelDataType getRowType() {
                return null;
              }
            });
    when(tableScan.getInputs()).thenReturn(List.of());

    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    assertNotNull(plan);
    assertNotNull(plan.getPlanId());
    assertFalse(plan.getExecutionStages().isEmpty());
  }

  @Test
  void should_handle_empty_partition_list() {
    // Given
    when(partitionDiscovery.discoverPartitions(any())).thenReturn(List.of());
    when(tableScan.getTable())
        .thenReturn(
            new org.apache.calcite.plan.RelOptAbstractTable(null, "test-table", null) {
              @Override
              public List<String> getQualifiedName() {
                return List.of("default", "index-1");
              }

              @Override
              public org.apache.calcite.rel.type.RelDataType getRowType() {
                return null;
              }
            });
    when(tableScan.getInputs()).thenReturn(List.of());

    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then - Should still create a valid plan, even with no partitions
    assertNotNull(plan);
    assertNotNull(plan.getExecutionStages());
  }

  @Test
  void should_create_unique_plan_ids() {
    // Given
    when(tableScan.getTable())
        .thenReturn(
            new org.apache.calcite.plan.RelOptAbstractTable(null, "test-table", null) {
              @Override
              public List<String> getQualifiedName() {
                return List.of("default", "index-1");
              }

              @Override
              public org.apache.calcite.rel.type.RelDataType getRowType() {
                return null;
              }
            });
    when(tableScan.getInputs()).thenReturn(List.of());

    // When
    DistributedPhysicalPlan plan1 = planner.plan(tableScan, planContext);
    DistributedPhysicalPlan plan2 = planner.plan(tableScan, planContext);

    // Then
    assertNotNull(plan1.getPlanId());
    assertNotNull(plan2.getPlanId());
    assertFalse(
        plan1.getPlanId().equals(plan2.getPlanId()), "Plan IDs should be unique for each plan");
  }

  @Test
  void should_create_plan_with_correct_metadata() {
    // Given
    when(tableScan.getTable())
        .thenReturn(
            new org.apache.calcite.plan.RelOptAbstractTable(null, "test-table", null) {
              @Override
              public List<String> getQualifiedName() {
                return List.of("default", "index-1");
              }

              @Override
              public org.apache.calcite.rel.type.RelDataType getRowType() {
                return null;
              }
            });
    when(tableScan.getInputs()).thenReturn(List.of());

    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    assertNotNull(plan.getPlanId());
    assertTrue(plan.getPlanId().startsWith("calcite-distributed-plan-"));
    assertEquals(DistributedPhysicalPlan.PlanStatus.CREATED, plan.getStatus());
  }

  @Test
  void should_create_work_units_for_each_partition() {
    // Given
    DataPartition partition1 =
        DataPartition.createLucenePartition("shard-1", "index-1", "node-1", 1024L);
    DataPartition partition2 =
        DataPartition.createLucenePartition("shard-2", "index-1", "node-2", 1024L);
    DataPartition partition3 =
        DataPartition.createLucenePartition("shard-3", "index-2", "node-1", 1024L);
    when(partitionDiscovery.discoverPartitions(any()))
        .thenReturn(List.of(partition1, partition2, partition3));

    when(tableScan.getTable())
        .thenReturn(
            new org.apache.calcite.plan.RelOptAbstractTable(null, "test-table", null) {
              @Override
              public List<String> getQualifiedName() {
                return List.of("default", "index-1");
              }

              @Override
              public org.apache.calcite.rel.type.RelDataType getRowType() {
                return null;
              }
            });
    when(tableScan.getInputs()).thenReturn(List.of());

    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    List<ExecutionStage> stages = plan.getExecutionStages();
    assertFalse(stages.isEmpty());

    ExecutionStage scanStage = stages.get(0);
    assertEquals(ExecutionStage.StageType.SCAN, scanStage.getStageType());

    List<WorkUnit> workUnits = scanStage.getWorkUnits();
    assertEquals(3, workUnits.size(), "Should create one work unit per partition");
  }
}
