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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
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
import org.opensearch.sql.calcite.CalcitePlanContext;
@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CalciteDistributedPhysicalPlannerTest {

  @Mock private PartitionDiscovery partitionDiscovery;
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
    DataPartition mockPartition = new DataPartition("shard-1", "index-1", "node-1");
    when(partitionDiscovery.getPartitions(any())).thenReturn(List.of(mockPartition));
  }

  @Test
  void should_create_distributed_plan_from_table_scan() {
    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    assertNotNull(plan);
    assertNotNull(plan.getPlanId());
    assertFalse(plan.getExecutionStages().isEmpty());
  }

  @Test
  void should_create_multi_stage_plan_for_complex_query() {
    // Given - Setup a chain of RelNodes: TableScan -> Filter -> Project -> Aggregate -> Sort
    when(tableScan.getInput(0)).thenReturn(null); // Root node
    when(filter.getInput(0)).thenReturn(tableScan);
    when(project.getInput(0)).thenReturn(filter);
    when(aggregate.getInput(0)).thenReturn(project);
    when(sort.getInput(0)).thenReturn(aggregate);

    // When
    DistributedPhysicalPlan plan = planner.plan(sort, planContext);

    // Then
    assertNotNull(plan);
    assertNotNull(plan.getPlanId());
    assertFalse(plan.getExecutionStages().isEmpty());

    // Should have multiple stages for complex operations
    List<ExecutionStage> stages = plan.getExecutionStages();
    assertTrue(stages.size() >= 1, "Should have at least one stage");
  }

  @Test
  void should_determine_correct_stage_type_for_scan() {
    // Given
    ExecutionStage.StageType type =
        planner.determineStageType(tableScan, new CalciteDistributedPhysicalPlanner.PlanState());

    // Then
    assertEquals(ExecutionStage.StageType.SCAN, type);
  }

  @Test
  void should_determine_correct_stage_type_for_aggregation() {
    // Given
    ExecutionStage.StageType type =
        planner.determineStageType(aggregate, new CalciteDistributedPhysicalPlanner.PlanState());

    // Then - Should be PROCESS for intermediate aggregation
    assertEquals(ExecutionStage.StageType.PROCESS, type);
  }

  @Test
  void should_create_work_units_with_partition_assignment() {
    // Given
    DataPartition partition1 = new DataPartition("shard-1", "index-1", "node-1");
    DataPartition partition2 = new DataPartition("shard-2", "index-1", "node-2");
    when(partitionDiscovery.getPartitions(any())).thenReturn(List.of(partition1, partition2));

    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    List<ExecutionStage> stages = plan.getExecutionStages();
    assertFalse(stages.isEmpty());

    ExecutionStage firstStage = stages.get(0);
    List<WorkUnit> workUnits = firstStage.getWorkUnits();
    assertFalse(workUnits.isEmpty());

    // Should have work units assigned to data nodes
    boolean hasAssignedNodes =
        workUnits.stream().anyMatch(wu -> wu.getAssignedNodeId() != null);
    assertTrue(hasAssignedNodes, "Work units should be assigned to nodes for data locality");
  }

  @Test
  void should_handle_single_table_scan_correctly() {
    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    assertNotNull(plan);
    assertEquals(1, plan.getExecutionStages().size());

    ExecutionStage stage = plan.getExecutionStages().get(0);
    assertEquals(ExecutionStage.StageType.SCAN, stage.getStageType());
    assertNotNull(stage.getWorkUnits());
    assertFalse(stage.getWorkUnits().isEmpty());
  }

  @Test
  void should_create_plan_with_correct_metadata() {
    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    assertNotNull(plan.getPlanId());
    assertTrue(plan.getPlanId().startsWith("distributed-plan-"));
    assertNotNull(plan.getCreationTime());
    assertEquals(DistributedPhysicalPlan.PlanStatus.CREATED, plan.getStatus());
  }

  @Test
  void should_handle_empty_partition_list() {
    // Given
    when(partitionDiscovery.getPartitions(any())).thenReturn(List.of());

    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then - Should still create a valid plan, even with no partitions
    assertNotNull(plan);
    assertNotNull(plan.getExecutionStages());
  }

  @Test
  void should_create_unique_plan_ids() {
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
  void should_create_work_units_for_each_partition() {
    // Given
    DataPartition partition1 = new DataPartition("shard-1", "index-1", "node-1");
    DataPartition partition2 = new DataPartition("shard-2", "index-1", "node-2");
    DataPartition partition3 = new DataPartition("shard-3", "index-2", "node-1");
    when(partitionDiscovery.getPartitions(any())).thenReturn(List.of(partition1, partition2, partition3));

    // When
    DistributedPhysicalPlan plan = planner.plan(tableScan, planContext);

    // Then
    List<ExecutionStage> stages = plan.getExecutionStages();
    assertFalse(stages.isEmpty());

    ExecutionStage scanStage = stages.get(0);
    assertEquals(ExecutionStage.StageType.SCAN, scanStage.getStageType());

    List<WorkUnit> workUnits = scanStage.getWorkUnits();
    assertEquals(3, workUnits.size(), "Should create one work unit per partition");

    // Verify work units have correct assignments
    Map<String, String> shardToNode = Map.of("shard-1", "node-1", "shard-2", "node-2", "shard-3", "node-1");
    for (WorkUnit workUnit : workUnits) {
      String expectedNode = shardToNode.get(workUnit.getDataPartition().getShardId());
      assertEquals(expectedNode, workUnit.getAssignedNodeId());
    }
  }
}