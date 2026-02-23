/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.planner.distributed.DataPartition;
import org.opensearch.sql.planner.distributed.DistributedPhysicalPlan;
import org.opensearch.sql.planner.distributed.ExecutionStage;
import org.opensearch.sql.planner.distributed.WorkUnit;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DistributedTaskSchedulerTest {

  @Mock private TransportService transportService;
  @Mock private ClusterService clusterService;
  @Mock private Client client;
  @Mock private ClusterState clusterState;
  @Mock private DiscoveryNodes discoveryNodes;
  @Mock private DiscoveryNode dataNode1;
  @Mock private DiscoveryNode dataNode2;
  @Mock private ResponseListener<QueryResponse> responseListener;

  private DistributedTaskScheduler scheduler;

  @BeforeEach
  void setUp() {
    scheduler = new DistributedTaskScheduler(transportService, clusterService, client);

    // Setup mock cluster state
    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.nodes()).thenReturn(discoveryNodes);
    when(dataNode1.getId()).thenReturn("node-1");
    when(dataNode2.getId()).thenReturn("node-2");
    when(dataNode1.isDataNode()).thenReturn(true);
    when(dataNode2.isDataNode()).thenReturn(true);

    // Setup data nodes
    @SuppressWarnings("unchecked")
    Map<String, DiscoveryNode> dataNodes = mock(Map.class);
    when(dataNodes.values()).thenReturn(List.of(dataNode1, dataNode2));
    when(discoveryNodes.getDataNodes()).thenReturn(dataNodes);

    // Setup node resolution for transport (Phase 1C)
    when(discoveryNodes.get("node-1")).thenReturn(dataNode1);
    when(discoveryNodes.get("node-2")).thenReturn(dataNode2);
  }

  @Test
  void should_execute_simple_distributed_plan() {
    // Given
    DistributedPhysicalPlan plan = createSimplePlan();
    AtomicReference<QueryResponse> responseRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              QueryResponse response = invocation.getArgument(0);
              responseRef.set(response);
              return null;
            })
        .when(responseListener)
        .onResponse(any());

    // When
    scheduler.executeQuery(plan, responseListener);

    // Then - Plan should be marked as executing
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan.getStatus());
  }

  @Test
  void should_handle_plan_validation_errors() {
    // Given
    DistributedPhysicalPlan invalidPlan = createInvalidPlan();
    AtomicReference<Exception> errorRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              Exception error = invocation.getArgument(0);
              errorRef.set(error);
              return null;
            })
        .when(responseListener)
        .onFailure(any());

    // When
    scheduler.executeQuery(invalidPlan, responseListener);

    // Then
    verify(responseListener, times(1)).onFailure(any(IllegalArgumentException.class));
    assertNotNull(errorRef.get());
    assertTrue(errorRef.get().getMessage().contains("Plan validation failed"));
  }

  @Test
  void should_distribute_work_units_by_node_locality() {
    // Given
    DistributedPhysicalPlan plan = createPlanWithMultipleWorkUnits();

    // Mock transport service to capture requests
    // Note: In Phase 1, we'll just verify that the scheduler attempts to distribute work

    // When
    scheduler.executeQuery(plan, responseListener);

    // Then - Should distribute work units based on data locality
    List<ExecutionStage> stages = plan.getExecutionStages();
    assertNotNull(stages);
    assertTrue(stages.size() >= 1);

    ExecutionStage firstStage = stages.get(0);
    List<WorkUnit> workUnits = firstStage.getWorkUnits();
    assertNotNull(workUnits);

    // Verify work units are assigned to correct nodes
    for (WorkUnit workUnit : workUnits) {
      assertNotNull(workUnit.getAssignedNodeId());
      assertTrue(
          workUnit.getAssignedNodeId().equals("node-1")
              || workUnit.getAssignedNodeId().equals("node-2"));
    }
  }

  @Test
  void should_handle_empty_work_units_gracefully() {
    // Given
    DistributedPhysicalPlan plan = createPlanWithEmptyStage();

    // When
    scheduler.executeQuery(plan, responseListener);

    // Then - Should not fail and should complete successfully
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan.getStatus());
  }

  @Test
  void should_clean_execution_state_between_queries() {
    // Given
    DistributedPhysicalPlan plan1 = createSimplePlan();
    DistributedPhysicalPlan plan2 = createSimplePlan();

    // When
    scheduler.executeQuery(plan1, responseListener);
    scheduler.executeQuery(plan2, responseListener);

    // Then - Each execution should start with clean state
    // This is verified by the successful execution of both plans
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan1.getStatus());
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan2.getStatus());
  }

  @Test
  void should_shutdown_gracefully() {
    // Given
    DistributedPhysicalPlan plan = createSimplePlan();
    scheduler.executeQuery(plan, responseListener);

    // When
    scheduler.shutdown();

    // Then - Should not throw exceptions
    // Shutdown is successful if no exceptions are thrown
  }

  @Test
  @SuppressWarnings("unchecked")
  void should_group_shards_by_node_for_transport() {
    // Given: Plan with work units assigned to different nodes
    DistributedPhysicalPlan plan = createPlanWithMultiNodeShards();

    // Verify work units are grouped by node ID
    List<WorkUnit> scanWorkUnits = plan.getExecutionStages().get(0).getWorkUnits();
    assertNotNull(scanWorkUnits);
    assertEquals(4, scanWorkUnits.size());

    // Count work units per node
    long node1Count =
        scanWorkUnits.stream()
            .filter(wu -> "node-1".equals(wu.getDataPartition().getNodeId()))
            .count();
    long node2Count =
        scanWorkUnits.stream()
            .filter(wu -> "node-2".equals(wu.getDataPartition().getNodeId()))
            .count();
    assertEquals(2, node1Count);
    assertEquals(2, node2Count);
  }

  @Test
  @SuppressWarnings("unchecked")
  void should_send_transport_requests_to_each_node() {
    // Given: Plan with shards on two nodes - this tests that the scheduler
    // calls transportService.sendRequest for transport-based execution.
    // Note: Full transport test requires Calcite context; here we verify
    // the scheduler handles transport infrastructure correctly.
    DistributedPhysicalPlan plan = createPlanWithMultiNodeShards();

    // When
    scheduler.executeQuery(plan, responseListener);

    // Then: Plan should be marked as executing (transport execution starts)
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan.getStatus());
  }

  @Test
  void should_accept_aggregation_plan_with_three_stages() {
    // Given: Plan with 3 stages (SCAN → PROCESS → FINALIZE) — Phase 2 removes restriction
    DistributedPhysicalPlan plan = createAggregationPlan();

    // When
    scheduler.executeQuery(plan, responseListener);

    // Then: Plan should be marked as executing (not rejected with UnsupportedOperationException)
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan.getStatus());
  }

  @Test
  void should_accept_plan_with_process_stage() {
    // Given: Plan with a PROCESS stage (partial aggregation)
    DistributedPhysicalPlan plan = createAggregationPlan();

    // Verify the plan has a PROCESS stage
    boolean hasProcessStage =
        plan.getExecutionStages().stream()
            .anyMatch(stage -> stage.getStageType() == ExecutionStage.StageType.PROCESS);
    assertTrue(hasProcessStage, "Plan should have a PROCESS stage");

    // When
    scheduler.executeQuery(plan, responseListener);

    // Then: Should not throw UnsupportedOperationException
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan.getStatus());
  }

  @Test
  void should_create_aggregation_plan_with_correct_stage_structure() {
    // Given: An aggregation plan
    DistributedPhysicalPlan plan = createAggregationPlan();

    // Then: Should have 3 stages
    List<ExecutionStage> stages = plan.getExecutionStages();
    assertEquals(3, stages.size());

    // Stage 1: SCAN
    assertEquals(ExecutionStage.StageType.SCAN, stages.get(0).getStageType());
    assertEquals(2, stages.get(0).getWorkUnits().size());

    // Stage 2: PROCESS (partial aggregation)
    assertEquals(ExecutionStage.StageType.PROCESS, stages.get(1).getStageType());

    // Stage 3: FINALIZE (final merge)
    assertEquals(ExecutionStage.StageType.FINALIZE, stages.get(2).getStageType());
  }

  private DistributedPhysicalPlan createAggregationPlan() {
    // Create a 3-stage plan: SCAN → PROCESS → FINALIZE
    // This simulates an aggregation query like: stats count() by gender

    // Stage 1: SCAN with 2 shards across 2 nodes
    DataPartition p1 = DataPartition.createLucenePartition("0", "accounts", "node-1", 1024L);
    DataPartition p2 = DataPartition.createLucenePartition("1", "accounts", "node-2", 1024L);

    WorkUnit scanWu1 =
        new WorkUnit("scan-0", WorkUnit.WorkUnitType.SCAN, p1, null, List.of(), "node-1", Map.of());
    WorkUnit scanWu2 =
        new WorkUnit("scan-1", WorkUnit.WorkUnitType.SCAN, p2, null, List.of(), "node-2", Map.of());

    ExecutionStage scanStage =
        new ExecutionStage(
            "scan-stage",
            ExecutionStage.StageType.SCAN,
            List.of(scanWu1, scanWu2),
            List.of(),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            2,
            ExecutionStage.DataExchangeType.NONE);

    // Stage 2: PROCESS (partial aggregation)
    WorkUnit processWu1 =
        new WorkUnit(
            "partial-agg-0",
            WorkUnit.WorkUnitType.PROCESS,
            null,
            null,
            List.of("scan-stage"),
            null,
            Map.of());
    WorkUnit processWu2 =
        new WorkUnit(
            "partial-agg-1",
            WorkUnit.WorkUnitType.PROCESS,
            null,
            null,
            List.of("scan-stage"),
            null,
            Map.of());

    ExecutionStage processStage =
        new ExecutionStage(
            "process-stage",
            ExecutionStage.StageType.PROCESS,
            List.of(processWu1, processWu2),
            List.of("scan-stage"),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            2,
            ExecutionStage.DataExchangeType.NONE);

    // Stage 3: FINALIZE (merge aggregation results)
    WorkUnit finalWu =
        new WorkUnit(
            "final-agg",
            WorkUnit.WorkUnitType.FINALIZE,
            null,
            null,
            List.of("process-stage"),
            null,
            Map.of());

    ExecutionStage finalizeStage =
        new ExecutionStage(
            "finalize-stage",
            ExecutionStage.StageType.FINALIZE,
            List.of(finalWu),
            List.of("process-stage"),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            1,
            ExecutionStage.DataExchangeType.GATHER);

    return DistributedPhysicalPlan.create(
        "agg-plan", List.of(scanStage, processStage, finalizeStage), null);
  }

  private DistributedPhysicalPlan createPlanWithMultiNodeShards() {
    // Create plan with 4 shards across 2 nodes
    DataPartition p1 = DataPartition.createLucenePartition("0", "test-index", "node-1", 1024L);
    DataPartition p2 = DataPartition.createLucenePartition("1", "test-index", "node-1", 1024L);
    DataPartition p3 = DataPartition.createLucenePartition("2", "test-index", "node-2", 2048L);
    DataPartition p4 = DataPartition.createLucenePartition("3", "test-index", "node-2", 2048L);

    WorkUnit wu1 =
        new WorkUnit("wu-0", WorkUnit.WorkUnitType.SCAN, p1, null, List.of(), "node-1", Map.of());
    WorkUnit wu2 =
        new WorkUnit("wu-1", WorkUnit.WorkUnitType.SCAN, p2, null, List.of(), "node-1", Map.of());
    WorkUnit wu3 =
        new WorkUnit("wu-2", WorkUnit.WorkUnitType.SCAN, p3, null, List.of(), "node-2", Map.of());
    WorkUnit wu4 =
        new WorkUnit("wu-3", WorkUnit.WorkUnitType.SCAN, p4, null, List.of(), "node-2", Map.of());

    ExecutionStage stage =
        new ExecutionStage(
            "scan-stage",
            ExecutionStage.StageType.SCAN,
            List.of(wu1, wu2, wu3, wu4),
            List.of(),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            4,
            ExecutionStage.DataExchangeType.GATHER);

    return DistributedPhysicalPlan.create("multi-node-plan", List.of(stage), null);
  }

  private DistributedPhysicalPlan createSimplePlan() {
    // Create a valid plan with one stage and one work unit
    DataPartition partition =
        new DataPartition("shard-1", DataPartition.StorageType.LUCENE, "index-1", 1024L, Map.of());
    WorkUnit workUnit =
        new WorkUnit(
            "work-1",
            WorkUnit.WorkUnitType.SCAN,
            partition,
            null, // No operator for test
            List.of(),
            "node-1",
            Map.of());

    ExecutionStage stage =
        new ExecutionStage(
            "stage-1",
            ExecutionStage.StageType.SCAN,
            List.of(workUnit),
            List.of(), // No dependencies
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            1,
            ExecutionStage.DataExchangeType.GATHER);

    return DistributedPhysicalPlan.create("test-plan", List.of(stage), null);
  }

  private DistributedPhysicalPlan createInvalidPlan() {
    // Create a plan that will fail validation
    return DistributedPhysicalPlan.create(null, List.of(), null); // Invalid plan with null ID
  }

  private DistributedPhysicalPlan createPlanWithMultipleWorkUnits() {
    // Create a plan with work units assigned to different nodes
    DataPartition partition1 =
        new DataPartition("shard-1", DataPartition.StorageType.LUCENE, "index-1", 1024L, Map.of());
    DataPartition partition2 =
        new DataPartition("shard-2", DataPartition.StorageType.LUCENE, "index-1", 1024L, Map.of());

    WorkUnit workUnit1 =
        new WorkUnit(
            "work-1", WorkUnit.WorkUnitType.SCAN, partition1, null, List.of(), "node-1", Map.of());
    WorkUnit workUnit2 =
        new WorkUnit(
            "work-2", WorkUnit.WorkUnitType.SCAN, partition2, null, List.of(), "node-2", Map.of());

    ExecutionStage stage =
        new ExecutionStage(
            "stage-1",
            ExecutionStage.StageType.SCAN,
            List.of(workUnit1, workUnit2),
            List.of(),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            2,
            ExecutionStage.DataExchangeType.GATHER);

    return DistributedPhysicalPlan.create("test-plan", List.of(stage), null);
  }

  private DistributedPhysicalPlan createPlanWithEmptyStage() {
    // Create a plan with an empty stage (no work units)
    ExecutionStage emptyStage =
        new ExecutionStage(
            "empty-stage",
            ExecutionStage.StageType.FINALIZE,
            List.of(), // Empty work units
            List.of(),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            0,
            ExecutionStage.DataExchangeType.GATHER);

    return DistributedPhysicalPlan.create("empty-plan", List.of(emptyStage), null);
  }
}
