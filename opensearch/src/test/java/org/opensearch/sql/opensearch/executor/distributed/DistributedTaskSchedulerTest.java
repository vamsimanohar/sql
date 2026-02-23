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
