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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.planner.distributed.DataPartition;
import org.opensearch.sql.planner.distributed.WorkUnit;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TransportExecuteDistributedTaskActionTest {

  @Mock private TransportService transportService;
  @Mock private ClusterService clusterService;
  @Mock private ActionFilters actionFilters;
  @Mock private Task task;
  @Mock private ActionListener<ExecuteDistributedTaskResponse> actionListener;

  private TransportExecuteDistributedTaskAction action;

  @BeforeEach
  void setUp() {
    action =
        new TransportExecuteDistributedTaskAction(transportService, actionFilters, clusterService);

    // Setup cluster service mock
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    when(localNode.getId()).thenReturn("test-node-1");
    when(clusterService.localNode()).thenReturn(localNode);
  }

  @Test
  void should_handle_valid_request_with_work_units() {
    // Given
    ExecuteDistributedTaskRequest request = createValidRequest();
    AtomicReference<ExecuteDistributedTaskResponse> responseRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              ExecuteDistributedTaskResponse response = invocation.getArgument(0);
              responseRef.set(response);
              return null;
            })
        .when(actionListener)
        .onResponse(any());

    // When
    action.doExecute(task, request, actionListener);

    // Then
    verify(actionListener).onResponse(any(ExecuteDistributedTaskResponse.class));
    ExecuteDistributedTaskResponse response = responseRef.get();
    assertNotNull(response);
    assertTrue(response.isSuccessful());
    assertEquals("test-node-1", response.getNodeId());
  }

  @Test
  void should_handle_empty_work_units_request() {
    // Given
    ExecuteDistributedTaskRequest request = createEmptyRequest();
    AtomicReference<ExecuteDistributedTaskResponse> responseRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              ExecuteDistributedTaskResponse response = invocation.getArgument(0);
              responseRef.set(response);
              return null;
            })
        .when(actionListener)
        .onResponse(any());

    // When
    action.doExecute(task, request, actionListener);

    // Then
    verify(actionListener).onResponse(any(ExecuteDistributedTaskResponse.class));
    ExecuteDistributedTaskResponse response = responseRef.get();
    assertNotNull(response);
    assertTrue(response.isSuccessful());
    assertEquals(0, response.getResultCount());
  }

  @Test
  void should_handle_request_execution_error() {
    // Given
    ExecuteDistributedTaskRequest invalidRequest = createInvalidRequest();
    AtomicReference<ExecuteDistributedTaskResponse> responseRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              ExecuteDistributedTaskResponse response = invocation.getArgument(0);
              responseRef.set(response);
              return null;
            })
        .when(actionListener)
        .onResponse(any());

    // When
    action.doExecute(task, invalidRequest, actionListener);

    // Then
    verify(actionListener).onResponse(any(ExecuteDistributedTaskResponse.class));
    ExecuteDistributedTaskResponse response = responseRef.get();
    assertNotNull(response);
    // Request with null stageId is invalid, so response should indicate failure
    assertTrue(!response.isSuccessful());
  }

  @Test
  void should_return_execution_statistics() {
    // Given
    ExecuteDistributedTaskRequest request = createValidRequest();
    AtomicReference<ExecuteDistributedTaskResponse> responseRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              ExecuteDistributedTaskResponse response = invocation.getArgument(0);
              responseRef.set(response);
              return null;
            })
        .when(actionListener)
        .onResponse(any());

    // When
    action.doExecute(task, request, actionListener);

    // Then
    ExecuteDistributedTaskResponse response = responseRef.get();
    assertNotNull(response);
    assertNotNull(response.getExecutionStats());
    Map<String, Object> stats = response.getExecutionStats();
    assertTrue(stats.containsKey("workUnitsExecuted"));
    assertTrue(stats.containsKey("executionTimeMs"));
  }

  @Test
  void action_name_should_be_defined() {
    // Then
    assertEquals(
        "cluster:admin/opensearch/sql/distributed/execute",
        TransportExecuteDistributedTaskAction.NAME);
  }

  @Test
  void should_process_multiple_work_units() {
    // Given
    ExecuteDistributedTaskRequest request = createMultiWorkUnitRequest();
    AtomicReference<ExecuteDistributedTaskResponse> responseRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              ExecuteDistributedTaskResponse response = invocation.getArgument(0);
              responseRef.set(response);
              return null;
            })
        .when(actionListener)
        .onResponse(any());

    // When
    action.doExecute(task, request, actionListener);

    // Then
    ExecuteDistributedTaskResponse response = responseRef.get();
    assertNotNull(response);
    assertTrue(response.isSuccessful());

    // Should process all work units
    Map<String, Object> stats = response.getExecutionStats();
    assertEquals(3, stats.get("workUnitsExecuted"));
  }

  @Test
  void should_include_node_id_in_response() {
    // Given
    ExecuteDistributedTaskRequest request = createValidRequest();
    AtomicReference<ExecuteDistributedTaskResponse> responseRef = new AtomicReference<>();

    doAnswer(
            invocation -> {
              ExecuteDistributedTaskResponse response = invocation.getArgument(0);
              responseRef.set(response);
              return null;
            })
        .when(actionListener)
        .onResponse(any());

    // When
    action.doExecute(task, request, actionListener);

    // Then
    ExecuteDistributedTaskResponse response = responseRef.get();
    assertNotNull(response);
    assertEquals("test-node-1", response.getNodeId());
  }

  private ExecuteDistributedTaskRequest createValidRequest() {
    DataPartition partition =
        new DataPartition(
            "shard-1", DataPartition.StorageType.LUCENE, "test-index", 1024L, Map.of());
    WorkUnit workUnit =
        new WorkUnit(
            "work-unit-1",
            WorkUnit.WorkUnitType.SCAN,
            partition,
            null, // Placeholder operator
            List.of(),
            "test-node-1",
            Map.of());

    return new ExecuteDistributedTaskRequest(List.of(workUnit), "test-stage", null);
  }

  private ExecuteDistributedTaskRequest createEmptyRequest() {
    return new ExecuteDistributedTaskRequest(List.of(), "empty-stage", null);
  }

  private ExecuteDistributedTaskRequest createInvalidRequest() {
    // Create request with null stage ID to test error handling
    return new ExecuteDistributedTaskRequest(List.of(), null, null);
  }

  private ExecuteDistributedTaskRequest createMultiWorkUnitRequest() {
    DataPartition partition1 =
        new DataPartition(
            "shard-1", DataPartition.StorageType.LUCENE, "test-index", 1024L, Map.of());
    DataPartition partition2 =
        new DataPartition(
            "shard-2", DataPartition.StorageType.LUCENE, "test-index", 1024L, Map.of());
    DataPartition partition3 =
        new DataPartition(
            "shard-3", DataPartition.StorageType.LUCENE, "test-index", 1024L, Map.of());

    WorkUnit workUnit1 =
        new WorkUnit(
            "work-unit-1",
            WorkUnit.WorkUnitType.SCAN,
            partition1,
            null,
            List.of(),
            "test-node-1",
            Map.of());
    WorkUnit workUnit2 =
        new WorkUnit(
            "work-unit-2",
            WorkUnit.WorkUnitType.SCAN,
            partition2,
            null,
            List.of(),
            "test-node-1",
            Map.of());
    WorkUnit workUnit3 =
        new WorkUnit(
            "work-unit-3",
            WorkUnit.WorkUnitType.SCAN,
            partition3,
            null,
            List.of(),
            "test-node-1",
            Map.of());

    return new ExecuteDistributedTaskRequest(
        List.of(workUnit1, workUnit2, workUnit3), "multi-work-stage", null);
  }
}
