/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.opensearch.executor.distributed.dataunit.OpenSearchDataUnit;
import org.opensearch.sql.opensearch.executor.distributed.planner.RelNodeAnalyzer;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;
import org.opensearch.sql.planner.distributed.stage.PartitioningScheme;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DistributedQueryCoordinatorTest {

  @Mock private ClusterService clusterService;
  @Mock private TransportService transportService;
  @Mock private ClusterState clusterState;
  @Mock private DiscoveryNodes discoveryNodes;

  private DistributedQueryCoordinator coordinator;
  private RelDataTypeFactory typeFactory;
  private RelOptCluster cluster;
  private RelTraitSet traitSet;

  @BeforeEach
  void setUp() {
    coordinator = new DistributedQueryCoordinator(clusterService, transportService);
    typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    VolcanoPlanner planner = new VolcanoPlanner();
    cluster = RelOptCluster.create(planner, rexBuilder);
    traitSet = cluster.traitSet();

    when(clusterService.state()).thenReturn(clusterState);
    when(clusterState.nodes()).thenReturn(discoveryNodes);
  }

  @Test
  void should_send_transport_requests_to_assigned_nodes() {
    // Setup two data nodes
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    DiscoveryNode node2 = mock(DiscoveryNode.class);
    when(node1.getId()).thenReturn("node-1");
    when(node2.getId()).thenReturn("node-2");

    @SuppressWarnings("unchecked")
    Map<String, DiscoveryNode> dataNodes = Map.of("node-1", node1, "node-2", node2);
    when(discoveryNodes.getDataNodes()).thenReturn(dataNodes);
    when(discoveryNodes.get("node-1")).thenReturn(node1);
    when(discoveryNodes.get("node-2")).thenReturn(node2);

    // Build staged plan with two shards on different nodes
    RelDataType rowType =
        typeFactory
            .builder()
            .add("name", SqlTypeName.VARCHAR, 256)
            .add("age", SqlTypeName.INTEGER)
            .build();
    RelNode relNode = createMockScan("accounts", rowType);

    ComputeStage leafStage =
        new ComputeStage(
            "0",
            PartitioningScheme.gather(),
            List.of(),
            List.of(
                new OpenSearchDataUnit("accounts", 0, List.of("node-1"), -1, -1),
                new OpenSearchDataUnit("accounts", 1, List.of("node-2"), -1, -1)),
            -1,
            -1,
            relNode);

    ComputeStage rootStage =
        new ComputeStage("1", PartitioningScheme.none(), List.of("0"), List.of(), -1, -1);
    StagedPlan plan = new StagedPlan("test-plan", List.of(leafStage, rootStage));

    RelNodeAnalyzer.AnalysisResult analysis =
        new RelNodeAnalyzer.AnalysisResult("accounts", List.of("name", "age"), -1, null);

    // Mock transport to respond with success (capture the handler and invoke it)
    doAnswer(
            invocation -> {
              org.opensearch.transport.TransportResponseHandler<ExecuteDistributedTaskResponse>
                  handler = invocation.getArgument(3);
              ExecuteDistributedTaskResponse response =
                  ExecuteDistributedTaskResponse.successWithRows(
                      invocation.getArgument(0, DiscoveryNode.class).getId(),
                      List.of("name", "age"),
                      List.of(List.of("Alice", 30)));
              handler.handleResponse(response);
              return null;
            })
        .when(transportService)
        .sendRequest(
            any(DiscoveryNode.class),
            eq(ExecuteDistributedTaskAction.NAME),
            any(ExecuteDistributedTaskRequest.class),
            any());

    @SuppressWarnings("unchecked")
    ResponseListener<QueryResponse> listener = mock(ResponseListener.class);
    AtomicReference<QueryResponse> capturedResponse = new AtomicReference<>();
    doAnswer(
            invocation -> {
              capturedResponse.set(invocation.getArgument(0));
              return null;
            })
        .when(listener)
        .onResponse(any());

    coordinator.execute(plan, analysis, relNode, listener);

    // Verify transport was called for each node
    verify(transportService, times(2))
        .sendRequest(
            any(DiscoveryNode.class),
            eq(ExecuteDistributedTaskAction.NAME),
            any(ExecuteDistributedTaskRequest.class),
            any());

    // Verify response was received
    verify(listener, times(1)).onResponse(any());
    assertNotNull(capturedResponse.get());
    assertEquals(2, capturedResponse.get().getSchema().getColumns().size());
    assertEquals("name", capturedResponse.get().getSchema().getColumns().get(0).getName());
    assertEquals(2, capturedResponse.get().getResults().size());
  }

  @Test
  void should_report_failure_when_node_not_found() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    when(node1.getId()).thenReturn("node-1");

    @SuppressWarnings("unchecked")
    Map<String, DiscoveryNode> dataNodes = Map.of("node-1", node1);
    when(discoveryNodes.getDataNodes()).thenReturn(dataNodes);
    when(discoveryNodes.get("node-1")).thenReturn(null); // node not found

    RelDataType rowType = typeFactory.builder().add("name", SqlTypeName.VARCHAR, 256).build();
    RelNode relNode = createMockScan("idx", rowType);

    ComputeStage leafStage =
        new ComputeStage(
            "0",
            PartitioningScheme.gather(),
            List.of(),
            List.of(new OpenSearchDataUnit("idx", 0, List.of("node-1"), -1, -1)),
            -1,
            -1,
            relNode);
    ComputeStage rootStage =
        new ComputeStage("1", PartitioningScheme.none(), List.of("0"), List.of(), -1, -1);
    StagedPlan plan = new StagedPlan("test-plan", List.of(leafStage, rootStage));

    RelNodeAnalyzer.AnalysisResult analysis =
        new RelNodeAnalyzer.AnalysisResult("idx", List.of("name"), -1, null);

    @SuppressWarnings("unchecked")
    ResponseListener<QueryResponse> listener = mock(ResponseListener.class);

    coordinator.execute(plan, analysis, relNode, listener);

    verify(listener, times(1)).onFailure(any(Exception.class));
  }

  @Test
  void should_apply_coordinator_side_limit() {
    DiscoveryNode node1 = mock(DiscoveryNode.class);
    when(node1.getId()).thenReturn("node-1");
    @SuppressWarnings("unchecked")
    Map<String, DiscoveryNode> dataNodes = Map.of("node-1", node1);
    when(discoveryNodes.getDataNodes()).thenReturn(dataNodes);
    when(discoveryNodes.get("node-1")).thenReturn(node1);

    RelDataType rowType = typeFactory.builder().add("name", SqlTypeName.VARCHAR, 256).build();
    RelNode relNode = createMockScan("idx", rowType);

    ComputeStage leafStage =
        new ComputeStage(
            "0",
            PartitioningScheme.gather(),
            List.of(),
            List.of(new OpenSearchDataUnit("idx", 0, List.of("node-1"), -1, -1)),
            -1,
            -1,
            relNode);
    ComputeStage rootStage =
        new ComputeStage("1", PartitioningScheme.none(), List.of("0"), List.of(), -1, -1);
    StagedPlan plan = new StagedPlan("test-plan", List.of(leafStage, rootStage));

    // Analysis with limit=2, but node returns 5 rows
    RelNodeAnalyzer.AnalysisResult analysis =
        new RelNodeAnalyzer.AnalysisResult("idx", List.of("name"), 2, null);

    doAnswer(
            invocation -> {
              org.opensearch.transport.TransportResponseHandler<ExecuteDistributedTaskResponse>
                  handler = invocation.getArgument(3);
              ExecuteDistributedTaskResponse response =
                  ExecuteDistributedTaskResponse.successWithRows(
                      "node-1",
                      List.of("name"),
                      List.of(
                          List.of("A"), List.of("B"), List.of("C"), List.of("D"), List.of("E")));
              handler.handleResponse(response);
              return null;
            })
        .when(transportService)
        .sendRequest(any(DiscoveryNode.class), any(), any(), any());

    @SuppressWarnings("unchecked")
    ResponseListener<QueryResponse> listener = mock(ResponseListener.class);
    AtomicReference<QueryResponse> capturedResponse = new AtomicReference<>();
    doAnswer(
            invocation -> {
              capturedResponse.set(invocation.getArgument(0));
              return null;
            })
        .when(listener)
        .onResponse(any());

    coordinator.execute(plan, analysis, relNode, listener);

    verify(listener, times(1)).onResponse(any());
    // Coordinator-side limit should truncate to 2
    assertEquals(2, capturedResponse.get().getResults().size());
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
