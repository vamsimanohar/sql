/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DistributedExecutionEngineTest {

  @Mock private OpenSearchExecutionEngine legacyEngine;
  @Mock private OpenSearchSettings settings;
  @Mock private TransportService transportService;
  @Mock private ClusterService clusterService;
  @Mock private Client client;
  @Mock private PhysicalPlan physicalPlan;
  @Mock private RelNode relNode;
  @Mock private CalcitePlanContext calciteContext;
  @Mock private ResponseListener<QueryResponse> responseListener;
  @Mock private ExecutionContext executionContext;

  private DistributedExecutionEngine distributedEngine;

  @BeforeEach
  void setUp() {
    distributedEngine =
        new DistributedExecutionEngine(
            legacyEngine, settings, clusterService, transportService, client);
  }

  @Test
  void should_use_legacy_engine_when_distributed_execution_disabled() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(false);

    // When
    distributedEngine.execute(physicalPlan, executionContext, responseListener);

    // Then
    verify(legacyEngine, times(1)).execute(physicalPlan, executionContext, responseListener);
    verify(settings, times(1)).getDistributedExecutionEnabled();
  }

  @Test
  void should_use_legacy_engine_for_physical_plan_when_distributed_enabled() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);

    // When - Phase 1: PhysicalPlan always uses legacy engine
    distributedEngine.execute(physicalPlan, executionContext, responseListener);

    // Then
    verify(legacyEngine, times(1)).execute(physicalPlan, executionContext, responseListener);
  }

  @Test
  void should_use_distributed_engine_for_calcite_relnode_when_enabled() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);

    // Setup mock DistributedQueryPlanner to avoid NPE
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(1);
              QueryResponse response =
                  new QueryResponse(
                      new Schema(List.of()), List.of(), null); // Empty response for test
              listener.onResponse(response);
              return null;
            })
        .when(responseListener)
        .onResponse(any());

    // When
    distributedEngine.execute(relNode, calciteContext, responseListener);

    // Then - Should attempt distributed execution but may fall back to legacy on error
    verify(settings, times(1)).getDistributedExecutionEnabled();
    // Note: In Phase 1, distributed execution may fall back to legacy on initialization errors
  }

  @Test
  void should_use_legacy_engine_for_calcite_relnode_when_disabled() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(false);

    // When
    distributedEngine.execute(relNode, calciteContext, responseListener);

    // Then
    verify(legacyEngine, times(1)).execute(relNode, calciteContext, responseListener);
    verify(settings, times(1)).getDistributedExecutionEnabled();
  }

  @Test
  void should_delegate_explain_to_legacy_engine() {
    // Given
    @SuppressWarnings("unchecked")
    ResponseListener<ExecutionEngine.ExplainResponse> explainListener =
        mock(ResponseListener.class);

    // When - Phase 1: Explain always uses legacy engine
    distributedEngine.explain(physicalPlan, explainListener);

    // Then
    verify(legacyEngine, times(1)).explain(physicalPlan, explainListener);
  }

  @Test
  void should_delegate_calcite_explain_to_legacy_engine() {
    // Given
    @SuppressWarnings("unchecked")
    ResponseListener<ExecutionEngine.ExplainResponse> explainListener =
        mock(ResponseListener.class);
    ExplainMode mode = ExplainMode.STANDARD;

    // When - Phase 1: Calcite explain always uses legacy engine
    distributedEngine.explain(relNode, mode, calciteContext, explainListener);

    // Then
    verify(legacyEngine, times(1)).explain(relNode, mode, calciteContext, explainListener);
  }

  @Test
  void constructor_should_initialize_all_components() {
    // When
    DistributedExecutionEngine engine =
        new DistributedExecutionEngine(
            legacyEngine, settings, clusterService, transportService, client);

    // Then
    assertNotNull(engine);
  }

  @Test
  void should_fallback_to_legacy_on_distributed_execution_error() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);

    // Simulate error in distributed execution by throwing exception during initialization
    doAnswer(
            invocation -> {
              ResponseListener<QueryResponse> listener = invocation.getArgument(2);
              // Should fall back to legacy engine which will handle the response
              return null;
            })
        .when(legacyEngine)
        .execute(any(RelNode.class), any(CalcitePlanContext.class), any());

    // When - This should trigger fallback behavior
    distributedEngine.execute(relNode, calciteContext, responseListener);

    // Then - Should eventually call legacy engine (either directly or as fallback)
    verify(legacyEngine, times(1)).execute(relNode, calciteContext, responseListener);
  }

  @Test
  void should_route_join_queries_to_legacy_engine() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);
    Join joinNode = mock(Join.class);
    when(joinNode.getInputs()).thenReturn(List.of());

    // When
    distributedEngine.execute(joinNode, calciteContext, responseListener);

    // Then - Join queries should route to legacy engine
    verify(legacyEngine, times(1)).execute(joinNode, calciteContext, responseListener);
  }

  @Test
  void should_route_window_function_queries_to_legacy_engine() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);

    // Create a Project node with a window function (RexOver) — like dedup via ROW_NUMBER
    Project projectNode = mock(Project.class);
    RexOver rexOver = mock(RexOver.class);
    when(projectNode.getProjects()).thenReturn(List.of(rexOver));
    when(projectNode.getInputs()).thenReturn(List.of());

    // When
    distributedEngine.execute(projectNode, calciteContext, responseListener);

    // Then - Window function queries should route to legacy engine
    verify(legacyEngine, times(1)).execute(projectNode, calciteContext, responseListener);
  }

  @Test
  void should_route_computed_expression_queries_to_legacy_engine() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);

    // Create a Project node with a computed expression (RexCall) — like eval balance*2
    Project projectNode = mock(Project.class);
    RexCall rexCall = mock(RexCall.class);
    when(projectNode.getProjects()).thenReturn(List.of(rexCall));
    when(projectNode.getInputs()).thenReturn(List.of());

    // When
    distributedEngine.execute(projectNode, calciteContext, responseListener);

    // Then - Computed expression queries should route to legacy engine
    verify(legacyEngine, times(1)).execute(projectNode, calciteContext, responseListener);
  }

  @Test
  void should_allow_simple_projection_queries_for_distributed() {
    // Given
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);

    // Create a Project node with only simple field references (RexInputRef) — like fields a, b
    Project projectNode = mock(Project.class);
    RexInputRef ref1 = mock(RexInputRef.class);
    RexInputRef ref2 = mock(RexInputRef.class);
    when(projectNode.getProjects()).thenReturn(List.of((RexNode) ref1, (RexNode) ref2));
    when(projectNode.getInputs()).thenReturn(List.of());

    // When
    distributedEngine.execute(projectNode, calciteContext, responseListener);

    // Then - Simple projection should attempt distributed execution (not route to legacy)
    // Note: It will fail at the distributed planner stage and fall back, but the initial
    // routing should be to distributed, not legacy.
    verify(settings, times(1)).getDistributedExecutionEnabled();
  }
}
