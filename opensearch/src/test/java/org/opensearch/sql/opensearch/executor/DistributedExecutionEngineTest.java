/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.calcite.rel.RelNode;
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
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.transport.TransportService;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DistributedExecutionEngineTest {

  @Mock private OpenSearchExecutionEngine legacyEngine;
  @Mock private OpenSearchSettings settings;
  @Mock private ClusterService clusterService;
  @Mock private TransportService transportService;
  @Mock private PhysicalPlan physicalPlan;
  @Mock private RelNode relNode;
  @Mock private CalcitePlanContext calciteContext;
  @Mock private ResponseListener<QueryResponse> responseListener;
  @Mock private ExecutionContext executionContext;

  private DistributedExecutionEngine distributedEngine;

  @BeforeEach
  void setUp() {
    distributedEngine =
        new DistributedExecutionEngine(legacyEngine, settings, clusterService, transportService);
  }

  @Test
  void should_use_legacy_engine_when_distributed_execution_disabled() {
    when(settings.getDistributedExecutionEnabled()).thenReturn(false);

    distributedEngine.execute(physicalPlan, executionContext, responseListener);

    verify(legacyEngine, times(1)).execute(physicalPlan, executionContext, responseListener);
  }

  @Test
  void should_throw_when_distributed_enabled_for_physical_plan() {
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);

    assertThrows(
        UnsupportedOperationException.class,
        () -> distributedEngine.execute(physicalPlan, executionContext, responseListener));
  }

  @Test
  void should_report_failure_when_distributed_enabled_with_invalid_relnode() {
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);
    // Mock RelNode with no inputs and not an AbstractCalciteIndexScan — will fail analysis
    when(relNode.getInputs()).thenReturn(java.util.List.of());

    distributedEngine.execute(relNode, calciteContext, responseListener);

    // Should call onFailure since the mock RelNode can't be analyzed
    verify(responseListener, times(1)).onFailure(any(Exception.class));
  }

  @Test
  void should_use_legacy_engine_for_calcite_relnode_when_disabled() {
    when(settings.getDistributedExecutionEnabled()).thenReturn(false);

    distributedEngine.execute(relNode, calciteContext, responseListener);

    verify(legacyEngine, times(1)).execute(relNode, calciteContext, responseListener);
  }

  @Test
  void should_delegate_explain_to_legacy_engine() {
    @SuppressWarnings("unchecked")
    ResponseListener<ExecutionEngine.ExplainResponse> explainListener =
        mock(ResponseListener.class);

    distributedEngine.explain(physicalPlan, explainListener);

    verify(legacyEngine, times(1)).explain(physicalPlan, explainListener);
  }

  @Test
  void should_delegate_calcite_explain_to_legacy_when_disabled() {
    @SuppressWarnings("unchecked")
    ResponseListener<ExecutionEngine.ExplainResponse> explainListener =
        mock(ResponseListener.class);
    ExplainMode mode = ExplainMode.STANDARD;
    when(settings.getDistributedExecutionEnabled()).thenReturn(false);

    distributedEngine.explain(relNode, mode, calciteContext, explainListener);

    verify(legacyEngine, times(1)).explain(relNode, mode, calciteContext, explainListener);
  }

  @Test
  void should_report_failure_for_calcite_explain_when_distributed_enabled_with_invalid_relnode() {
    @SuppressWarnings("unchecked")
    ResponseListener<ExecutionEngine.ExplainResponse> explainListener =
        mock(ResponseListener.class);
    ExplainMode mode = ExplainMode.STANDARD;
    when(settings.getDistributedExecutionEnabled()).thenReturn(true);
    when(relNode.getInputs()).thenReturn(java.util.List.of());

    distributedEngine.explain(relNode, mode, calciteContext, explainListener);

    // Should call onFailure since the mock RelNode can't be analyzed
    verify(explainListener, times(1)).onFailure(any(Exception.class));
  }

  @Test
  void constructor_should_initialize() {
    DistributedExecutionEngine engine =
        new DistributedExecutionEngine(legacyEngine, settings, clusterService, transportService);
    assertNotNull(engine);
  }
}
