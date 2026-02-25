/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Distributed execution engine that routes queries between legacy single-node execution and
 * distributed multi-node execution based on configuration.
 *
 * <p>When distributed execution is disabled (default), all queries delegate to the legacy {@link
 * OpenSearchExecutionEngine}. When enabled, queries throw {@link UnsupportedOperationException} —
 * distributed execution will be implemented in the next phase against the clean H2 interfaces
 * (ComputeStage, DataUnit, PlanFragmenter, etc.).
 */
public class DistributedExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(DistributedExecutionEngine.class);

  private final OpenSearchExecutionEngine legacyEngine;
  private final OpenSearchSettings settings;

  public DistributedExecutionEngine(
      OpenSearchExecutionEngine legacyEngine, OpenSearchSettings settings) {
    this.legacyEngine = legacyEngine;
    this.settings = settings;
    logger.info("Initialized DistributedExecutionEngine");
  }

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    execute(plan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    if (isDistributedEnabled()) {
      throw new UnsupportedOperationException("Distributed execution not yet implemented");
    }
    legacyEngine.execute(plan, context, listener);
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    legacyEngine.explain(plan, listener);
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    if (isDistributedEnabled()) {
      throw new UnsupportedOperationException("Distributed execution not yet implemented");
    }
    legacyEngine.execute(plan, context, listener);
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    if (isDistributedEnabled()) {
      throw new UnsupportedOperationException("Distributed execution not yet implemented");
    }
    legacyEngine.explain(plan, mode, context, listener);
  }

  private boolean isDistributedEnabled() {
    return settings.getDistributedExecutionEnabled();
  }
}
