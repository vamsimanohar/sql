/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.executor.distributed.DistributedQueryCoordinator;
import org.opensearch.sql.opensearch.executor.distributed.planner.OpenSearchFragmentationContext;
import org.opensearch.sql.opensearch.executor.distributed.planner.RelNodeAnalyzer;
import org.opensearch.sql.opensearch.executor.distributed.planner.SimplePlanFragmenter;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.transport.TransportService;

/**
 * Distributed execution engine that routes queries between legacy single-node execution and
 * distributed multi-node execution based on configuration.
 *
 * <p>When distributed execution is disabled (default), all queries delegate to the legacy {@link
 * OpenSearchExecutionEngine}. When enabled, queries are fragmented into a staged plan and executed
 * across data nodes via transport actions.
 */
public class DistributedExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(DistributedExecutionEngine.class);

  private final OpenSearchExecutionEngine legacyEngine;
  private final OpenSearchSettings settings;
  private final ClusterService clusterService;
  private final TransportService transportService;

  public DistributedExecutionEngine(
      OpenSearchExecutionEngine legacyEngine,
      OpenSearchSettings settings,
      ClusterService clusterService,
      TransportService transportService) {
    this.legacyEngine = legacyEngine;
    this.settings = settings;
    this.clusterService = clusterService;
    this.transportService = transportService;
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
      throw new UnsupportedOperationException(
          "Distributed execution via PhysicalPlan not supported. Use RelNode path.");
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
      executeDistributed(plan, listener);
      return;
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
      explainDistributed(plan, listener);
      return;
    }
    legacyEngine.explain(plan, mode, context, listener);
  }

  private void executeDistributed(RelNode relNode, ResponseListener<QueryResponse> listener) {
    try {
      RelNodeAnalyzer.AnalysisResult analysis = RelNodeAnalyzer.analyze(relNode);
      FragmentationContext fragContext = new OpenSearchFragmentationContext(clusterService);
      StagedPlan stagedPlan = new SimplePlanFragmenter().fragment(relNode, fragContext);

      logger.info(
          "Distributed execute: index={}, stages={}, shards={}",
          analysis.getIndexName(),
          stagedPlan.getStageCount(),
          stagedPlan.getLeafStages().get(0).getDataUnits().size());

      DistributedQueryCoordinator coordinator =
          new DistributedQueryCoordinator(clusterService, transportService);
      coordinator.execute(stagedPlan, analysis, relNode, listener);

    } catch (Exception e) {
      logger.error("Failed to execute distributed query", e);
      listener.onFailure(e);
    }
  }

  private void explainDistributed(RelNode relNode, ResponseListener<ExplainResponse> listener) {
    try {
      RelNodeAnalyzer.AnalysisResult analysis = RelNodeAnalyzer.analyze(relNode);
      FragmentationContext fragContext = new OpenSearchFragmentationContext(clusterService);
      StagedPlan stagedPlan = new SimplePlanFragmenter().fragment(relNode, fragContext);

      // Build explain output
      StringBuilder sb = new StringBuilder();
      sb.append("Distributed Execution Plan\n");
      sb.append("==========================\n");
      sb.append("Plan ID: ").append(stagedPlan.getPlanId()).append("\n");
      sb.append("Mode: Distributed\n");
      sb.append("Stages: ").append(stagedPlan.getStageCount()).append("\n\n");

      for (ComputeStage stage : stagedPlan.getStages()) {
        sb.append("[Stage ").append(stage.getStageId()).append("]\n");
        sb.append("  Type: ").append(stage.isLeaf() ? "LEAF (scan)" : "ROOT (merge)").append("\n");
        sb.append("  Exchange: ")
            .append(stage.getOutputPartitioning().getExchangeType())
            .append("\n");
        sb.append("  DataUnits: ").append(stage.getDataUnits().size()).append("\n");
        if (!stage.getSourceStageIds().isEmpty()) {
          sb.append("  Dependencies: ").append(stage.getSourceStageIds()).append("\n");
        }
        if (stage.getPlanFragment() != null) {
          sb.append("  Plan: ").append(RelOptUtil.toString(stage.getPlanFragment())).append("\n");
        }
      }

      String logicalPlan = RelOptUtil.toString(relNode);
      String physicalPlan = sb.toString();

      ExplainResponseNodeV2 calciteNode =
          new ExplainResponseNodeV2(logicalPlan, physicalPlan, null);
      ExplainResponse explainResponse = new ExplainResponse(calciteNode);
      listener.onResponse(explainResponse);

    } catch (Exception e) {
      logger.error("Failed to explain distributed query", e);
      listener.onFailure(e);
    }
  }

  private boolean isDistributedEnabled() {
    return settings.getDistributedExecutionEnabled();
  }
}
