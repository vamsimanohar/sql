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
import org.opensearch.sql.opensearch.executor.distributed.planner.CalciteDistributedPhysicalPlanner;
import org.opensearch.sql.opensearch.executor.distributed.planner.OpenSearchCostEstimator;
import org.opensearch.sql.opensearch.executor.distributed.planner.OpenSearchFragmentationContext;
import org.opensearch.sql.opensearch.executor.distributed.planner.RelNodeAnalyzer;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;
import org.opensearch.sql.planner.distributed.planner.PhysicalPlanner;
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
      logger.info("Using distributed physical planner for execution");

      // Step 1: Create physical planner with enhanced cost estimator
      FragmentationContext fragContext = createEnhancedFragmentationContext();
      PhysicalPlanner planner = new CalciteDistributedPhysicalPlanner(fragContext);

      // Step 2: Generate staged plan using intelligent fragmentation
      StagedPlan stagedPlan = planner.plan(relNode);

      logger.info("Generated {} stages for distributed query", stagedPlan.getStageCount());

      // Step 3: Execute via coordinator
      DistributedQueryCoordinator coordinator =
          new DistributedQueryCoordinator(clusterService, transportService);

      // For Phase 1B, we still need the legacy analysis for compatibility
      // Future phases will eliminate this dependency
      RelNodeAnalyzer.AnalysisResult analysis = RelNodeAnalyzer.analyze(relNode);
      coordinator.execute(stagedPlan, analysis, relNode, listener);

    } catch (Exception e) {
      logger.error("Failed to execute distributed query", e);
      listener.onFailure(e);
    }
  }

  private void explainDistributed(RelNode relNode, ResponseListener<ExplainResponse> listener) {
    try {
      // Generate staged plan using distributed physical planner
      FragmentationContext fragContext = createEnhancedFragmentationContext();
      PhysicalPlanner planner = new CalciteDistributedPhysicalPlanner(fragContext);
      StagedPlan stagedPlan = planner.plan(relNode);

      // Build enhanced explain output
      StringBuilder sb = new StringBuilder();
      sb.append("Distributed Execution Plan\n");
      sb.append("==========================\n");
      sb.append("Plan ID: ").append(stagedPlan.getPlanId()).append("\n");
      sb.append("Mode: Distributed Physical Planning\n");
      sb.append("Stages: ").append(stagedPlan.getStageCount()).append("\n\n");

      for (ComputeStage stage : stagedPlan.getStages()) {
        sb.append("[")
            .append(stage.getStageId())
            .append("] ")
            .append(stage.getOutputPartitioning().getExchangeType())
            .append(" Exchange (parallelism: ")
            .append(stage.getDataUnits().size())
            .append(")\n");

        if (stage.isLeaf()) {
          sb.append("├─ LuceneScanOperator (shard-based data access)\n");
          if (stage.getEstimatedRows() > 0) {
            sb.append("├─ Estimated rows: ").append(stage.getEstimatedRows()).append("\n");
          }
          if (stage.getEstimatedBytes() > 0) {
            sb.append("├─ Estimated bytes: ").append(stage.getEstimatedBytes()).append("\n");
          }
          sb.append("└─ Data units: ").append(stage.getDataUnits().size()).append(" shards\n");
        } else {
          sb.append("└─ CoordinatorMergeOperator (results aggregation)\n");
        }

        if (!stage.getSourceStageIds().isEmpty()) {
          sb.append("   Dependencies: ").append(stage.getSourceStageIds()).append("\n");
        }
        sb.append("\n");
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

  /** Creates an enhanced fragmentation context with real cost estimation. */
  private FragmentationContext createEnhancedFragmentationContext() {
    // Create enhanced cost estimator instead of stub
    OpenSearchCostEstimator costEstimator = new OpenSearchCostEstimator(clusterService);

    // Create fragmentation context with enhanced cost estimator
    return new OpenSearchFragmentationContext(clusterService, costEstimator);
  }
}
