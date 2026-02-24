/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.executor.distributed.DistributedTaskScheduler;
import org.opensearch.sql.opensearch.executor.distributed.OpenSearchPartitionDiscovery;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.planner.distributed.DistributedPhysicalPlan;
import org.opensearch.sql.planner.distributed.DistributedQueryPlanner;
import org.opensearch.sql.planner.distributed.ExecutionStage;
import org.opensearch.sql.planner.distributed.WorkUnit;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Distributed execution engine that routes queries between legacy single-node execution and
 * distributed multi-node execution based on configuration and query characteristics.
 *
 * <p>This engine serves as the entry point for distributed PPL query processing, with fallback to
 * the legacy OpenSearchExecutionEngine for compatibility.
 */
public class DistributedExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(DistributedExecutionEngine.class);

  private final OpenSearchExecutionEngine legacyEngine;
  private final OpenSearchSettings settings;
  private final DistributedQueryPlanner distributedQueryPlanner;
  private final DistributedTaskScheduler distributedTaskScheduler;

  public DistributedExecutionEngine(
      OpenSearchExecutionEngine legacyEngine,
      OpenSearchSettings settings,
      ClusterService clusterService,
      TransportService transportService,
      Client client) {
    this.legacyEngine = legacyEngine;
    this.settings = settings;
    this.distributedQueryPlanner =
        new DistributedQueryPlanner(new OpenSearchPartitionDiscovery(clusterService));
    this.distributedTaskScheduler =
        new DistributedTaskScheduler(transportService, clusterService, client);
    logger.info("Initialized DistributedExecutionEngine");
  }

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    execute(plan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {

    if (shouldUseDistributedExecution(plan, context)) {
      logger.info(
          "Using distributed execution for query plan: {}", plan.getClass().getSimpleName());
      executeDistributed(plan, context, listener);
    } else {
      logger.debug("Using legacy execution for query plan: {}", plan.getClass().getSimpleName());
      legacyEngine.execute(plan, context, listener);
    }
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    // For now, always use legacy engine for explain
    // TODO: Add distributed explain support in future phases
    legacyEngine.explain(plan, listener);
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {

    if (shouldUseDistributedExecution(plan, context)) {
      logger.info(
          "Using distributed execution for Calcite RelNode: {}", plan.getClass().getSimpleName());
      executeDistributedCalcite(plan, context, listener);
    } else {
      logger.debug(
          "Using legacy execution for Calcite RelNode: {}", plan.getClass().getSimpleName());
      legacyEngine.execute(plan, context, listener);
    }
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    if (isDistributedEnabled()) {
      explainDistributed(plan, mode, context, listener);
    } else {
      legacyEngine.explain(plan, mode, context, listener);
    }
  }

  /**
   * Generates an explain response showing the distributed execution plan. Shows the Calcite logical
   * plan and the distributed stage breakdown (work units, partitions, operators).
   */
  private void explainDistributed(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    try {
      // Calcite logical plan (does not consume the JDBC connection)
      SqlExplainLevel level =
          switch (mode) {
            case COST -> SqlExplainLevel.ALL_ATTRIBUTES;
            case SIMPLE -> SqlExplainLevel.NO_ATTRIBUTES;
            default -> SqlExplainLevel.EXPPLAN_ATTRIBUTES;
          };
      String logical = RelOptUtil.toString(plan, level);

      // Create distributed plan (analyzes RelNode tree + discovers partitions, no execution)
      DistributedPhysicalPlan distributedPlan = distributedQueryPlanner.plan(plan, context);
      String distributed = formatDistributedPlan(distributedPlan);

      listener.onResponse(
          new ExplainResponse(new ExplainResponseNodeV2(logical, distributed, null)));
    } catch (Exception e) {
      logger.error("Error generating distributed explain", e);
      listener.onFailure(e);
    }
  }

  /**
   * Formats a DistributedPhysicalPlan as a human-readable tree for explain output. Uses box-drawing
   * characters similar to Trino's TextRenderer and numbered stages like Spark.
   */
  private String formatDistributedPlan(DistributedPhysicalPlan plan) {
    StringBuilder sb = new StringBuilder();
    List<ExecutionStage> stages = plan.getExecutionStages();

    // Header
    sb.append("== Distributed Execution Plan ==\n");
    sb.append("Plan: ").append(plan.getPlanId()).append("\n");
    sb.append("Mode: Phase 2 (distributed aggregation)\n");
    sb.append("Stages: ").append(stages.size()).append("\n");

    for (int i = 0; i < stages.size(); i++) {
      ExecutionStage stage = stages.get(i);
      boolean isLast = (i == stages.size() - 1);
      List<WorkUnit> workUnits = stage.getWorkUnits();

      // Stage connector
      if (i > 0) {
        sb.append("\u2502\n");
        sb.append("\u25bc\n");
      } else {
        sb.append("\n");
      }

      // Stage header: [1] SCAN  (exchange: NONE, parallelism: 5)
      sb.append("[").append(i + 1).append("] ").append(stage.getStageType());
      if (stage.getStageType() == ExecutionStage.StageType.PROCESS) {
        sb.append(" (partial aggregation)");
      } else if (stage.getStageType() == ExecutionStage.StageType.FINALIZE
          && stages.stream().anyMatch(s -> s.getStageType() == ExecutionStage.StageType.PROCESS)) {
        sb.append(" (merge aggregation via InternalAggregations.reduce)");
      }
      sb.append("  (exchange: ").append(stage.getDataExchange());
      sb.append(", parallelism: ").append(stage.getEstimatedParallelism()).append(")\n");

      // Indent prefix for content under this stage
      String indent = isLast ? "    " : "\u2502   ";

      // Dependencies
      if (stage.getDependencyStages() != null && !stage.getDependencyStages().isEmpty()) {
        sb.append(indent).append("Depends on: ");
        sb.append(String.join(", ", stage.getDependencyStages())).append("\n");
      }

      // Work units as tree
      if (workUnits.isEmpty()) {
        sb.append(indent).append("(no work units - partitions pending)\n");
      } else {
        for (int j = 0; j < workUnits.size(); j++) {
          WorkUnit wu = workUnits.get(j);
          boolean isLastWu = (j == workUnits.size() - 1);
          String branch = isLastWu ? "\u2514\u2500 " : "\u251c\u2500 ";

          sb.append(indent).append(branch);
          formatWorkUnit(sb, wu);
          sb.append("\n");
        }
      }
    }

    return sb.toString();
  }

  /** Formats a single work unit inline: operator [type] -> node (partition details). */
  private void formatWorkUnit(StringBuilder sb, WorkUnit wu) {
    // Operator type from TaskOperator (the semantic meaning)
    if (wu.getTaskOperator() != null) {
      sb.append(wu.getTaskOperator().getOperatorType());
    } else {
      sb.append(wu.getType());
    }

    // Partition info (index/shard)
    if (wu.getDataPartition() != null) {
      String index = wu.getDataPartition().getIndexName();
      String shard = wu.getDataPartition().getShardId();
      if (index != null) {
        sb.append("  [").append(index);
        if (shard != null) {
          sb.append("/").append(shard);
        }
        sb.append("]");
      }
      long sizeBytes = wu.getDataPartition().getEstimatedSizeBytes();
      if (sizeBytes > 0) {
        sb.append("  ~").append(formatBytes(sizeBytes));
      }
    }

    // Target node
    if (wu.getAssignedNodeId() != null) {
      sb.append("  \u2192 ").append(wu.getAssignedNodeId());
    }
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) return bytes + "B";
    if (bytes < 1024 * 1024) return String.format("%.1fKB", bytes / 1024.0);
    if (bytes < 1024 * 1024 * 1024) return String.format("%.1fMB", bytes / (1024.0 * 1024));
    return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
  }

  /**
   * Determines whether to use distributed execution for the given query plan.
   *
   * @param plan The physical plan to analyze
   * @param context The execution context
   * @return true if distributed execution should be used, false otherwise
   */
  private boolean shouldUseDistributedExecution(PhysicalPlan plan, ExecutionContext context) {
    // Check if distributed execution is enabled
    if (!isDistributedEnabled()) {
      logger.debug("Distributed execution disabled via configuration");
      return false;
    }

    // For Phase 1: Always use legacy engine since distributed components aren't implemented yet
    // TODO: In future phases, add query analysis logic here:
    // - Check if plan contains supported operations (aggregations, filters, etc.)
    // - Analyze query complexity and data volume
    // - Determine if distributed execution would benefit the query

    logger.debug("Distributed PhysicalPlan execution not yet implemented, using legacy engine");
    return false;
  }

  /**
   * Determines whether to use distributed execution for the given Calcite RelNode.
   *
   * @param plan The Calcite RelNode to analyze
   * @param context The Calcite plan context
   * @return true if distributed execution should be used, false otherwise
   */
  private boolean shouldUseDistributedExecution(RelNode plan, CalcitePlanContext context) {
    // Check if distributed execution is enabled
    if (!isDistributedEnabled()) {
      logger.debug("Distributed execution disabled via configuration");
      return false;
    }

    // Check for unsupported operations that the SSB-based distributed engine can't handle.
    // The distributed engine extracts a SearchSourceBuilder from the Calcite-optimized scan
    // and sends it to data nodes via transport. Operations NOT pushed into the SSB (joins,
    // window functions, computed expressions) would be silently dropped, producing wrong results.
    String unsupported = findUnsupportedOperation(plan);
    if (unsupported != null) {
      logger.debug(
          "Query contains unsupported operation for distributed execution: {} — routing to legacy"
              + " engine",
          unsupported);
      return false;
    }

    logger.debug(
        "Calcite distributed execution enabled - plan: {}", plan.getClass().getSimpleName());
    return true;
  }

  /**
   * Walks the logical RelNode tree to find operations that the distributed engine cannot handle.
   * Returns a description of the unsupported operation, or null if the plan is supported.
   *
   * <p>The SSB-based distributed engine can only handle operations that OpenSearch's
   * SearchSourceBuilder can express (scan, filter, projection, aggregation, sort, limit).
   * Operations like joins, computed expressions (eval), and window functions (dedup) cannot be
   * pushed into the SSB and would produce incorrect results if silently dropped.
   */
  private String findUnsupportedOperation(RelNode node) {
    // Join requires multi-table coordination the SSB engine can't express
    if (node instanceof Join) {
      return "Join";
    }

    // Aggregate requires partial/final aggregation not yet implemented
    if (node instanceof Aggregate) {
      return "Aggregate (stats/top/rare) — requires distributed aggregation";
    }

    // Check Project nodes for computed expressions (eval) and window functions (dedup)
    if (node instanceof Project project) {
      for (RexNode expr : project.getProjects()) {
        if (expr instanceof RexOver) {
          return "Window function (RexOver) — used by dedup";
        }
        if (expr instanceof RexCall) {
          return "Computed expression (RexCall) — used by eval";
        }
      }
    }

    // Recurse into children
    for (RelNode input : node.getInputs()) {
      String found = findUnsupportedOperation(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Executes the query using distributed processing (PhysicalPlan).
   *
   * @param plan The physical plan to execute
   * @param context The execution context
   * @param listener Response listener for async execution
   */
  private void executeDistributed(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {

    try {
      // TODO: Phase 1 Implementation for PhysicalPlan
      // 1. Convert PhysicalPlan to DistributedPhysicalPlan
      // 2. Break into ExecutionStages with WorkUnits
      // 3. Schedule WorkUnits across cluster nodes
      // 4. Coordinate stage-by-stage execution
      // 5. Collect and merge results

      // For now, fallback to legacy engine with warning
      logger.warn(
          "Distributed PhysicalPlan execution not yet implemented, falling back to legacy engine");
      legacyEngine.execute(plan, context, listener);

    } catch (Exception e) {
      logger.error("Error in distributed PhysicalPlan execution, falling back to legacy engine", e);
      // Always fallback to legacy engine on any error
      legacyEngine.execute(plan, context, listener);
    }
  }

  /**
   * Executes the Calcite RelNode query using distributed processing.
   *
   * @param plan The Calcite RelNode to execute
   * @param context The Calcite plan context
   * @param listener Response listener for async execution
   */
  private void executeDistributedCalcite(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {

    try {
      // Phase 1: Convert RelNode to DistributedPhysicalPlan
      DistributedPhysicalPlan distributedPlan = distributedQueryPlanner.plan(plan, context);
      logger.info("Created distributed plan: {}", distributedPlan);

      // Phase 1: Execute distributed plan using DistributedTaskScheduler
      distributedTaskScheduler.executeQuery(distributedPlan, listener);

    } catch (Exception e) {
      logger.error("Error in distributed Calcite execution, falling back to legacy engine", e);
      // Always fallback to legacy engine on any error
      legacyEngine.execute(plan, context, listener);
    }
  }

  /**
   * Checks if distributed execution is enabled in cluster settings.
   *
   * @return true if distributed execution is enabled, false otherwise
   */
  private boolean isDistributedEnabled() {
    return settings.getDistributedExecutionEnabled();
  }
}
