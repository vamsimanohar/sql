/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.planner.distributed.CalciteDistributedPhysicalPlanner;
import org.opensearch.sql.planner.distributed.DistributedPhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Distributed execution engine that routes queries between legacy single-node execution
 * and distributed multi-node execution based on configuration and query characteristics.
 *
 * This engine serves as the entry point for distributed PPL query processing, with
 * fallback to the legacy OpenSearchExecutionEngine for compatibility.
 */
@RequiredArgsConstructor
public class DistributedExecutionEngine implements ExecutionEngine {
  private static final Logger logger = LogManager.getLogger(DistributedExecutionEngine.class);

  private final OpenSearchExecutionEngine legacyEngine;
  private final OpenSearchSettings settings;
  private final CalciteDistributedPhysicalPlanner calciteDistributedPlanner;

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    execute(plan, ExecutionContext.emptyExecutionContext(), listener);
  }

  @Override
  public void execute(
      PhysicalPlan plan,
      ExecutionContext context,
      ResponseListener<QueryResponse> listener) {

    if (shouldUseDistributedExecution(plan, context)) {
      logger.info("Using distributed execution for query plan: {}", plan.getClass().getSimpleName());
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
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<QueryResponse> listener) {

    if (shouldUseDistributedExecution(plan, context)) {
      logger.info("Using distributed execution for Calcite RelNode: {}", plan.getClass().getSimpleName());
      executeDistributedCalcite(plan, context, listener);
    } else {
      logger.debug("Using legacy execution for Calcite RelNode: {}", plan.getClass().getSimpleName());
      legacyEngine.execute(plan, context, listener);
    }
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    // For Calcite-based explain, delegate to legacy engine
    legacyEngine.explain(plan, mode, context, listener);
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

    // For Phase 1: Enable distributed execution for Calcite RelNodes to test the planner
    // TODO: In future phases, add more sophisticated analysis:
    // - Check RelNode types (TableScan + Filter + Aggregate = good candidate)
    // - Analyze query complexity and estimated data volume
    // - Determine if distributed execution would provide benefits

    logger.debug("Calcite distributed execution enabled for testing - plan: {}", plan.getClass().getSimpleName());
    return true; // Enable for Phase 1 testing
  }

  /**
   * Executes the query using distributed processing (PhysicalPlan).
   *
   * @param plan The physical plan to execute
   * @param context The execution context
   * @param listener Response listener for async execution
   */
  private void executeDistributed(
      PhysicalPlan plan,
      ExecutionContext context,
      ResponseListener<QueryResponse> listener) {

    try {
      // TODO: Phase 1 Implementation for PhysicalPlan
      // 1. Convert PhysicalPlan to DistributedPhysicalPlan
      // 2. Break into ExecutionStages with WorkUnits
      // 3. Schedule WorkUnits across cluster nodes
      // 4. Coordinate stage-by-stage execution
      // 5. Collect and merge results

      // For now, fallback to legacy engine with warning
      logger.warn("Distributed PhysicalPlan execution not yet implemented, falling back to legacy engine");
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
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<QueryResponse> listener) {

    try {
      // Phase 1: Convert RelNode to DistributedPhysicalPlan
      DistributedPhysicalPlan distributedPlan = calciteDistributedPlanner.plan(plan, context);

      // TODO: Phase 1 Implementation
      // 1. Schedule WorkUnits across cluster nodes via DistributedTaskScheduler
      // 2. Coordinate stage-by-stage execution
      // 3. Collect and merge results from distributed tasks
      // 4. Return results via ResponseListener

      // For now, log the successful planning but fallback to legacy engine
      logger.warn("Calcite distributed plan created successfully: {}, but execution not yet implemented. Falling back to legacy engine", distributedPlan);
      legacyEngine.execute(plan, context, listener);

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