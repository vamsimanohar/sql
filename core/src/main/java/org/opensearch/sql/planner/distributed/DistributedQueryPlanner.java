/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.CalcitePlanContext;

/**
 * Custom distributed query planner that converts Calcite RelNode trees into multi-stage distributed
 * execution plans.
 *
 * <p>Following the pattern used by MPP engines, this planner operates as a <strong>separate
 * pass</strong> after Calcite's VolcanoPlanner has optimized the logical plan:
 *
 * <ol>
 *   <li><strong>Step 1</strong>: Calcite VolcanoPlanner optimizes the logical plan
 *       (filter/project/agg pushdown)
 *   <li><strong>Step 2</strong>: DistributedQueryPlanner creates distributed execution stages with
 *       exchange boundaries
 * </ol>
 *
 * <p>The planner analyzes PPL queries that have been converted to Calcite RelNode trees and breaks
 * them into stages that can be executed across multiple nodes in parallel:
 *
 * <ul>
 *   <li><strong>Stage 1 (SCAN)</strong>: Direct shard access with filters and projections
 *   <li><strong>Stage 2 (PROCESS)</strong>: Partial aggregations on each node
 *   <li><strong>Stage 3 (FINALIZE)</strong>: Global aggregation on coordinator
 * </ul>
 */
@Log4j2
@RequiredArgsConstructor
public class DistributedQueryPlanner {

  private final PartitionDiscovery partitionDiscovery;

  /**
   * Converts a Calcite RelNode into a distributed physical plan.
   *
   * @param relNode The Calcite RelNode to convert
   * @param context The Calcite plan context
   * @return Multi-stage distributed execution plan
   */
  public DistributedPhysicalPlan plan(RelNode relNode, CalcitePlanContext context) {
    String planId = "distributed-plan-" + UUID.randomUUID().toString().substring(0, 8);
    log.info("Creating distributed physical plan: {}", planId);

    try {
      // Analyze the RelNode tree to determine distributed execution strategy
      DistributedPlanAnalyzer analyzer = new DistributedPlanAnalyzer();
      RelNodeAnalysis analysis = analyzer.analyze(relNode, context);

      if (!analysis.isDistributable()) {
        log.debug("RelNode not suitable for distributed execution: {}", analysis.getReason());
        throw new UnsupportedOperationException(
            "RelNode not suitable for distributed execution: " + analysis.getReason());
      }

      // Create execution stages based on RelNode analysis
      List<ExecutionStage> stages = createExecutionStages(analysis);

      // Build the final distributed plan
      DistributedPhysicalPlan distributedPlan =
          DistributedPhysicalPlan.create(planId, stages, analysis.getOutputSchema());

      // Store RelNode and context for execution
      distributedPlan.setLocalExecutionContext(relNode, context);

      log.info("Created distributed plan {} with {} stages", planId, stages.size());
      return distributedPlan;

    } catch (Exception e) {
      log.error("Failed to create distributed physical plan for: {}", relNode, e);
      throw new RuntimeException("Failed to create distributed physical plan", e);
    }
  }

  /** Creates execution stages from the RelNode analysis. */
  private List<ExecutionStage> createExecutionStages(RelNodeAnalysis analysis) {
    List<ExecutionStage> stages = new ArrayList<>();

    if (analysis.hasJoin()) {
      // Join query: create two SCAN stages (left + right) tagged with "side" property
      ExecutionStage leftScanStage = createJoinScanStage(analysis.getLeftTableName(), "left");
      stages.add(leftScanStage);

      ExecutionStage rightScanStage = createJoinScanStage(analysis.getRightTableName(), "right");
      stages.add(rightScanStage);

      // Finalize stage depends on both scan stages
      ExecutionStage finalStage =
          createResultCollectionStage(
              analysis, leftScanStage.getStageId(), rightScanStage.getStageId());
      stages.add(finalStage);
    } else {
      // Stage 1: Distributed scanning with filters and projections
      ExecutionStage scanStage = createScanStage(analysis);
      stages.add(scanStage);

      // Stage 2: Partial aggregation (if needed)
      if (analysis.hasAggregation()) {
        ExecutionStage processStage =
            createPartialAggregationStage(analysis, scanStage.getStageId());
        stages.add(processStage);

        // Stage 3: Final aggregation
        ExecutionStage finalStage =
            createFinalAggregationStage(analysis, processStage.getStageId());
        stages.add(finalStage);
      } else {
        // No aggregation - add finalize stage for result collection
        ExecutionStage finalStage = createResultCollectionStage(analysis, scanStage.getStageId());
        stages.add(finalStage);
      }
    }

    return stages;
  }

  /** Creates a SCAN stage for one side of a join, tagged with "side" property. */
  private ExecutionStage createJoinScanStage(String tableName, String side) {
    String stageId = side + "-scan-stage-" + UUID.randomUUID().toString().substring(0, 8);

    List<DataPartition> partitions = partitionDiscovery.discoverPartitions(tableName);

    List<WorkUnit> workUnits =
        partitions.stream()
            .map(
                partition -> {
                  String workUnitId = side + "-scan-" + partition.getPartitionId();
                  return WorkUnit.createScanUnit(workUnitId, partition, partition.getNodeId());
                })
            .collect(Collectors.toList());

    log.debug("Created {} scan stage {} with {} work units", side, stageId, workUnits.size());

    ExecutionStage stage = ExecutionStage.createScanStage(stageId, workUnits);
    stage.setProperties(new HashMap<>(Map.of("side", side, "tableName", tableName)));
    return stage;
  }

  /** Creates Stage 1: Distributed scanning with filters and projections. */
  private ExecutionStage createScanStage(RelNodeAnalysis analysis) {
    String stageId = "scan-stage-" + UUID.randomUUID().toString().substring(0, 8);

    // Discover partitions for the target table
    List<DataPartition> partitions = partitionDiscovery.discoverPartitions(analysis.getTableName());

    // Create work units for each partition (shard)
    List<WorkUnit> workUnits =
        partitions.stream()
            .map(partition -> createScanWorkUnit(partition))
            .collect(Collectors.toList());

    log.debug("Created scan stage {} with {} work units", stageId, workUnits.size());

    return ExecutionStage.createScanStage(stageId, workUnits);
  }

  /** Creates a scan work unit for a specific partition. */
  private WorkUnit createScanWorkUnit(DataPartition partition) {
    String workUnitId = "scan-" + partition.getPartitionId();
    return WorkUnit.createScanUnit(workUnitId, partition, partition.getNodeId());
  }

  /** Creates Stage 2: Partial aggregation processing. */
  private ExecutionStage createPartialAggregationStage(
      RelNodeAnalysis analysis, String scanStageId) {
    String stageId = "partial-agg-stage-" + UUID.randomUUID().toString().substring(0, 8);

    List<WorkUnit> workUnits =
        IntStream.range(0, 3) // Assume 3 data nodes for now
            .mapToObj(
                i -> {
                  String workUnitId = "partial-agg-" + i;
                  return WorkUnit.createProcessUnit(workUnitId, List.of(scanStageId));
                })
            .collect(Collectors.toList());

    log.debug("Created partial aggregation stage {} with {} work units", stageId, workUnits.size());

    return ExecutionStage.createProcessStage(
        stageId, workUnits, List.of(scanStageId), ExecutionStage.DataExchangeType.NONE);
  }

  /** Creates Stage 3: Final aggregation. */
  private ExecutionStage createFinalAggregationStage(
      RelNodeAnalysis analysis, String processStageId) {
    String stageId = "final-agg-stage-" + UUID.randomUUID().toString().substring(0, 8);
    String workUnitId = "final-agg";

    WorkUnit finalWorkUnit = WorkUnit.createFinalizeUnit(workUnitId, List.of(processStageId));

    log.debug("Created final aggregation stage {}", stageId);

    return ExecutionStage.createFinalizeStage(stageId, finalWorkUnit, List.of(processStageId));
  }

  /** Creates a result collection stage for non-aggregation queries. */
  private ExecutionStage createResultCollectionStage(
      RelNodeAnalysis analysis, String... dependencyStageIds) {
    String stageId = "collect-stage-" + UUID.randomUUID().toString().substring(0, 8);
    String workUnitId = "collect-results";

    List<String> deps = List.of(dependencyStageIds);

    WorkUnit collectWorkUnit = WorkUnit.createFinalizeUnit(workUnitId, deps);

    log.debug("Created result collection stage {}", stageId);

    return ExecutionStage.createFinalizeStage(stageId, collectWorkUnit, deps);
  }
}
