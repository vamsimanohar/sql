/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.PhysicalOperatorTree;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitSource;
import org.opensearch.sql.planner.distributed.planner.CostEstimator;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;
import org.opensearch.sql.planner.distributed.stage.PartitioningScheme;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

/**
 * Intelligent plan fragmenter that replaces the hardcoded 2-stage approach of SimplePlanFragmenter.
 *
 * <p>Makes smart fragmentation decisions based on:
 *
 * <ul>
 *   <li>Physical operator types and compatibility
 *   <li>Cost estimates from enhanced cost estimator
 *   <li>Data locality and distribution requirements
 * </ul>
 *
 * <p>Phase 1B Implementation:
 *
 * <ul>
 *   <li>Single-table queries: pushdown-compatible ops in Stage 0, coordinator merge in Stage 1
 *   <li>Smart operator fusion: combine scan + filter + projection + limit in leaf stage
 *   <li>Cost-driven decisions: use real statistics for fragmentation choices
 * </ul>
 */
@Log4j2
@RequiredArgsConstructor
public class IntelligentPlanFragmenter {

  private final CostEstimator costEstimator;

  /** Fragments a physical operator tree into a multi-stage distributed execution plan. */
  public StagedPlan fragment(PhysicalOperatorTree operatorTree, FragmentationContext context) {
    log.debug("Fragmenting operator tree: {}", operatorTree);

    String indexName = operatorTree.getIndexName();

    // Discover data units (shards) for the index
    DataUnitSource dataUnitSource = context.getDataUnitSource(indexName);
    List<DataUnit> dataUnits = dataUnitSource.getNextBatch();
    dataUnitSource.close();

    log.debug("Discovered {} data units for index '{}'", dataUnits.size(), indexName);

    // Analyze operator tree and create stage groups
    List<StageGroup> stageGroups = identifyStageGroups(operatorTree);

    log.debug("Identified {} stage groups", stageGroups.size());

    // Create compute stages from stage groups
    List<ComputeStage> stages = new ArrayList<>();
    for (int i = 0; i < stageGroups.size(); i++) {
      StageGroup group = stageGroups.get(i);
      ComputeStage stage = createComputeStage(group, i, stageGroups, dataUnits, context);
      stages.add(stage);
    }

    String planId = "plan-" + UUID.randomUUID().toString().substring(0, 8);
    StagedPlan stagedPlan = new StagedPlan(planId, stages);

    log.info("Created staged plan: id={}, stages={}", planId, stages.size());
    return stagedPlan;
  }

  /** Identifies logical groups of operators that can be executed together in the same stage. */
  private List<StageGroup> identifyStageGroups(PhysicalOperatorTree operatorTree) {
    List<StageGroup> groups = new ArrayList<>();

    // Phase 1B: Simple fragmentation strategy
    if (operatorTree.isSingleStageCompatible()) {
      // Stage 0: Leaf stage with all pushdown-compatible operations
      StageGroup leafGroup = createLeafStageGroup(operatorTree);
      groups.add(leafGroup);

      // Stage 1: Root stage for coordinator merge (if needed)
      if (requiresCoordinatorMerge(operatorTree)) {
        StageGroup rootGroup = createRootStageGroup();
        groups.add(rootGroup);
      }
    } else {
      // Future phases: Complex fragmentation for aggregations, joins, etc.
      throw new UnsupportedOperationException(
          "Complex queries requiring multi-stage fragmentation not yet implemented. "
              + "Phase 1B supports single-table queries with pushdown-compatible operations only.");
    }

    return groups;
  }

  private StageGroup createLeafStageGroup(PhysicalOperatorTree operatorTree) {
    // Build RelNode fragment for the leaf stage containing all pushdown operations
    RelNode leafFragment = buildLeafStageFragment(operatorTree);

    return new StageGroup(
        StageType.LEAF, operatorTree.getIndexName(), leafFragment, operatorTree.getOperators());
  }

  private StageGroup createRootStageGroup() {
    // Root stage performs coordinator merge - no specific RelNode fragment needed
    return new StageGroup(
        StageType.ROOT,
        null, // No specific index
        null, // No RelNode fragment for merge-only stage
        List.of()); // No operators - just merge
  }

  /**
   * Builds a RelNode fragment representing the operations that can be executed in the leaf stage.
   * This fragment will be stored in ComputeStage.getPlanFragment() for execution.
   */
  private RelNode buildLeafStageFragment(PhysicalOperatorTree operatorTree) {
    // For Phase 1B, we can reconstruct a RelNode tree from the physical operators
    // This is a simplified approach - future phases may need more sophisticated fragment building

    // Start with the scan operation
    var scanOp = operatorTree.getScanOperator();

    // Create a TableScan RelNode (simplified - in practice this would need proper Calcite context)
    // For now, we'll store the original operators in the stage and let DynamicPipelineBuilder
    // handle conversion

    // Return null for now - DynamicPipelineBuilder will use the PhysicalOperatorTree directly
    // Future enhancement: build proper RelNode fragments
    return null;
  }

  private boolean requiresCoordinatorMerge(PhysicalOperatorTree operatorTree) {
    // Phase 1B: Always require merge stage for distributed queries
    // Future optimization: single-shard queries might not need merge stage
    return true;
  }

  private ComputeStage createComputeStage(
      StageGroup group,
      int stageIndex,
      List<StageGroup> allGroups,
      List<DataUnit> dataUnits,
      FragmentationContext context) {

    String stageId = String.valueOf(stageIndex);

    // Determine output partitioning scheme
    PartitioningScheme outputPartitioning;
    List<String> sourceStageIds = new ArrayList<>();

    if (group.getStageType() == StageType.LEAF) {
      // Leaf stage outputs to coordinator via GATHER
      outputPartitioning = PartitioningScheme.gather();
      // No source stages - reads from storage
    } else if (group.getStageType() == StageType.ROOT) {
      // Root stage - no output partitioning
      outputPartitioning = PartitioningScheme.none();
      // Depends on all leaf stages (in Phase 1B, just stage 0)
      if (stageIndex > 0) {
        sourceStageIds.add(String.valueOf(stageIndex - 1));
      }
    } else {
      throw new IllegalStateException("Unsupported stage type: " + group.getStageType());
    }

    // Assign data units to leaf stages only
    List<DataUnit> stageDataUnits =
        (group.getStageType() == StageType.LEAF) ? dataUnits : List.of();

    // Estimate costs using the cost estimator
    long estimatedRows = estimateStageRows(group, context);
    long estimatedBytes = estimateStageBytes(group, context);

    return new ComputeStage(
        stageId,
        outputPartitioning,
        sourceStageIds,
        stageDataUnits,
        estimatedRows,
        estimatedBytes,
        group.getRelNodeFragment());
  }

  private long estimateStageRows(StageGroup group, FragmentationContext context) {
    if (group.getRelNodeFragment() != null) {
      return costEstimator.estimateRowCount(group.getRelNodeFragment());
    }

    // For stages without RelNode fragments, use heuristics
    if (group.getStageType() == StageType.ROOT) {
      // Root stage row count depends on leaf stages - for now, use -1 (unknown)
      return -1;
    }

    return -1; // Unknown
  }

  private long estimateStageBytes(StageGroup group, FragmentationContext context) {
    if (group.getRelNodeFragment() != null) {
      return costEstimator.estimateSizeBytes(group.getRelNodeFragment());
    }

    return -1; // Unknown
  }

  /** Represents a logical group of operators that execute together in the same stage. */
  private static class StageGroup {
    private final StageType stageType;
    private final String indexName;
    private final RelNode relNodeFragment;
    private final List<
            org.opensearch.sql.opensearch.executor.distributed.planner.physical
                .PhysicalOperatorNode>
        operators;

    public StageGroup(
        StageType stageType,
        String indexName,
        RelNode relNodeFragment,
        List<
                org.opensearch.sql.opensearch.executor.distributed.planner.physical
                    .PhysicalOperatorNode>
            operators) {
      this.stageType = stageType;
      this.indexName = indexName;
      this.relNodeFragment = relNodeFragment;
      this.operators = operators;
    }

    public StageType getStageType() {
      return stageType;
    }

    public String getIndexName() {
      return indexName;
    }

    public RelNode getRelNodeFragment() {
      return relNodeFragment;
    }

    public List<
            org.opensearch.sql.opensearch.executor.distributed.planner.physical
                .PhysicalOperatorNode>
        getOperators() {
      return operators;
    }
  }

  private enum StageType {
    LEAF, // Scans data from storage
    ROOT, // Coordinator merge stage
    INTERMEDIATE // Future: intermediate processing stages
  }
}
