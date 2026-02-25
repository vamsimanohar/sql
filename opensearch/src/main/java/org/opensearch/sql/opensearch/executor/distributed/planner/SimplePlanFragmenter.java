/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitSource;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;
import org.opensearch.sql.planner.distributed.planner.PlanFragmenter;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;
import org.opensearch.sql.planner.distributed.stage.PartitioningScheme;
import org.opensearch.sql.planner.distributed.stage.StagedPlan;

/**
 * Creates a 2-stage plan for single-table scan queries.
 *
 * <pre>
 * Stage "0" (leaf):  GATHER exchange, holds dataUnits (shards), stores RelNode as planFragment
 * Stage "1" (root):  NONE exchange, depends on stage-0, coordinator merge (no dataUnits)
 * </pre>
 *
 * <p>Supported query patterns: simple scans, scans with filter, scans with limit, scans with filter
 * and limit. Throws {@link UnsupportedOperationException} for joins, aggregations, or other complex
 * patterns.
 */
public class SimplePlanFragmenter implements PlanFragmenter {

  @Override
  public StagedPlan fragment(RelNode optimizedPlan, FragmentationContext context) {
    RelNodeAnalyzer.AnalysisResult analysis = RelNodeAnalyzer.analyze(optimizedPlan);
    String indexName = analysis.getIndexName();

    // Discover shards for the index
    DataUnitSource dataUnitSource = context.getDataUnitSource(indexName);
    List<DataUnit> dataUnits = dataUnitSource.getNextBatch();
    dataUnitSource.close();

    // Estimate rows (use cost estimator if available, otherwise -1)
    long estimatedRows = context.getCostEstimator().estimateRowCount(optimizedPlan);

    // Stage 0: Leaf stage — runs on data nodes, one task per shard group
    ComputeStage leafStage =
        new ComputeStage(
            "0",
            PartitioningScheme.gather(),
            List.of(),
            dataUnits,
            estimatedRows,
            -1,
            optimizedPlan);

    // Stage 1: Root stage — runs on coordinator, merges results from stage 0
    ComputeStage rootStage =
        new ComputeStage(
            "1", PartitioningScheme.none(), List.of("0"), List.of(), estimatedRows, -1);

    String planId = "plan-" + UUID.randomUUID().toString().substring(0, 8);
    return new StagedPlan(planId, List.of(leafStage, rootStage));
  }
}
