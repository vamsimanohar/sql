/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.planner.logical;

import java.util.Arrays;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeAggAndIndexScan;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeAggAndRelation;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeFilterAndRelation;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeLimitAndIndexScan;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeLimitAndRelation;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeSortAndIndexAgg;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeSortAndIndexScan;
import org.opensearch.sql.prometheus.planner.logical.rule.MergeSortAndRelation;
import org.opensearch.sql.prometheus.planner.logical.rule.PushProjectAndIndexScan;
import org.opensearch.sql.prometheus.planner.logical.rule.PushProjectAndRelation;

/**
 * OpenSearch storage specified logical plan optimizer.
 */
@UtilityClass
public class PrometheusLogicalPlanOptimizerFactory {

  /**
   * Create OpenSearch storage specified logical plan optimizer.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(Arrays.asList(
        new MergeFilterAndRelation(),
        new MergeAggAndIndexScan(),
        new MergeAggAndRelation(),
        new MergeSortAndRelation(),
        new MergeSortAndIndexScan(),
        new MergeSortAndIndexAgg(),
        new MergeSortAndIndexScan(),
        new MergeLimitAndRelation(),
        new MergeLimitAndIndexScan(),
        new PushProjectAndRelation(),
        new PushProjectAndIndexScan()
    ));
  }
}
