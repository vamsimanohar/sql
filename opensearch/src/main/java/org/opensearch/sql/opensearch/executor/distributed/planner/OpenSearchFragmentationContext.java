/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelNode;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.opensearch.executor.distributed.dataunit.OpenSearchDataUnitSource;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitSource;
import org.opensearch.sql.planner.distributed.planner.CostEstimator;
import org.opensearch.sql.planner.distributed.planner.FragmentationContext;

/**
 * Provides cluster topology and data unit discovery to the plan fragmenter. Reads data node IDs and
 * shard information from the OpenSearch ClusterState.
 */
public class OpenSearchFragmentationContext implements FragmentationContext {

  private final ClusterService clusterService;
  private final CostEstimator costEstimator;

  public OpenSearchFragmentationContext(ClusterService clusterService) {
    this.clusterService = clusterService;
    this.costEstimator = createStubCostEstimator();
  }

  public OpenSearchFragmentationContext(
      ClusterService clusterService, CostEstimator costEstimator) {
    this.clusterService = clusterService;
    this.costEstimator = costEstimator;
  }

  @Override
  public List<String> getAvailableNodes() {
    return clusterService.state().nodes().getDataNodes().values().stream()
        .map(DiscoveryNode::getId)
        .collect(Collectors.toList());
  }

  @Override
  public CostEstimator getCostEstimator() {
    return costEstimator;
  }

  /**
   * Creates a stub cost estimator that returns -1 for all estimates. Used when no enhanced cost
   * estimator is provided.
   */
  private CostEstimator createStubCostEstimator() {
    return new CostEstimator() {
      @Override
      public long estimateRowCount(RelNode relNode) {
        return -1;
      }

      @Override
      public long estimateSizeBytes(RelNode relNode) {
        return -1;
      }

      @Override
      public double estimateSelectivity(RelNode relNode) {
        return 1.0;
      }
    };
  }

  @Override
  public DataUnitSource getDataUnitSource(String tableName) {
    return new OpenSearchDataUnitSource(clusterService, tableName);
  }

  @Override
  public int getMaxTasksPerStage() {
    return clusterService.state().nodes().getDataNodes().size();
  }

  @Override
  public String getCoordinatorNodeId() {
    return clusterService.localNode().getId();
  }
}
