/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.dataunit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitSource;

/**
 * Discovers OpenSearch shards for a given index from ClusterState. Each shard becomes an {@link
 * OpenSearchDataUnit} with preferred node information from the primary and replica assignments.
 *
 * <p>All shards are returned in a single batch since shard discovery is a lightweight metadata
 * operation.
 */
public class OpenSearchDataUnitSource implements DataUnitSource {

  private final ClusterService clusterService;
  private final String indexName;
  private boolean finished;

  public OpenSearchDataUnitSource(ClusterService clusterService, String indexName) {
    this.clusterService = clusterService;
    this.indexName = indexName;
    this.finished = false;
  }

  @Override
  public List<DataUnit> getNextBatch(int maxBatchSize) {
    if (finished) {
      return List.of();
    }
    finished = true;

    ClusterState state = clusterService.state();
    IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);
    if (indexRoutingTable == null) {
      throw new IllegalArgumentException("Index not found in cluster routing table: " + indexName);
    }

    List<DataUnit> dataUnits = new ArrayList<>();
    for (Map.Entry<Integer, IndexShardRoutingTable> entry :
        indexRoutingTable.getShards().entrySet()) {
      int shardId = entry.getKey();
      IndexShardRoutingTable shardRoutingTable = entry.getValue();
      List<String> preferredNodes = new ArrayList<>();

      // Primary shard first, then replicas
      ShardRouting primary = shardRoutingTable.primaryShard();
      if (primary.assignedToNode()) {
        preferredNodes.add(primary.currentNodeId());
      }
      for (ShardRouting replica : shardRoutingTable.replicaShards()) {
        if (replica.assignedToNode()) {
          preferredNodes.add(replica.currentNodeId());
        }
      }

      if (preferredNodes.isEmpty()) {
        throw new IllegalStateException(
            "Shard " + indexName + "/" + shardId + " has no assigned nodes");
      }

      dataUnits.add(new OpenSearchDataUnit(indexName, shardId, preferredNodes, -1, -1));
    }

    return dataUnits;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void close() {
    // No resources to release
  }
}
