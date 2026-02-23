/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.planner.distributed.DataPartition;
import org.opensearch.sql.planner.distributed.DistributedQueryPlanner.PartitionDiscovery;

/**
 * OpenSearch-specific implementation of partition discovery for distributed queries.
 *
 * <p>Discovers data partitions (shards) within OpenSearch indexes, providing information needed for
 * data locality optimization in distributed execution.
 *
 * <p><strong>Partition Information:</strong>
 *
 * <ul>
 *   <li>Shard ID and index name for Lucene access
 *   <li>Node assignment for data locality
 *   <li>Estimated shard size for scheduling optimization
 * </ul>
 *
 * <p><strong>Phase 1 Implementation:</strong> - Basic shard discovery from cluster routing table -
 * Simple size estimation (placeholder) - Primary shard only (no replica handling)
 */
@Log4j2
@RequiredArgsConstructor
public class OpenSearchPartitionDiscovery implements PartitionDiscovery {

  private final ClusterService clusterService;

  @Override
  public List<DataPartition> discoverPartitions(String tableName) {
    log.info("Discovering partitions for table: {}", tableName);

    List<DataPartition> partitions = new ArrayList<>();

    try {
      // Parse index pattern from table name
      // In PPL: "search source=logs-*" -> tableName could be "logs-*"
      String indexPattern = parseIndexPattern(tableName);

      // Get routing table for the indexes
      var clusterState = clusterService.state();
      var routingTable = clusterState.routingTable();

      // Find matching indexes
      for (IndexRoutingTable indexRoutingTable : routingTable) {
        String indexName = indexRoutingTable.getIndex().getName();

        // Simple pattern matching for Phase 1 (exact match or wildcard)
        if (matchesPattern(indexName, indexPattern)) {
          log.debug("Processing index: {} for pattern: {}", indexName, indexPattern);

          // Discover shards for this index
          List<DataPartition> indexPartitions = discoverIndexShards(indexName, indexRoutingTable);
          partitions.addAll(indexPartitions);
        }
      }

      log.info("Discovered {} partitions for table: {}", partitions.size(), tableName);

    } catch (Exception e) {
      log.error("Failed to discover partitions for table: {}", tableName, e);
      throw new RuntimeException("Partition discovery failed for: " + tableName, e);
    }

    return partitions;
  }

  /** Discovers shards for a specific index. */
  private List<DataPartition> discoverIndexShards(
      String indexName, IndexRoutingTable indexRoutingTable) {
    List<DataPartition> shards = new ArrayList<>();

    for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
      int shardId = shardRoutingTable.shardId().id();

      // For Phase 1, we'll use primary shards only
      ShardRouting primaryShard = shardRoutingTable.primaryShard();
      if (primaryShard != null && primaryShard.assignedToNode()) {
        String nodeId = primaryShard.currentNodeId();

        // Create partition for this shard
        DataPartition partition =
            DataPartition.createLucenePartition(
                String.valueOf(shardId), indexName, nodeId, estimateShardSize(indexName, shardId));

        shards.add(partition);
        log.debug("Added partition for shard: {}/{} on node: {}", indexName, shardId, nodeId);
      }
    }

    return shards;
  }

  /**
   * Parses the index pattern from table name.
   *
   * @param tableName Table name from PPL query (e.g., "logs-*", "events-2024-*")
   * @return Index pattern for matching
   */
  private String parseIndexPattern(String tableName) {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name cannot be null");
    }

    String pattern = tableName.trim();

    // Handle Calcite qualified name format: [schema, table]
    if (pattern.startsWith("[") && pattern.endsWith("]")) {
      pattern = pattern.substring(1, pattern.length() - 1);
      String[] parts = pattern.split(",");
      pattern = parts[parts.length - 1].trim();
    }

    // Remove quotes if present
    if (pattern.startsWith("\"") && pattern.endsWith("\"")) {
      pattern = pattern.substring(1, pattern.length() - 1);
    }

    return pattern;
  }

  /** Checks if an index name matches the given pattern. */
  private boolean matchesPattern(String indexName, String pattern) {
    if (pattern.equals(indexName)) {
      return true; // Exact match
    }

    if (pattern.contains("*")) {
      // Simple wildcard matching for Phase 1
      String regex = pattern.replace("*", ".*");
      return indexName.matches(regex);
    }

    return false;
  }

  /**
   * Estimates the size of a shard in bytes.
   *
   * @param indexName Index name
   * @param shardId Shard ID
   * @return Estimated size in bytes
   */
  private long estimateShardSize(String indexName, int shardId) {
    // TODO: Phase 1 - Implement actual shard size estimation
    // This could use:
    // - Index stats API to get shard sizes
    // - Cluster stats for approximation
    // - Historical sizing data

    // For Phase 1, return a placeholder estimate
    // This helps with work distribution even if not accurate
    return 100 * 1024 * 1024L; // 100MB placeholder
  }
}
