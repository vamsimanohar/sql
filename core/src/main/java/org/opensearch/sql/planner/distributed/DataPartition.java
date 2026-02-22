/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a partition of data that can be processed independently by a work unit.
 *
 * <p>Data partitions abstract the storage-specific details of how data is divided:
 *
 * <ul>
 *   <li>For Lucene: Represents an OpenSearch shard
 *   <li>For Parquet: Represents a file or file group
 *   <li>For future formats: Represents appropriate storage unit
 * </ul>
 *
 * <p>The partition contains metadata needed for the task operator to:
 *
 * <ul>
 *   <li>Locate the data (index, shard, file path, etc.)
 *   <li>Apply filters and projections efficiently
 *   <li>Coordinate with storage-specific optimizations
 * </ul>
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataPartition {

  /** Unique identifier for this partition */
  private String partitionId;

  /** Type of storage system containing this partition */
  private StorageType storageType;

  /** Storage-specific location information */
  private String location;

  /** Estimated size in bytes (for scheduling optimization) */
  private long estimatedSizeBytes;

  /** Storage-specific metadata for partition access */
  private Map<String, Object> metadata;

  /** Enumeration of supported storage types for data partitions. */
  public enum StorageType {
    /** OpenSearch Lucene indexes - current implementation target */
    LUCENE,

    /** Parquet columnar files - future Phase 3 support */
    PARQUET,

    /** ORC columnar files - future Phase 3 support */
    ORC,

    /** Delta Lake tables - future Phase 4 support */
    DELTA_LAKE,

    /** Apache Iceberg tables - future Phase 4 support */
    ICEBERG
  }

  /**
   * Creates a Lucene shard partition for OpenSearch index scanning.
   *
   * @param shardId OpenSearch shard identifier
   * @param indexName OpenSearch index name
   * @param nodeId Node containing this shard
   * @param estimatedSize Estimated shard size in bytes
   * @return Configured Lucene partition
   */
  public static DataPartition createLucenePartition(
      String shardId, String indexName, String nodeId, long estimatedSize) {
    return new DataPartition(
        shardId,
        StorageType.LUCENE,
        indexName + "/" + shardId,
        estimatedSize,
        Map.of(
            "indexName", indexName,
            "shardId", shardId,
            "nodeId", nodeId));
  }

  /**
   * Creates a Parquet file partition for columnar scanning.
   *
   * @param fileId File identifier
   * @param filePath File system path
   * @param estimatedSize File size in bytes
   * @return Configured Parquet partition
   */
  public static DataPartition createParquetPartition(
      String fileId, String filePath, long estimatedSize) {
    return new DataPartition(
        fileId, StorageType.PARQUET, filePath, estimatedSize, Map.of("filePath", filePath));
  }

  /**
   * Gets the index name for Lucene partitions.
   *
   * @return Index name or null if not a Lucene partition
   */
  public String getIndexName() {
    if (storageType == StorageType.LUCENE && metadata != null) {
      return (String) metadata.get("indexName");
    }
    return null;
  }

  /**
   * Gets the shard ID for Lucene partitions.
   *
   * @return Shard ID or null if not a Lucene partition
   */
  public String getShardId() {
    if (storageType == StorageType.LUCENE && metadata != null) {
      return (String) metadata.get("shardId");
    }
    return null;
  }

  /**
   * Gets the node ID containing this partition (for data locality).
   *
   * @return Node ID or null if not specified
   */
  public String getNodeId() {
    if (metadata != null) {
      return (String) metadata.get("nodeId");
    }
    return null;
  }

  /**
   * Checks if this partition is local to the specified node.
   *
   * @param nodeId Node to check locality against
   * @return true if partition is local to the node, false otherwise
   */
  public boolean isLocalTo(String nodeId) {
    String partitionNodeId = getNodeId();
    return partitionNodeId != null && partitionNodeId.equals(nodeId);
  }
}
