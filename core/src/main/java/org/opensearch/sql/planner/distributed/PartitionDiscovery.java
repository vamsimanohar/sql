/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.List;

/**
 * Interface for discovering data partitions in tables.
 *
 * <p>Implementations map table names to their physical partitions (shards, files, etc.) so the
 * distributed planner can create work units for parallel execution.
 */
public interface PartitionDiscovery {
  /**
   * Discovers data partitions for a given table name.
   *
   * @param tableName Table name to discover partitions for
   * @return List of data partitions (shards, files, etc.)
   */
  List<DataPartition> discoverPartitions(String tableName);
}
