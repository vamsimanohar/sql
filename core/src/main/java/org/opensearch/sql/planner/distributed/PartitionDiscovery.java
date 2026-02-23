/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.List;

/**
 * Interface for discovering data partitions from storage systems.
 *
 * <p>This abstraction allows the distributed planner to work with different storage backends
 * without direct dependencies on specific implementations.
 */
public interface PartitionDiscovery {

  /**
   * Discovers all available partitions for the given table/index.
   *
   * @param tableName The name of the table/index to discover partitions for
   * @return List of data partitions available for processing
   */
  List<DataPartition> getPartitions(String tableName);
}
