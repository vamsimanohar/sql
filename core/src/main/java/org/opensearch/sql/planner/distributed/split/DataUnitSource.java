/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.split;

import java.util.List;

/**
 * Generates {@link DataUnit}s for a source operator. Implementations discover available data units
 * (e.g., shards) from cluster state and create them with preferred node information.
 */
public interface DataUnitSource extends AutoCloseable {

  /**
   * Returns the next batch of data units, up to the specified maximum batch size. Returns an empty
   * list if no more data units are available.
   *
   * @param maxBatchSize maximum number of data units to return
   * @return list of data units
   */
  List<DataUnit> getNextBatch(int maxBatchSize);

  /**
   * Returns the next batch of data units with a default batch size.
   *
   * @return list of data units
   */
  default List<DataUnit> getNextBatch() {
    return getNextBatch(1000);
  }

  /** Returns true if all data units have been generated. */
  boolean isFinished();

  @Override
  void close();
}
