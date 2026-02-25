/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.operator;

import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.page.Page;

/**
 * A source operator that reads data from external storage (e.g., Lucene shards). Source operators
 * do not accept input from upstream operators — they produce data from assigned {@link DataUnit}s.
 *
 * <p>The pipeline driver assigns data units via {@link #addDataUnit(DataUnit)} and signals
 * completion via {@link #noMoreDataUnits()}. The operator reads data from data units and produces
 * {@link Page} batches via {@link #getOutput()}.
 */
public interface SourceOperator extends Operator {

  /**
   * Assigns a unit of work (e.g., a shard) to this source operator.
   *
   * @param dataUnit the data unit to read from
   */
  void addDataUnit(DataUnit dataUnit);

  /** Signals that no more data units will be assigned. */
  void noMoreDataUnits();

  /** Source operators never accept input from upstream. */
  @Override
  default boolean needsInput() {
    return false;
  }

  /** Source operators never accept input from upstream. */
  @Override
  default void addInput(Page page) {
    throw new UnsupportedOperationException("Source operators do not accept input");
  }
}
