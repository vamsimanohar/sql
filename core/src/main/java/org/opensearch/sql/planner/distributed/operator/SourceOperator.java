/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.operator;

import org.opensearch.sql.planner.distributed.page.Page;
import org.opensearch.sql.planner.distributed.split.Split;

/**
 * A source operator that reads data from external storage (e.g., Lucene shards). Source operators
 * do not accept input from upstream operators — they produce data from assigned {@link Split}s.
 *
 * <p>The pipeline driver assigns splits via {@link #addSplit(Split)} and signals completion via
 * {@link #noMoreSplits()}. The operator reads data from splits and produces {@link Page} batches
 * via {@link #getOutput()}.
 */
public interface SourceOperator extends Operator {

  /**
   * Assigns a unit of work (e.g., a shard) to this source operator.
   *
   * @param split the split to read from
   */
  void addSplit(Split split);

  /** Signals that no more splits will be assigned. */
  void noMoreSplits();

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
