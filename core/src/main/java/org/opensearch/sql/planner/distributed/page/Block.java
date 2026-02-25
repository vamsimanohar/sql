/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.page;

/**
 * A column of data within a {@link Page}. Each Block holds values for a single column across all
 * rows in the page. Designed to align with Apache Arrow's columnar model: a future {@code
 * ArrowBlock} implementation can wrap Arrow {@code FieldVector} for zero-copy exchange via Arrow
 * IPC.
 */
public interface Block {

  /** Returns the number of values (rows) in this block. */
  int getPositionCount();

  /**
   * Returns the value at the given position.
   *
   * @param position the row index (0-based)
   * @return the value, or null if the position is null
   */
  Object getValue(int position);

  /**
   * Returns true if the value at the given position is null.
   *
   * @param position the row index (0-based)
   * @return true if null
   */
  boolean isNull(int position);

  /** Returns the estimated memory retained by this block in bytes. */
  long getRetainedSizeBytes();

  /**
   * Returns a sub-region of this block.
   *
   * @param positionOffset the starting row index
   * @param length the number of rows in the region
   * @return a new Block representing the sub-region
   */
  Block getRegion(int positionOffset, int length);

  /** Returns the data type of this block's values. */
  BlockType getType();

  /** Supported block data types, aligned with Arrow's type system. */
  enum BlockType {
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    BYTES,
    TIMESTAMP,
    DATE,
    NULL,
    UNKNOWN
  }
}
