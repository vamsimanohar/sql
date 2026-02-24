/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.page;

/**
 * A batch of rows or columns flowing through the operator pipeline. Designed to be columnar-ready:
 * Phase 5A uses a row-based implementation ({@link RowPage}), but future phases can swap in an
 * Arrow-backed implementation for zero-copy columnar processing.
 */
public interface Page {

  /** Returns the number of rows in this page. */
  int getPositionCount();

  /** Returns the number of columns in this page. */
  int getChannelCount();

  /**
   * Returns the value at the given row and column position.
   *
   * @param position the row index (0-based)
   * @param channel the column index (0-based)
   * @return the value, or null if the cell is null
   */
  Object getValue(int position, int channel);

  /**
   * Returns a sub-region of this page.
   *
   * @param positionOffset the starting row index
   * @param length the number of rows in the region
   * @return a new Page representing the sub-region
   */
  Page getRegion(int positionOffset, int length);

  /** Returns an empty page with zero rows and the given number of columns. */
  static Page empty(int channelCount) {
    return new RowPage(new Object[0][channelCount], channelCount);
  }
}
