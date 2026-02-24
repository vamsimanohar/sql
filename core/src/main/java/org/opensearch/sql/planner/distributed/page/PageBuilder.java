/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.page;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link Page} row by row. Call {@link #beginRow()}, set values via {@link #setValue(int,
 * Object)}, then {@link #endRow()} to commit. Call {@link #build()} to produce the final Page.
 */
public class PageBuilder {

  private final int channelCount;
  private final List<Object[]> rows;
  private Object[] currentRow;

  public PageBuilder(int channelCount) {
    if (channelCount < 0) {
      throw new IllegalArgumentException("channelCount must be non-negative: " + channelCount);
    }
    this.channelCount = channelCount;
    this.rows = new ArrayList<>();
  }

  /** Starts a new row. Values default to null. */
  public void beginRow() {
    currentRow = new Object[channelCount];
  }

  /**
   * Sets a value in the current row.
   *
   * @param channel the column index (0-based)
   * @param value the value to set
   */
  public void setValue(int channel, Object value) {
    if (currentRow == null) {
      throw new IllegalStateException("beginRow() must be called before setValue()");
    }
    if (channel < 0 || channel >= channelCount) {
      throw new IndexOutOfBoundsException(
          "Channel " + channel + " out of range [0, " + channelCount + ")");
    }
    currentRow[channel] = value;
  }

  /** Commits the current row to the page. */
  public void endRow() {
    if (currentRow == null) {
      throw new IllegalStateException("beginRow() must be called before endRow()");
    }
    rows.add(currentRow);
    currentRow = null;
  }

  /** Returns the number of rows added so far. */
  public int getRowCount() {
    return rows.size();
  }

  /** Returns true if no rows have been added. */
  public boolean isEmpty() {
    return rows.isEmpty();
  }

  /** Builds the final Page from all committed rows and resets the builder. */
  public Page build() {
    if (currentRow != null) {
      throw new IllegalStateException("endRow() must be called before build()");
    }
    Object[][] data = rows.toArray(new Object[0][]);
    rows.clear();
    return new RowPage(data, channelCount);
  }
}
