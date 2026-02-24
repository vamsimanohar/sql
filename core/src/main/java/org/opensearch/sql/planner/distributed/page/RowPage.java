/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.page;

import java.util.Arrays;

/**
 * Simple row-based {@link Page} implementation. Each row is an Object array where the index
 * corresponds to the column (channel) position. This is the Phase 5A implementation; future phases
 * will add an Arrow-backed columnar implementation.
 */
public class RowPage implements Page {

  private final Object[][] rows;
  private final int channelCount;

  /**
   * Creates a RowPage from pre-built row data.
   *
   * @param rows 2D array where rows[i][j] is the value at row i, column j
   * @param channelCount the number of columns
   */
  public RowPage(Object[][] rows, int channelCount) {
    this.rows = rows;
    this.channelCount = channelCount;
  }

  @Override
  public int getPositionCount() {
    return rows.length;
  }

  @Override
  public int getChannelCount() {
    return channelCount;
  }

  @Override
  public Object getValue(int position, int channel) {
    if (position < 0 || position >= rows.length) {
      throw new IndexOutOfBoundsException(
          "Position " + position + " out of range [0, " + rows.length + ")");
    }
    if (channel < 0 || channel >= channelCount) {
      throw new IndexOutOfBoundsException(
          "Channel " + channel + " out of range [0, " + channelCount + ")");
    }
    return rows[position][channel];
  }

  @Override
  public Page getRegion(int positionOffset, int length) {
    if (positionOffset < 0 || positionOffset + length > rows.length) {
      throw new IndexOutOfBoundsException(
          "Region ["
              + positionOffset
              + ", "
              + (positionOffset + length)
              + ") out of range [0, "
              + rows.length
              + ")");
    }
    Object[][] region = Arrays.copyOfRange(rows, positionOffset, positionOffset + length);
    return new RowPage(region, channelCount);
  }
}
