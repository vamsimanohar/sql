/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.operator;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.planner.distributed.page.Page;

/**
 * Collects pages from the operator pipeline into a list of rows. Used on data nodes to gather
 * pipeline output before serializing into transport response.
 */
public class ResultCollector {

  private final List<String> fieldNames;
  private final List<List<Object>> rows;

  public ResultCollector(List<String> fieldNames) {
    this.fieldNames = fieldNames;
    this.rows = new ArrayList<>();
  }

  /** Extracts rows from a page and adds them to the collected results. */
  public void addPage(Page page) {
    if (page == null) {
      return;
    }
    int channelCount = page.getChannelCount();
    for (int pos = 0; pos < page.getPositionCount(); pos++) {
      List<Object> row = new ArrayList<>(channelCount);
      for (int ch = 0; ch < channelCount; ch++) {
        row.add(page.getValue(pos, ch));
      }
      rows.add(row);
    }
  }

  /** Returns the field names for the collected data. */
  public List<String> getFieldNames() {
    return fieldNames;
  }

  /** Returns all collected rows. */
  public List<List<Object>> getRows() {
    return rows;
  }
}
