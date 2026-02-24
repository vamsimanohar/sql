/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * In-memory Calcite ScannableTable that wraps pre-fetched rows from distributed data node scans.
 * Used by coordinator-side Calcite execution to replace OpenSearch-backed TableScan nodes with
 * in-memory data.
 */
public class InMemoryScannableTable extends AbstractTable implements ScannableTable {
  private final RelDataType rowType;
  private final List<Object[]> rows;

  public InMemoryScannableTable(RelDataType rowType, List<Object[]> rows) {
    this.rowType = rowType;
    this.rows = rows;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return Linq4j.asEnumerable(rows);
  }
}
