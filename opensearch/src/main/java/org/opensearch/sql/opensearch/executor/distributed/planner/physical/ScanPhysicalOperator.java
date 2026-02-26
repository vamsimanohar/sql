/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.planner.physical;

import java.util.List;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Physical operator representing a table scan operation.
 *
 * <p>Corresponds to Calcite TableScan RelNode. Contains index name and field names to read from
 * storage.
 */
@Getter
@RequiredArgsConstructor
public class ScanPhysicalOperator implements PhysicalOperatorNode {

  /** Index name to scan. */
  private final String indexName;

  /** Field names to read from the index. */
  private final List<String> fieldNames;

  @Override
  public PhysicalOperatorType getOperatorType() {
    return PhysicalOperatorType.SCAN;
  }

  @Override
  public String describe() {
    return String.format("Scan(index=%s, fields=[%s])", indexName, String.join(", ", fieldNames));
  }
}
