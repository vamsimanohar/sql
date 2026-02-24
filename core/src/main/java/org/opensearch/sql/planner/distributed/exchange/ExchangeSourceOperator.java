/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.exchange;

import org.opensearch.sql.planner.distributed.operator.SourceOperator;

/**
 * A source operator that receives pages from an upstream compute stage. Implementations handle
 * deserialization and buffering of data received from upstream stages.
 */
public interface ExchangeSourceOperator extends SourceOperator {

  /** Returns the ID of the upstream stage this source receives data from. */
  String getSourceStageId();
}
