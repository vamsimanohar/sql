/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.exchange;

import org.opensearch.sql.planner.distributed.operator.SinkOperator;

/**
 * A sink operator that sends pages to a downstream compute stage. Implementations handle the
 * serialization and transport of data between stages (e.g., via OpenSearch transport, Arrow Flight,
 * or in-memory buffers for local exchanges).
 */
public interface ExchangeSinkOperator extends SinkOperator {

  /** Returns the ID of the downstream stage this sink sends data to. */
  String getTargetStageId();
}
