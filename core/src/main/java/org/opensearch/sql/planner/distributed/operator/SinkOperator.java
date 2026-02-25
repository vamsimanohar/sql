/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.operator;

import org.opensearch.sql.planner.distributed.page.Page;

/**
 * A terminal operator that consumes pages without producing output. Sink operators collect results
 * (e.g., into a response buffer) or send data to downstream stages (e.g., exchange sinks).
 *
 * <p>Sink operators always need input (until finished) and never produce output via {@link
 * #getOutput()}.
 */
public interface SinkOperator extends Operator {

  /** Sink operators do not produce output pages. Always returns null. */
  @Override
  default Page getOutput() {
    return null;
  }
}
