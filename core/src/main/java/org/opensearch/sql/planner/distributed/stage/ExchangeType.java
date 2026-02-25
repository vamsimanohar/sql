/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.stage;

/** How data is exchanged between compute stages. */
public enum ExchangeType {
  /** All data flows to a single node (coordinator). Used for final merge. */
  GATHER,

  /** Data is repartitioned by hash key across nodes. Used for distributed joins and aggs. */
  HASH_REPARTITION,

  /** Data is sent to all downstream nodes. Used for broadcast joins (small table). */
  BROADCAST,

  /** No exchange — stage runs locally after the previous stage on the same node. */
  NONE
}
