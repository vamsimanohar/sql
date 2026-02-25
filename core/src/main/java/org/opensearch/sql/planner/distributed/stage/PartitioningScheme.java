/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.stage;

import java.util.Collections;
import java.util.List;

/** Describes how a stage's output data is partitioned across nodes. */
public class PartitioningScheme {

  private final ExchangeType exchangeType;
  private final List<Integer> hashChannels;

  private PartitioningScheme(ExchangeType exchangeType, List<Integer> hashChannels) {
    this.exchangeType = exchangeType;
    this.hashChannels = Collections.unmodifiableList(hashChannels);
  }

  /** Creates a GATHER partitioning (all data to coordinator). */
  public static PartitioningScheme gather() {
    return new PartitioningScheme(ExchangeType.GATHER, List.of());
  }

  /** Creates a HASH_REPARTITION partitioning on the given column indices. */
  public static PartitioningScheme hashRepartition(List<Integer> hashChannels) {
    return new PartitioningScheme(ExchangeType.HASH_REPARTITION, hashChannels);
  }

  /** Creates a BROADCAST partitioning (all data to all nodes). */
  public static PartitioningScheme broadcast() {
    return new PartitioningScheme(ExchangeType.BROADCAST, List.of());
  }

  /** Creates a NONE partitioning (no exchange). */
  public static PartitioningScheme none() {
    return new PartitioningScheme(ExchangeType.NONE, List.of());
  }

  public ExchangeType getExchangeType() {
    return exchangeType;
  }

  /** Returns the column indices used for hash partitioning. Empty for non-hash schemes. */
  public List<Integer> getHashChannels() {
    return hashChannels;
  }
}
