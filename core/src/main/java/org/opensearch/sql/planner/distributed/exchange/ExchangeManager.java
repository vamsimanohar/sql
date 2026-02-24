/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.exchange;

import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.stage.PartitioningScheme;

/**
 * Manages the lifecycle of exchanges between compute stages. Creates exchange sink and source
 * operators for inter-stage data transfer.
 */
public interface ExchangeManager {

  /**
   * Creates an exchange sink operator for sending data from one stage to another.
   *
   * @param context the operator context
   * @param targetStageId the downstream stage receiving the data
   * @param partitioning how the output should be partitioned
   * @return the exchange sink operator
   */
  ExchangeSinkOperator createSink(
      OperatorContext context, String targetStageId, PartitioningScheme partitioning);

  /**
   * Creates an exchange source operator for receiving data from an upstream stage.
   *
   * @param context the operator context
   * @param sourceStageId the upstream stage sending the data
   * @return the exchange source operator
   */
  ExchangeSourceOperator createSource(OperatorContext context, String sourceStageId);
}
