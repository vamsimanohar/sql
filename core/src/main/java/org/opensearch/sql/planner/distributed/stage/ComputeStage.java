/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.stage;

import java.util.Collections;
import java.util.List;
import org.opensearch.sql.planner.distributed.operator.OperatorFactory;
import org.opensearch.sql.planner.distributed.operator.SourceOperatorFactory;
import org.opensearch.sql.planner.distributed.split.Split;

/**
 * A portion of the distributed plan that runs as a pipeline on one or more nodes. Each ComputeStage
 * contains a pipeline of operators (source + transforms), an output partitioning scheme (how
 * results flow to the next stage), and metadata about dependencies and parallelism.
 *
 * <p>Naming follows the convention: "ComputeStage" (not "Fragment") — a unit of distributed
 * computation.
 */
public class ComputeStage {

  private final String stageId;
  private final SourceOperatorFactory sourceFactory;
  private final List<OperatorFactory> operatorFactories;
  private final PartitioningScheme outputPartitioning;
  private final List<String> sourceStageIds;
  private final List<Split> splits;
  private final long estimatedRows;
  private final long estimatedBytes;

  public ComputeStage(
      String stageId,
      SourceOperatorFactory sourceFactory,
      List<OperatorFactory> operatorFactories,
      PartitioningScheme outputPartitioning,
      List<String> sourceStageIds,
      List<Split> splits,
      long estimatedRows,
      long estimatedBytes) {
    this.stageId = stageId;
    this.sourceFactory = sourceFactory;
    this.operatorFactories = Collections.unmodifiableList(operatorFactories);
    this.outputPartitioning = outputPartitioning;
    this.sourceStageIds = Collections.unmodifiableList(sourceStageIds);
    this.splits = Collections.unmodifiableList(splits);
    this.estimatedRows = estimatedRows;
    this.estimatedBytes = estimatedBytes;
  }

  public String getStageId() {
    return stageId;
  }

  public SourceOperatorFactory getSourceFactory() {
    return sourceFactory;
  }

  /** Returns the ordered list of intermediate operator factories (after source). */
  public List<OperatorFactory> getOperatorFactories() {
    return operatorFactories;
  }

  /** Returns how this stage's output is partitioned for the downstream stage. */
  public PartitioningScheme getOutputPartitioning() {
    return outputPartitioning;
  }

  /** Returns the IDs of upstream stages that feed data into this stage. */
  public List<String> getSourceStageIds() {
    return sourceStageIds;
  }

  /** Returns the splits assigned to this stage (for source stages with shard assignments). */
  public List<Split> getSplits() {
    return splits;
  }

  /** Returns the estimated row count for this stage's output. */
  public long getEstimatedRows() {
    return estimatedRows;
  }

  /** Returns the estimated byte size for this stage's output. */
  public long getEstimatedBytes() {
    return estimatedBytes;
  }

  /** Returns true if this is a leaf stage (no upstream dependencies). */
  public boolean isLeaf() {
    return sourceStageIds.isEmpty();
  }

  /** Returns the total operator count (source + intermediates). */
  public int getOperatorCount() {
    return 1 + operatorFactories.size();
  }

  @Override
  public String toString() {
    return "ComputeStage{"
        + "id='"
        + stageId
        + "', operators="
        + getOperatorCount()
        + ", exchange="
        + outputPartitioning.getExchangeType()
        + ", splits="
        + splits.size()
        + ", deps="
        + sourceStageIds
        + '}';
  }
}
