/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.stage;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;

/**
 * A portion of the distributed plan that runs as a pipeline on one or more nodes. Each ComputeStage
 * contains an output partitioning scheme (how results flow to the next stage), and metadata about
 * dependencies and parallelism.
 *
 * <p>Naming follows the convention: "ComputeStage" (not "Fragment") — a unit of distributed
 * computation.
 */
public class ComputeStage {

  private final String stageId;
  private final PartitioningScheme outputPartitioning;
  private final List<String> sourceStageIds;
  private final List<DataUnit> dataUnits;
  private final long estimatedRows;
  private final long estimatedBytes;
  private final RelNode planFragment;

  public ComputeStage(
      String stageId,
      PartitioningScheme outputPartitioning,
      List<String> sourceStageIds,
      List<DataUnit> dataUnits,
      long estimatedRows,
      long estimatedBytes) {
    this(
        stageId,
        outputPartitioning,
        sourceStageIds,
        dataUnits,
        estimatedRows,
        estimatedBytes,
        null);
  }

  public ComputeStage(
      String stageId,
      PartitioningScheme outputPartitioning,
      List<String> sourceStageIds,
      List<DataUnit> dataUnits,
      long estimatedRows,
      long estimatedBytes,
      RelNode planFragment) {
    this.stageId = stageId;
    this.outputPartitioning = outputPartitioning;
    this.sourceStageIds = Collections.unmodifiableList(sourceStageIds);
    this.dataUnits = Collections.unmodifiableList(dataUnits);
    this.estimatedRows = estimatedRows;
    this.estimatedBytes = estimatedBytes;
    this.planFragment = planFragment;
  }

  public String getStageId() {
    return stageId;
  }

  /** Returns how this stage's output is partitioned for the downstream stage. */
  public PartitioningScheme getOutputPartitioning() {
    return outputPartitioning;
  }

  /** Returns the IDs of upstream stages that feed data into this stage. */
  public List<String> getSourceStageIds() {
    return sourceStageIds;
  }

  /** Returns the data units assigned to this stage (for source stages with shard assignments). */
  public List<DataUnit> getDataUnits() {
    return dataUnits;
  }

  /** Returns the estimated row count for this stage's output. */
  public long getEstimatedRows() {
    return estimatedRows;
  }

  /** Returns the estimated byte size for this stage's output. */
  public long getEstimatedBytes() {
    return estimatedBytes;
  }

  /**
   * Returns the sub-plan (Calcite RelNode) for data node execution, or null if this stage does not
   * push down a plan fragment. Enables query pushdown: the data node can execute this sub-plan
   * locally instead of just scanning raw data.
   */
  public RelNode getPlanFragment() {
    return planFragment;
  }

  /** Returns true if this is a leaf stage (no upstream dependencies). */
  public boolean isLeaf() {
    return sourceStageIds.isEmpty();
  }

  @Override
  public String toString() {
    return "ComputeStage{"
        + "id='"
        + stageId
        + "', exchange="
        + outputPartitioning.getExchangeType()
        + ", dataUnits="
        + dataUnits.size()
        + ", deps="
        + sourceStageIds
        + '}';
  }
}
