/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.stage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The complete distributed execution plan as a tree of {@link ComputeStage}s. Created by the
 * physical planner from a Calcite RelNode tree. Stages are ordered by dependency — leaf stages
 * (scans) first, root stage (final merge) last.
 */
public class StagedPlan {

  private final String planId;
  private final List<ComputeStage> stages;

  public StagedPlan(String planId, List<ComputeStage> stages) {
    this.planId = planId;
    this.stages = Collections.unmodifiableList(stages);
  }

  public String getPlanId() {
    return planId;
  }

  /** Returns all stages in dependency order (leaves first, root last). */
  public List<ComputeStage> getStages() {
    return stages;
  }

  /** Returns the root stage (last in the list — typically the coordinator merge stage). */
  public ComputeStage getRootStage() {
    if (stages.isEmpty()) {
      throw new IllegalStateException("StagedPlan has no stages");
    }
    return stages.get(stages.size() - 1);
  }

  /** Returns leaf stages (stages with no upstream dependencies). */
  public List<ComputeStage> getLeafStages() {
    return stages.stream().filter(ComputeStage::isLeaf).collect(Collectors.toList());
  }

  /** Returns a stage by its ID. */
  public ComputeStage getStage(String stageId) {
    return stages.stream()
        .filter(s -> s.getStageId().equals(stageId))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("Stage not found: " + stageId));
  }

  /** Returns the total number of stages. */
  public int getStageCount() {
    return stages.size();
  }

  /**
   * Validates the plan. Returns a list of validation errors, or empty list if valid.
   *
   * @return list of error messages
   */
  public List<String> validate() {
    List<String> errors = new ArrayList<>();
    if (planId == null || planId.isEmpty()) {
      errors.add("Plan ID is required");
    }
    if (stages.isEmpty()) {
      errors.add("Plan must have at least one stage");
    }

    // Check that all referenced source stages exist
    Map<String, ComputeStage> stageMap =
        stages.stream().collect(Collectors.toMap(ComputeStage::getStageId, s -> s));
    for (ComputeStage stage : stages) {
      for (String depId : stage.getSourceStageIds()) {
        if (!stageMap.containsKey(depId)) {
          errors.add("Stage '" + stage.getStageId() + "' references unknown stage: " + depId);
        }
      }
    }

    return errors;
  }

  @Override
  public String toString() {
    return "StagedPlan{id='" + planId + "', stages=" + stages.size() + '}';
  }
}
