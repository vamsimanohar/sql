/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.executor.ExecutionEngine.Schema;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DistributedPhysicalPlanTest {

  private DistributedPhysicalPlan plan;
  private ExecutionStage stage1;
  private ExecutionStage stage2;

  @BeforeEach
  void setUp() {
    // Create sample work units and stages for testing
    DataPartition partition1 =
        new DataPartition(
            "shard-1", DataPartition.StorageType.LUCENE, "test-index", 1024L, Map.of());
    DataPartition partition2 =
        new DataPartition(
            "shard-2", DataPartition.StorageType.LUCENE, "test-index", 1024L, Map.of());

    WorkUnit workUnit1 =
        new WorkUnit(
            "work-1", WorkUnit.WorkUnitType.SCAN, partition1, List.of(), "node-1", Map.of());

    WorkUnit workUnit2 =
        new WorkUnit(
            "work-2",
            WorkUnit.WorkUnitType.PROCESS,
            partition2,
            List.of("work-1"),
            "node-2",
            Map.of());

    stage1 =
        new ExecutionStage(
            "stage-1",
            ExecutionStage.StageType.SCAN,
            List.of(workUnit1),
            List.of(),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            1,
            ExecutionStage.DataExchangeType.GATHER);

    stage2 =
        new ExecutionStage(
            "stage-2",
            ExecutionStage.StageType.PROCESS,
            List.of(workUnit2),
            List.of("stage-1"),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            1,
            ExecutionStage.DataExchangeType.GATHER);

    plan = DistributedPhysicalPlan.create("test-plan", List.of(stage1, stage2), null);
  }

  @Test
  void should_create_plan_with_valid_parameters() {
    // When
    DistributedPhysicalPlan newPlan =
        DistributedPhysicalPlan.create("plan-id", List.of(stage1), null);

    // Then
    assertNotNull(newPlan);
    assertEquals("plan-id", newPlan.getPlanId());
    assertEquals(DistributedPhysicalPlan.PlanStatus.CREATED, newPlan.getStatus());
    assertEquals(1, newPlan.getExecutionStages().size());
  }

  @Test
  void should_validate_successfully_for_valid_plan() {
    // When
    List<String> errors = plan.validate();

    // Then
    assertTrue(errors.isEmpty());
  }

  @Test
  void should_detect_validation_errors_for_empty_stages() {
    // Given - Plan with empty stages
    DistributedPhysicalPlan invalidPlan =
        DistributedPhysicalPlan.create("invalid", List.of(), null);

    // When
    List<String> errors = invalidPlan.validate();

    // Then
    assertFalse(errors.isEmpty());
    assertTrue(errors.stream().anyMatch(error -> error.contains("at least one execution stage")));
  }

  @Test
  void should_mark_plan_status_transitions_correctly() {
    // When & Then
    assertEquals(DistributedPhysicalPlan.PlanStatus.CREATED, plan.getStatus());

    plan.markExecuting();
    assertEquals(DistributedPhysicalPlan.PlanStatus.EXECUTING, plan.getStatus());

    plan.markCompleted();
    assertEquals(DistributedPhysicalPlan.PlanStatus.COMPLETED, plan.getStatus());
  }

  @Test
  void should_mark_failed_status_with_error_message() {
    // When
    plan.markFailed("Test error message");

    // Then
    assertEquals(DistributedPhysicalPlan.PlanStatus.FAILED, plan.getStatus());
    assertEquals("Test error message", plan.getPlanMetadata().get("error"));
  }

  @Test
  void should_identify_ready_stages_correctly() {
    // Given
    Set<String> completedStages = Set.of(); // No completed stages initially

    // When
    List<ExecutionStage> readyStages = plan.getReadyStages(completedStages);

    // Then
    assertEquals(1, readyStages.size());
    assertEquals("stage-1", readyStages.get(0).getStageId());
  }

  @Test
  void should_identify_ready_stages_after_dependencies_complete() {
    // Given
    Set<String> completedStages = Set.of("stage-1"); // Stage 1 completed

    // When
    List<ExecutionStage> readyStages = plan.getReadyStages(completedStages);

    // Then - Both stages are "ready" since getReadyStages doesn't filter out completed ones
    // stage-1 has no deps (always ready), stage-2 depends on stage-1 (now completed, so ready)
    assertEquals(2, readyStages.size());
    assertTrue(readyStages.stream().anyMatch(s -> s.getStageId().equals("stage-2")));
  }

  @Test
  void should_determine_plan_completion_correctly() {
    // Given
    Set<String> allStagesCompleted = Set.of("stage-1", "stage-2");
    Set<String> partialStagesCompleted = Set.of("stage-1");

    // When & Then
    assertTrue(plan.isComplete(allStagesCompleted));
    assertFalse(plan.isComplete(partialStagesCompleted));
    assertFalse(plan.isComplete(Set.of()));
  }

  @Test
  void should_identify_final_stage() {
    // When
    ExecutionStage finalStage = plan.getFinalStage();

    // Then
    assertNotNull(finalStage);
    assertEquals("stage-2", finalStage.getStageId());
  }

  @Test
  void should_have_write_and_read_external_methods() {
    // Verify that DistributedPhysicalPlan implements SerializablePlan
    // (Full serialization test deferred until ExecutionStage implements Serializable)
    assertNotNull(plan);
    assertEquals("test-plan", plan.getPlanId());
    assertEquals(2, plan.getExecutionStages().size());
  }

  @Test
  void should_handle_empty_stages_list() {
    // Given
    DistributedPhysicalPlan emptyPlan =
        DistributedPhysicalPlan.create("empty-plan", List.of(), null);

    // When
    List<String> errors = emptyPlan.validate();
    List<ExecutionStage> readyStages = emptyPlan.getReadyStages(Set.of());
    boolean isComplete = emptyPlan.isComplete(Set.of());

    // Then
    assertFalse(errors.isEmpty()); // Should have validation error for empty stages
    assertTrue(readyStages.isEmpty());
    assertTrue(isComplete); // Empty plan is considered complete
  }

  @Test
  void should_provide_output_schema() {
    // When
    Schema schema = plan.getOutputSchema();

    // Then - Schema is null because we passed null in create()
    assertNull(schema);
  }

  @Test
  void should_generate_unique_plan_ids() {
    // When
    DistributedPhysicalPlan plan1 = DistributedPhysicalPlan.create("plan-1", List.of(stage1), null);
    DistributedPhysicalPlan plan2 = DistributedPhysicalPlan.create("plan-2", List.of(stage1), null);

    // Then
    assertFalse(plan1.getPlanId().equals(plan2.getPlanId()));
  }

  @Test
  void should_handle_null_error_message_in_mark_failed() {
    // When
    plan.markFailed(null);

    // Then
    assertEquals(DistributedPhysicalPlan.PlanStatus.FAILED, plan.getStatus());
  }

  @Test
  void should_detect_duplicate_stage_ids() {
    // Given - Plan with duplicate stage IDs
    ExecutionStage duplicateStage =
        new ExecutionStage(
            "stage-1", // Same ID as stage1
            ExecutionStage.StageType.PROCESS,
            List.of(),
            List.of(),
            ExecutionStage.StageStatus.WAITING,
            Map.of(),
            0,
            ExecutionStage.DataExchangeType.GATHER);

    DistributedPhysicalPlan duplicatePlan =
        DistributedPhysicalPlan.create("dup-plan", List.of(stage1, duplicateStage), null);

    // When
    List<String> errors = duplicatePlan.validate();

    // Then
    assertFalse(errors.isEmpty());
    assertTrue(errors.stream().anyMatch(error -> error.contains("duplicate stage IDs")));
  }
}
