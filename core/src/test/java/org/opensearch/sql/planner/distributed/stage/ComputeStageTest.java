/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ComputeStageTest {

  @Test
  void should_create_leaf_stage_with_data_units() {
    DataUnit du1 = new TestDataUnit("accounts/0", List.of("node-1", "node-2"), 50000L);
    DataUnit du2 = new TestDataUnit("accounts/1", List.of("node-2", "node-3"), 45000L);

    ComputeStage stage =
        new ComputeStage(
            "stage-0", PartitioningScheme.gather(), List.of(), List.of(du1, du2), 95000L, 0L);

    assertEquals("stage-0", stage.getStageId());
    assertTrue(stage.isLeaf());
    assertEquals(2, stage.getDataUnits().size());
    assertEquals(ExchangeType.GATHER, stage.getOutputPartitioning().getExchangeType());
    assertEquals(95000L, stage.getEstimatedRows());
  }

  @Test
  void should_create_non_leaf_stage_with_dependencies() {
    ComputeStage stage =
        new ComputeStage(
            "stage-1", PartitioningScheme.none(), List.of("stage-0"), List.of(), 0L, 0L);

    assertFalse(stage.isLeaf());
    assertEquals(List.of("stage-0"), stage.getSourceStageIds());
  }

  @Test
  void should_create_staged_plan() {
    ComputeStage scan =
        new ComputeStage(
            "scan",
            PartitioningScheme.gather(),
            List.of(),
            List.of(new TestDataUnit("idx/0", List.of("n1"), 1000L)),
            1000L,
            0L);

    ComputeStage merge =
        new ComputeStage("merge", PartitioningScheme.none(), List.of("scan"), List.of(), 1000L, 0L);

    StagedPlan plan = new StagedPlan("plan-1", List.of(scan, merge));

    assertEquals("plan-1", plan.getPlanId());
    assertEquals(2, plan.getStageCount());
    assertEquals("merge", plan.getRootStage().getStageId());
    assertEquals(1, plan.getLeafStages().size());
    assertEquals("scan", plan.getLeafStages().get(0).getStageId());
  }

  @Test
  void should_validate_staged_plan() {
    StagedPlan validPlan =
        new StagedPlan(
            "p1",
            List.of(
                new ComputeStage("s1", PartitioningScheme.gather(), List.of(), List.of(), 0L, 0L)));

    assertTrue(validPlan.validate().isEmpty());
  }

  @Test
  void should_detect_invalid_plan() {
    // Null plan ID
    StagedPlan nullId = new StagedPlan(null, List.of());
    assertFalse(nullId.validate().isEmpty());

    // Empty stages
    StagedPlan noStages = new StagedPlan("p1", List.of());
    assertFalse(noStages.validate().isEmpty());

    // Reference to non-existent stage
    StagedPlan badRef =
        new StagedPlan(
            "p1",
            List.of(
                new ComputeStage(
                    "s1", PartitioningScheme.none(), List.of("nonexistent"), List.of(), 0L, 0L)));
    assertFalse(badRef.validate().isEmpty());
  }

  @Test
  void should_lookup_stage_by_id() {
    ComputeStage s1 =
        new ComputeStage("s1", PartitioningScheme.gather(), List.of(), List.of(), 0L, 0L);
    StagedPlan plan = new StagedPlan("p1", List.of(s1));

    assertEquals("s1", plan.getStage("s1").getStageId());
    assertThrows(IllegalArgumentException.class, () -> plan.getStage("nonexistent"));
  }

  @Test
  void should_create_partitioning_schemes() {
    PartitioningScheme gather = PartitioningScheme.gather();
    assertEquals(ExchangeType.GATHER, gather.getExchangeType());
    assertTrue(gather.getHashChannels().isEmpty());

    PartitioningScheme hash = PartitioningScheme.hashRepartition(List.of(0, 1));
    assertEquals(ExchangeType.HASH_REPARTITION, hash.getExchangeType());
    assertEquals(List.of(0, 1), hash.getHashChannels());

    PartitioningScheme broadcast = PartitioningScheme.broadcast();
    assertEquals(ExchangeType.BROADCAST, broadcast.getExchangeType());

    PartitioningScheme none = PartitioningScheme.none();
    assertEquals(ExchangeType.NONE, none.getExchangeType());
  }

  /** Minimal test stub for DataUnit. */
  static class TestDataUnit extends DataUnit {
    private final String id;
    private final List<String> preferredNodes;
    private final long estimatedRows;

    TestDataUnit(String id, List<String> preferredNodes, long estimatedRows) {
      this.id = id;
      this.preferredNodes = preferredNodes;
      this.estimatedRows = estimatedRows;
    }

    @Override
    public String getDataUnitId() {
      return id;
    }

    @Override
    public List<String> getPreferredNodes() {
      return preferredNodes;
    }

    @Override
    public long getEstimatedRows() {
      return estimatedRows;
    }

    @Override
    public long getEstimatedSizeBytes() {
      return 0;
    }

    @Override
    public Map<String, String> getProperties() {
      return Collections.emptyMap();
    }
  }
}
