/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.distributed.operator.Operator;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.operator.OperatorFactory;
import org.opensearch.sql.planner.distributed.operator.SourceOperator;
import org.opensearch.sql.planner.distributed.operator.SourceOperatorFactory;
import org.opensearch.sql.planner.distributed.page.Page;
import org.opensearch.sql.planner.distributed.split.Split;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class ComputeStageTest {

  @Test
  void should_create_leaf_stage_with_splits() {
    Split split1 = new Split("accounts", 0, List.of("node-1", "node-2"), 50000L);
    Split split2 = new Split("accounts", 1, List.of("node-2", "node-3"), 45000L);

    ComputeStage stage =
        new ComputeStage(
            "stage-0",
            new NoOpSourceFactory(),
            List.of(),
            PartitioningScheme.gather(),
            List.of(),
            List.of(split1, split2),
            95000L,
            0L);

    assertEquals("stage-0", stage.getStageId());
    assertTrue(stage.isLeaf());
    assertEquals(2, stage.getSplits().size());
    assertEquals(1, stage.getOperatorCount());
    assertEquals(ExchangeType.GATHER, stage.getOutputPartitioning().getExchangeType());
    assertEquals(95000L, stage.getEstimatedRows());
  }

  @Test
  void should_create_non_leaf_stage_with_dependencies() {
    ComputeStage stage =
        new ComputeStage(
            "stage-1",
            new NoOpSourceFactory(),
            List.of(new NoOpOperatorFactory()),
            PartitioningScheme.none(),
            List.of("stage-0"),
            List.of(),
            0L,
            0L);

    assertFalse(stage.isLeaf());
    assertEquals(List.of("stage-0"), stage.getSourceStageIds());
    assertEquals(2, stage.getOperatorCount());
  }

  @Test
  void should_create_staged_plan() {
    ComputeStage scan =
        new ComputeStage(
            "scan",
            new NoOpSourceFactory(),
            List.of(),
            PartitioningScheme.gather(),
            List.of(),
            List.of(new Split("idx", 0, List.of("n1"), 1000L)),
            1000L,
            0L);

    ComputeStage merge =
        new ComputeStage(
            "merge",
            new NoOpSourceFactory(),
            List.of(),
            PartitioningScheme.none(),
            List.of("scan"),
            List.of(),
            1000L,
            0L);

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
                new ComputeStage(
                    "s1",
                    new NoOpSourceFactory(),
                    List.of(),
                    PartitioningScheme.gather(),
                    List.of(),
                    List.of(),
                    0L,
                    0L)));

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
                    "s1",
                    new NoOpSourceFactory(),
                    List.of(),
                    PartitioningScheme.none(),
                    List.of("nonexistent"),
                    List.of(),
                    0L,
                    0L)));
    assertFalse(badRef.validate().isEmpty());
  }

  @Test
  void should_lookup_stage_by_id() {
    ComputeStage s1 =
        new ComputeStage(
            "s1",
            new NoOpSourceFactory(),
            List.of(),
            PartitioningScheme.gather(),
            List.of(),
            List.of(),
            0L,
            0L);
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

  /** No-op source factory for testing. */
  static class NoOpSourceFactory implements SourceOperatorFactory {
    @Override
    public SourceOperator createOperator(OperatorContext context) {
      return new SourceOperator() {
        @Override
        public void addSplit(Split split) {}

        @Override
        public void noMoreSplits() {}

        @Override
        public Page getOutput() {
          return null;
        }

        @Override
        public boolean isFinished() {
          return true;
        }

        @Override
        public void finish() {}

        @Override
        public OperatorContext getContext() {
          return context;
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public void noMoreOperators() {}
  }

  /** No-op operator factory for testing. */
  static class NoOpOperatorFactory implements OperatorFactory {
    @Override
    public Operator createOperator(OperatorContext context) {
      return new Operator() {
        @Override
        public boolean needsInput() {
          return false;
        }

        @Override
        public void addInput(Page page) {}

        @Override
        public Page getOutput() {
          return null;
        }

        @Override
        public boolean isFinished() {
          return true;
        }

        @Override
        public void finish() {}

        @Override
        public OperatorContext getContext() {
          return context;
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public void noMoreOperators() {}
  }
}
