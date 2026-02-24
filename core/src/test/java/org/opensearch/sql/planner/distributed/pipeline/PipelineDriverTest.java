/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.distributed.operator.Operator;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.operator.SourceOperator;
import org.opensearch.sql.planner.distributed.page.Page;
import org.opensearch.sql.planner.distributed.page.PageBuilder;
import org.opensearch.sql.planner.distributed.split.Split;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PipelineDriverTest {

  @Test
  void should_run_source_only_pipeline() {
    // Given: A source that produces one page
    MockSourceOperator source = new MockSourceOperator(List.of(createTestPage(3, 2)));

    // When
    PipelineDriver driver = new PipelineDriver(source, List.of());
    Page result = driver.run();

    // Then
    assertTrue(driver.isFinished());
    assertEquals(PipelineContext.Status.FINISHED, driver.getContext().getStatus());
  }

  @Test
  void should_run_source_to_transform_pipeline() {
    // Given: Source produces a page, transform doubles column 1 values
    Page inputPage = createTestPage(3, 2);
    MockSourceOperator source = new MockSourceOperator(List.of(inputPage));
    PassThroughOperator passThrough = new PassThroughOperator();

    // When
    PipelineDriver driver = new PipelineDriver(source, List.of(passThrough));
    driver.run();

    // Then
    assertTrue(driver.isFinished());
    assertTrue(passThrough.receivedPages > 0);
  }

  @Test
  void should_run_source_to_sink_pipeline() {
    // Given: Source produces pages, sink collects them
    Page page1 = createTestPage(2, 2);
    Page page2 = createTestPage(3, 2);
    MockSourceOperator source = new MockSourceOperator(List.of(page1, page2));
    CollectingSinkOperator sink = new CollectingSinkOperator();

    // When
    PipelineDriver driver = new PipelineDriver(source, List.of(sink));
    driver.run();

    // Then
    assertTrue(driver.isFinished());
    assertEquals(2, sink.collectedPages.size());
    assertEquals(2, sink.collectedPages.get(0).getPositionCount());
    assertEquals(3, sink.collectedPages.get(1).getPositionCount());
  }

  @Test
  void should_chain_multiple_operators() {
    // Given: source → passthrough1 → passthrough2 → sink
    Page inputPage = createTestPage(5, 3);
    MockSourceOperator source = new MockSourceOperator(List.of(inputPage));
    PassThroughOperator pass1 = new PassThroughOperator();
    PassThroughOperator pass2 = new PassThroughOperator();
    CollectingSinkOperator sink = new CollectingSinkOperator();

    // When
    PipelineDriver driver = new PipelineDriver(source, List.of(pass1, pass2, sink));
    driver.run();

    // Then
    assertTrue(driver.isFinished());
    assertTrue(pass1.receivedPages > 0);
    assertTrue(pass2.receivedPages > 0);
    assertEquals(1, sink.collectedPages.size());
  }

  private Page createTestPage(int rows, int cols) {
    PageBuilder builder = new PageBuilder(cols);
    for (int r = 0; r < rows; r++) {
      builder.beginRow();
      for (int c = 0; c < cols; c++) {
        builder.setValue(c, "r" + r + "c" + c);
      }
      builder.endRow();
    }
    return builder.build();
  }

  /** Mock source operator that produces pre-built pages. */
  static class MockSourceOperator implements SourceOperator {
    private final List<Page> pages;
    private int index = 0;
    private boolean finished = false;

    MockSourceOperator(List<Page> pages) {
      this.pages = new ArrayList<>(pages);
    }

    @Override
    public void addSplit(Split split) {}

    @Override
    public void noMoreSplits() {}

    @Override
    public Page getOutput() {
      if (index < pages.size()) {
        return pages.get(index++);
      }
      finished = true;
      return null;
    }

    @Override
    public boolean isFinished() {
      return finished && index >= pages.size();
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public OperatorContext getContext() {
      return OperatorContext.createDefault("mock-source");
    }

    @Override
    public void close() {}
  }

  /** Pass-through operator that forwards pages unchanged. */
  static class PassThroughOperator implements Operator {
    private Page buffered;
    private boolean finished = false;
    int receivedPages = 0;

    @Override
    public boolean needsInput() {
      return buffered == null && !finished;
    }

    @Override
    public void addInput(Page page) {
      buffered = page;
      receivedPages++;
    }

    @Override
    public Page getOutput() {
      Page out = buffered;
      buffered = null;
      return out;
    }

    @Override
    public boolean isFinished() {
      return finished && buffered == null;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public OperatorContext getContext() {
      return OperatorContext.createDefault("passthrough");
    }

    @Override
    public void close() {}
  }

  /** Sink operator that collects all received pages. */
  static class CollectingSinkOperator implements Operator {
    final List<Page> collectedPages = new ArrayList<>();
    private boolean finished = false;

    @Override
    public boolean needsInput() {
      return !finished;
    }

    @Override
    public void addInput(Page page) {
      collectedPages.add(page);
    }

    @Override
    public Page getOutput() {
      return null;
    }

    @Override
    public boolean isFinished() {
      return finished;
    }

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public OperatorContext getContext() {
      return OperatorContext.createDefault("sink");
    }

    @Override
    public void close() {}
  }
}
