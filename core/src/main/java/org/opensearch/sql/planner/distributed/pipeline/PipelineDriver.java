/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.pipeline;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.planner.distributed.operator.Operator;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.operator.OperatorFactory;
import org.opensearch.sql.planner.distributed.operator.SourceOperator;
import org.opensearch.sql.planner.distributed.page.Page;
import org.opensearch.sql.planner.distributed.split.DataUnit;

/**
 * Executes a pipeline by driving data through a chain of operators. The driver implements a
 * pull/push loop: it pulls output from upstream operators and pushes it as input to downstream
 * operators.
 *
 * <p>Execution model:
 *
 * <ol>
 *   <li>Source operator produces pages from data units
 *   <li>Each intermediate operator transforms pages
 *   <li>The last operator (or sink) consumes the final output
 *   <li>When all operators are finished, the pipeline is complete
 * </ol>
 */
public class PipelineDriver {

  private static final Logger log = LogManager.getLogger(PipelineDriver.class);

  private final SourceOperator sourceOperator;
  private final List<Operator> operators;
  private final PipelineContext context;

  /**
   * Creates a PipelineDriver from a Pipeline definition.
   *
   * @param pipeline the pipeline to execute
   * @param operatorContext the context for creating operators
   * @param dataUnits the data units to assign to the source operator
   */
  public PipelineDriver(
      Pipeline pipeline, OperatorContext operatorContext, List<DataUnit> dataUnits) {
    this.context = new PipelineContext();

    // Create source operator
    this.sourceOperator = pipeline.getSourceFactory().createOperator(operatorContext);
    for (DataUnit dataUnit : dataUnits) {
      this.sourceOperator.addDataUnit(dataUnit);
    }
    this.sourceOperator.noMoreDataUnits();

    // Create intermediate operators
    this.operators = new ArrayList<>();
    for (OperatorFactory factory : pipeline.getOperatorFactories()) {
      this.operators.add(factory.createOperator(operatorContext));
    }
  }

  /**
   * Creates a PipelineDriver from pre-built operators (for testing).
   *
   * @param sourceOperator the source operator
   * @param operators the intermediate operators
   */
  public PipelineDriver(SourceOperator sourceOperator, List<Operator> operators) {
    this.context = new PipelineContext();
    this.sourceOperator = sourceOperator;
    this.operators = new ArrayList<>(operators);
  }

  /**
   * Runs the pipeline to completion. Drives data from source through all operators until all are
   * finished or cancellation is requested.
   *
   * @return the final output page from the last operator (may be null for sink pipelines)
   */
  public Page run() {
    context.setRunning();
    Page lastOutput = null;

    try {
      while (!isFinished() && !context.isCancelled()) {
        boolean madeProgress = processOnce();
        if (!madeProgress && !isFinished()) {
          // No progress and not finished — avoid busy-wait
          Thread.yield();
        }
      }

      // Collect any remaining output from the last operator
      if (!operators.isEmpty()) {
        Page output = operators.get(operators.size() - 1).getOutput();
        if (output != null) {
          lastOutput = output;
        }
      } else {
        Page output = sourceOperator.getOutput();
        if (output != null) {
          lastOutput = output;
        }
      }

      if (context.isCancelled()) {
        context.setCancelled();
      } else {
        context.setFinished();
      }
    } catch (Exception e) {
      context.setFailed(e.getMessage());
      throw new RuntimeException("Pipeline execution failed", e);
    } finally {
      closeAll();
    }

    return lastOutput;
  }

  /**
   * Processes one iteration of the pipeline loop. Returns true if any progress was made (data
   * moved).
   */
  boolean processOnce() {
    boolean madeProgress = false;

    // Drive source → first operator (or collect output if no intermediates)
    if (!sourceOperator.isFinished()) {
      Page sourcePage = sourceOperator.getOutput();
      if (sourcePage != null && sourcePage.getPositionCount() > 0) {
        if (!operators.isEmpty() && operators.get(0).needsInput()) {
          operators.get(0).addInput(sourcePage);
          madeProgress = true;
        }
      }
    } else if (!operators.isEmpty()) {
      // Source finished — signal finish to first operator
      Operator first = operators.get(0);
      if (!first.isFinished()) {
        first.finish();
        madeProgress = true;
      }
    }

    // Drive through intermediate operators: operator[i] → operator[i+1]
    for (int i = 0; i < operators.size() - 1; i++) {
      Operator current = operators.get(i);
      Operator next = operators.get(i + 1);

      Page output = current.getOutput();
      if (output != null && output.getPositionCount() > 0 && next.needsInput()) {
        next.addInput(output);
        madeProgress = true;
      }

      if (current.isFinished() && !next.isFinished()) {
        next.finish();
        madeProgress = true;
      }
    }

    // Drain the last operator's output so it can transition to finished.
    // Without this, operators that buffer pages (e.g., PassThroughOperator)
    // would never have getOutput() called, preventing isFinished() from
    // returning true.
    if (!operators.isEmpty()) {
      Operator last = operators.get(operators.size() - 1);
      Page output = last.getOutput();
      if (output != null) {
        madeProgress = true;
      }
    }

    return madeProgress;
  }

  /** Returns true if all operators have finished processing. */
  public boolean isFinished() {
    if (!operators.isEmpty()) {
      return operators.get(operators.size() - 1).isFinished();
    }
    return sourceOperator.isFinished();
  }

  /** Returns the pipeline execution context. */
  public PipelineContext getContext() {
    return context;
  }

  /** Closes all operators, releasing resources. */
  private void closeAll() {
    try {
      sourceOperator.close();
    } catch (Exception e) {
      log.warn("Error closing source operator", e);
    }
    for (Operator op : operators) {
      try {
        op.close();
      } catch (Exception e) {
        log.warn("Error closing operator", e);
      }
    }
  }
}
