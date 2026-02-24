/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.pipeline;

import java.util.Collections;
import java.util.List;
import org.opensearch.sql.planner.distributed.operator.OperatorFactory;
import org.opensearch.sql.planner.distributed.operator.SourceOperatorFactory;

/**
 * An ordered chain of operator factories that defines the processing logic for a compute stage. The
 * first element is a {@link SourceOperatorFactory} (reads from storage or exchange), followed by
 * zero or more intermediate {@link OperatorFactory} instances (filter, project, aggregate, etc.).
 */
public class Pipeline {

  private final String pipelineId;
  private final SourceOperatorFactory sourceFactory;
  private final List<OperatorFactory> operatorFactories;

  /**
   * Creates a pipeline.
   *
   * @param pipelineId unique identifier
   * @param sourceFactory the source operator factory (first in chain)
   * @param operatorFactories ordered list of intermediate operator factories
   */
  public Pipeline(
      String pipelineId,
      SourceOperatorFactory sourceFactory,
      List<OperatorFactory> operatorFactories) {
    this.pipelineId = pipelineId;
    this.sourceFactory = sourceFactory;
    this.operatorFactories = Collections.unmodifiableList(operatorFactories);
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public SourceOperatorFactory getSourceFactory() {
    return sourceFactory;
  }

  public List<OperatorFactory> getOperatorFactories() {
    return operatorFactories;
  }

  /** Returns the total number of operators (source + intermediates). */
  public int getOperatorCount() {
    return 1 + operatorFactories.size();
  }
}
