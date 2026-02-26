/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.sql.opensearch.executor.distributed.operator.FilterToLuceneConverter;
import org.opensearch.sql.opensearch.executor.distributed.operator.LimitOperator;
import org.opensearch.sql.opensearch.executor.distributed.operator.LuceneScanOperator;
import org.opensearch.sql.opensearch.executor.distributed.operator.ProjectionOperator;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.FilterPhysicalOperator;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.LimitPhysicalOperator;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.PhysicalOperatorTree;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.ProjectionPhysicalOperator;
import org.opensearch.sql.opensearch.executor.distributed.planner.physical.ScanPhysicalOperator;
import org.opensearch.sql.planner.distributed.operator.Operator;
import org.opensearch.sql.planner.distributed.operator.OperatorContext;
import org.opensearch.sql.planner.distributed.stage.ComputeStage;

/**
 * Dynamically builds operator pipelines from ComputeStage plan fragments.
 *
 * <p>Replaces the hardcoded LuceneScan → Limit → Collect pipeline construction in
 * OperatorPipelineExecutor with dynamic assembly based on the physical operators stored in the
 * stage.
 *
 * <p>For Phase 1B, builds pipelines from PhysicalOperatorTree containing:
 *
 * <ul>
 *   <li>ScanPhysicalOperator → LuceneScanOperator (with filter pushdown)
 *   <li>ProjectionPhysicalOperator → ProjectionOperator
 *   <li>LimitPhysicalOperator → LimitOperator
 * </ul>
 */
@Log4j2
public class DynamicPipelineBuilder {

  /**
   * Builds an operator pipeline for the given compute stage.
   *
   * @param stage the compute stage containing physical operators
   * @param executionContext execution context containing IndexShard, field mappings, etc.
   * @return ordered list of operators forming the pipeline
   */
  public static List<Operator> buildPipeline(
      ComputeStage stage, ExecutionContext executionContext) {
    log.debug("Building pipeline for stage {}", stage.getStageId());

    // For Phase 1B, we extract the PhysicalOperatorTree from the stage
    // In the future, this might come from the RelNode planFragment
    PhysicalOperatorTree operatorTree = extractOperatorTree(stage, executionContext);

    if (operatorTree == null) {
      throw new IllegalStateException(
          "Stage " + stage.getStageId() + " has no physical operator information");
    }

    return buildPipelineFromOperatorTree(operatorTree, executionContext);
  }

  /**
   * Extracts the PhysicalOperatorTree from the stage. For Phase 1B, this is passed through the
   * execution context. Future phases might reconstruct from RelNode planFragment.
   */
  private static PhysicalOperatorTree extractOperatorTree(
      ComputeStage stage, ExecutionContext executionContext) {

    // Phase 1B: Get operator tree from execution context
    // This is set by the coordinator when dispatching tasks
    return executionContext.getPhysicalOperatorTree();
  }

  /** Builds the operator pipeline from the physical operator tree. */
  private static List<Operator> buildPipelineFromOperatorTree(
      PhysicalOperatorTree operatorTree, ExecutionContext executionContext) {

    List<Operator> pipeline = new ArrayList<>();

    // Get physical operators
    ScanPhysicalOperator scanOp = operatorTree.getScanOperator();
    List<FilterPhysicalOperator> filterOps = operatorTree.getFilterOperators();
    List<ProjectionPhysicalOperator> projectionOps = operatorTree.getProjectionOperators();
    List<LimitPhysicalOperator> limitOps = operatorTree.getLimitOperators();

    // Build scan operator with pushed-down filters
    LuceneScanOperator luceneScanOp = buildLuceneScanOperator(scanOp, filterOps, executionContext);
    pipeline.add(luceneScanOp);

    // Add projection operator if needed
    if (!projectionOps.isEmpty()) {
      ProjectionOperator projectionOperator =
          buildProjectionOperator(projectionOps.get(0), scanOp.getFieldNames(), executionContext);
      pipeline.add(projectionOperator);
    }

    // Add limit operator if needed
    if (!limitOps.isEmpty()) {
      LimitOperator limitOperator = buildLimitOperator(limitOps.get(0), executionContext);
      pipeline.add(limitOperator);
    }

    log.debug(
        "Built pipeline with {} operators: {}",
        pipeline.size(),
        pipeline.stream()
            .map(op -> op.getClass().getSimpleName())
            .reduce((a, b) -> a + " → " + b)
            .orElse("empty"));

    return pipeline;
  }

  /** Builds LuceneScanOperator with filter pushdown. */
  private static LuceneScanOperator buildLuceneScanOperator(
      ScanPhysicalOperator scanOp,
      List<FilterPhysicalOperator> filterOps,
      ExecutionContext executionContext) {

    IndexShard indexShard = executionContext.getIndexShard();
    List<String> fieldNames = scanOp.getFieldNames();
    int batchSize = executionContext.getBatchSize();
    OperatorContext operatorContext =
        OperatorContext.createDefault("scan-" + executionContext.getStageId());

    // Build Lucene query from filter operators
    Query luceneQuery = buildLuceneQuery(filterOps, executionContext);

    log.debug(
        "Created LuceneScanOperator: index={}, fields={}, hasFilter={}",
        scanOp.getIndexName(),
        fieldNames,
        luceneQuery != null);

    return new LuceneScanOperator(indexShard, fieldNames, batchSize, operatorContext, luceneQuery);
  }

  /** Builds Lucene Query from filter physical operators. */
  private static Query buildLuceneQuery(
      List<FilterPhysicalOperator> filterOps, ExecutionContext executionContext) {

    if (filterOps.isEmpty()) {
      return new MatchAllDocsQuery(); // No filters - match all documents
    }

    // Phase 1B: Convert RexNode conditions to legacy filter condition format
    // This reuses the existing FilterToLuceneConverter
    List<Map<String, Object>> filterConditions = new ArrayList<>();

    for (FilterPhysicalOperator filterOp : filterOps) {
      // Convert RexNode to legacy filter condition format
      // This is a simplified conversion - future phases should improve this
      Map<String, Object> condition = convertRexNodeToFilterCondition(filterOp.getCondition());
      if (condition != null) {
        filterConditions.add(condition);
      }
    }

    if (filterConditions.isEmpty()) {
      return new MatchAllDocsQuery();
    }

    // Use existing FilterToLuceneConverter static method
    return FilterToLuceneConverter.convert(filterConditions, executionContext.getMapperService());
  }

  /**
   * Converts RexNode condition to legacy filter condition format. This is a simplified
   * implementation for Phase 1B.
   */
  private static Map<String, Object> convertRexNodeToFilterCondition(
      org.apache.calcite.rex.RexNode condition) {

    // Phase 1B: Simplified conversion
    // Future phases should implement proper RexNode → filter condition conversion

    log.warn(
        "RexNode to filter condition conversion not fully implemented. "
            + "Using match-all for condition: {}",
        condition);

    // For now, return null to indicate no filter conversion
    // This means filters won't be pushed down in Phase 1B
    // The existing RelNodeAnalyzer-based flow will continue to handle filter pushdown
    return null;
  }

  /** Builds ProjectionOperator. */
  private static ProjectionOperator buildProjectionOperator(
      ProjectionPhysicalOperator projectionOp,
      List<String> inputFieldNames,
      ExecutionContext executionContext) {

    List<String> projectedFields = projectionOp.getProjectedFields();
    OperatorContext operatorContext =
        OperatorContext.createDefault("project-" + executionContext.getStageId());

    log.debug("Created ProjectionOperator: projectedFields={}", projectedFields);

    return new ProjectionOperator(projectedFields, inputFieldNames, operatorContext);
  }

  /** Builds LimitOperator. */
  private static LimitOperator buildLimitOperator(
      LimitPhysicalOperator limitOp, ExecutionContext executionContext) {

    int limit = limitOp.getLimit();
    OperatorContext operatorContext =
        OperatorContext.createDefault("limit-" + executionContext.getStageId());

    log.debug("Created LimitOperator: limit={}", limit);

    return new LimitOperator(limit, operatorContext);
  }

  /** Execution context containing resources needed for operator creation. */
  public static class ExecutionContext {
    private final String stageId;
    private final IndexShard indexShard;
    private final org.opensearch.index.mapper.MapperService mapperService;
    private final int batchSize;
    private PhysicalOperatorTree physicalOperatorTree;

    public ExecutionContext(
        String stageId,
        IndexShard indexShard,
        org.opensearch.index.mapper.MapperService mapperService,
        int batchSize) {
      this.stageId = stageId;
      this.indexShard = indexShard;
      this.mapperService = mapperService;
      this.batchSize = batchSize;
    }

    public String getStageId() {
      return stageId;
    }

    public IndexShard getIndexShard() {
      return indexShard;
    }

    public org.opensearch.index.mapper.MapperService getMapperService() {
      return mapperService;
    }

    public int getBatchSize() {
      return batchSize;
    }

    public PhysicalOperatorTree getPhysicalOperatorTree() {
      return physicalOperatorTree;
    }

    public void setPhysicalOperatorTree(PhysicalOperatorTree tree) {
      this.physicalOperatorTree = tree;
    }
  }
}
