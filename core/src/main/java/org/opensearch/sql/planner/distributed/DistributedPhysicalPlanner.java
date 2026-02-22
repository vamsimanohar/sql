/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.storage.Table;

/**
 * Converts LogicalPlan trees into multi-stage distributed execution plans.
 *
 * <p>The distributed physical planner analyzes PPL logical plans and breaks them into
 * stages that can be executed across multiple nodes in parallel:
 *
 * <ul>
 *   <li><strong>Stage 1 (SCAN)</strong>: Direct shard access with filters and projections</li>
 *   <li><strong>Stage 2 (PROCESS)</strong>: Partial aggregations on each node</li>
 *   <li><strong>Stage 3 (FINALIZE)</strong>: Global aggregation on coordinator</li>
 * </ul>
 *
 * <p><strong>Phase 1 Support:</strong> Simple aggregation queries with filters
 * <pre>
 * search source=logs-* | where status >= 400 | stats count(), avg(latency) by service
 * </pre>
 *
 * <p><strong>Future Phases:</strong> Complex operations (joins, window functions, etc.)
 */
@Log4j2
@RequiredArgsConstructor
public class DistributedPhysicalPlanner {

  /** Interface for discovering data partitions in tables */
  public interface PartitionDiscovery {
    /**
     * Discovers data partitions for a given table.
     *
     * @param table Table to discover partitions for
     * @return List of data partitions (shards, files, etc.)
     */
    List<DataPartition> discoverPartitions(Table table);
  }

  private final PartitionDiscovery partitionDiscovery;

  /**
   * Converts a logical plan into a distributed physical plan.
   *
   * @param logicalPlan The logical plan to convert
   * @return Multi-stage distributed execution plan
   */
  public DistributedPhysicalPlan plan(LogicalPlan logicalPlan) {
    String planId = "distributed-plan-" + UUID.randomUUID().toString().substring(0, 8);
    log.info("Creating distributed physical plan: {}", planId);

    try {
      // Analyze the logical plan to determine distributed execution strategy
      PlanAnalysis analysis = analyzeLogicalPlan(logicalPlan);

      if (!analysis.isDistributable()) {
        log.debug("Plan not suitable for distributed execution: {}", analysis.getReason());
        throw new UnsupportedOperationException(
            "Plan not suitable for distributed execution: " + analysis.getReason());
      }

      // Create execution stages based on plan analysis
      List<ExecutionStage> stages = createExecutionStages(analysis);

      // Build the final distributed plan
      DistributedPhysicalPlan distributedPlan = DistributedPhysicalPlan.create(
          planId, stages, analysis.getOutputSchema());

      log.info("Created distributed plan {} with {} stages", planId, stages.size());
      return distributedPlan;

    } catch (Exception e) {
      log.error("Failed to create distributed physical plan for: {}", logicalPlan, e);
      throw new RuntimeException("Failed to create distributed physical plan", e);
    }
  }

  /**
   * Analyzes the logical plan to determine distributed execution feasibility.
   */
  private PlanAnalysis analyzeLogicalPlan(LogicalPlan logicalPlan) {
    PlanAnalyzer analyzer = new PlanAnalyzer();
    return logicalPlan.accept(analyzer, new AnalysisContext());
  }

  /**
   * Creates execution stages from the plan analysis.
   */
  private List<ExecutionStage> createExecutionStages(PlanAnalysis analysis) {
    List<ExecutionStage> stages = new ArrayList<>();

    // Stage 1: Distributed scanning with filters and projections
    ExecutionStage scanStage = createScanStage(analysis);
    stages.add(scanStage);

    // Stage 2: Partial aggregation (if needed)
    if (analysis.hasAggregation()) {
      ExecutionStage processStage = createPartialAggregationStage(analysis, scanStage.getStageId());
      stages.add(processStage);

      // Stage 3: Final aggregation
      ExecutionStage finalStage = createFinalAggregationStage(analysis, processStage.getStageId());
      stages.add(finalStage);
    } else {
      // No aggregation - add finalize stage for result collection
      ExecutionStage finalStage = createResultCollectionStage(analysis, scanStage.getStageId());
      stages.add(finalStage);
    }

    return stages;
  }

  /**
   * Creates Stage 1: Distributed scanning with filters and projections.
   */
  private ExecutionStage createScanStage(PlanAnalysis analysis) {
    String stageId = "scan-stage-" + UUID.randomUUID().toString().substring(0, 8);

    // Discover partitions for the target table
    List<DataPartition> partitions = partitionDiscovery.discoverPartitions(analysis.getTable());

    // Create work units for each partition (shard)
    List<WorkUnit> workUnits = partitions.stream()
        .map(partition -> createScanWorkUnit(partition, analysis))
        .collect(Collectors.toList());

    log.debug("Created scan stage {} with {} work units", stageId, workUnits.size());

    return ExecutionStage.createScanStage(stageId, workUnits);
  }

  /**
   * Creates a scan work unit for a specific partition.
   */
  private WorkUnit createScanWorkUnit(DataPartition partition, PlanAnalysis analysis) {
    String workUnitId = "scan-" + partition.getPartitionId();

    // Create scan operator with filter and projection
    TaskOperator scanOperator = new LuceneScanOperator(
        workUnitId + "-op",
        TaskOperator.OperatorType.SCAN,
        Map.of(
            "filters", analysis.getFilterConditions(),
            "projections", analysis.getProjections(),
            "indexName", partition.getIndexName(),
            "shardId", partition.getShardId()));

    return WorkUnit.createScanUnit(
        workUnitId,
        partition,
        scanOperator,
        partition.getNodeId());
  }

  /**
   * Creates Stage 2: Partial aggregation processing.
   */
  private ExecutionStage createPartialAggregationStage(PlanAnalysis analysis, String scanStageId) {
    String stageId = "partial-agg-stage-" + UUID.randomUUID().toString().substring(0, 8);

    // Create work units for partial aggregation (one per node that has data)
    // For Phase 1, we'll create a simple work unit per expected node
    List<WorkUnit> workUnits = IntStream.range(0, 3) // Assume 3 data nodes for now
        .mapToObj(i -> createPartialAggregationWorkUnit(i, analysis, scanStageId))
        .collect(Collectors.toList());

    log.debug("Created partial aggregation stage {} with {} work units", stageId, workUnits.size());

    return ExecutionStage.createProcessStage(
        stageId,
        workUnits,
        List.of(scanStageId),
        ExecutionStage.DataExchangeType.NONE);
  }

  /**
   * Creates a partial aggregation work unit.
   */
  private WorkUnit createPartialAggregationWorkUnit(int nodeIndex, PlanAnalysis analysis, String scanStageId) {
    String workUnitId = "partial-agg-" + nodeIndex;

    TaskOperator aggregationOperator = new PartialAggregationOperator(
        workUnitId + "-op",
        TaskOperator.OperatorType.PARTIAL_AGGREGATE,
        Map.of(
            "groupByFields", analysis.getGroupByFields(),
            "aggregations", analysis.getAggregations(),
            "nodeIndex", nodeIndex));

    return WorkUnit.createProcessUnit(
        workUnitId,
        aggregationOperator,
        List.of(scanStageId));
  }

  /**
   * Creates Stage 3: Final aggregation.
   */
  private ExecutionStage createFinalAggregationStage(PlanAnalysis analysis, String processStageId) {
    String stageId = "final-agg-stage-" + UUID.randomUUID().toString().substring(0, 8);
    String workUnitId = "final-agg";

    TaskOperator finalOperator = new FinalAggregationOperator(
        workUnitId + "-op",
        TaskOperator.OperatorType.FINAL_AGGREGATE,
        Map.of(
            "groupByFields", analysis.getGroupByFields(),
            "aggregations", analysis.getAggregations()));

    WorkUnit finalWorkUnit = WorkUnit.createFinalizeUnit(
        workUnitId,
        finalOperator,
        List.of(processStageId));

    log.debug("Created final aggregation stage {}", stageId);

    return ExecutionStage.createFinalizeStage(stageId, finalWorkUnit, List.of(processStageId));
  }

  /**
   * Creates a result collection stage for non-aggregation queries.
   */
  private ExecutionStage createResultCollectionStage(PlanAnalysis analysis, String scanStageId) {
    String stageId = "collect-stage-" + UUID.randomUUID().toString().substring(0, 8);
    String workUnitId = "collect-results";

    TaskOperator collectOperator = new ResultCollectionOperator(
        workUnitId + "-op",
        TaskOperator.OperatorType.FINALIZE,
        Map.of(
            "limit", analysis.getLimit(),
            "sortFields", analysis.getSortFields()));

    WorkUnit collectWorkUnit = WorkUnit.createFinalizeUnit(
        workUnitId,
        collectOperator,
        List.of(scanStageId));

    log.debug("Created result collection stage {}", stageId);

    return ExecutionStage.createFinalizeStage(stageId, collectWorkUnit, List.of(scanStageId));
  }

  /**
   * Visitor that analyzes logical plans to determine distributed execution strategy.
   */
  private static class PlanAnalyzer extends LogicalPlanNodeVisitor<PlanAnalysis, AnalysisContext> {

    @Override
    public PlanAnalysis visitRelation(LogicalRelation plan, AnalysisContext context) {
      context.setTable(plan.getTable());
      context.setRelationName(plan.getRelationName());
      return visitChildren(plan, context);
    }

    @Override
    public PlanAnalysis visitFilter(LogicalFilter plan, AnalysisContext context) {
      context.addFilterCondition(plan.getCondition().toString());
      return visitChildren(plan, context);
    }

    @Override
    public PlanAnalysis visitProject(LogicalProject plan, AnalysisContext context) {
      plan.getProjectList().forEach(expr ->
          context.addProjection(expr.getNameOrAlias(), expr.toString()));
      return visitChildren(plan, context);
    }

    @Override
    public PlanAnalysis visitAggregation(LogicalAggregation plan, AnalysisContext context) {
      context.setHasAggregation(true);

      // Extract group by fields
      plan.getGroupByList().forEach(expr ->
          context.addGroupByField(expr.getNameOrAlias()));

      // Extract aggregation functions
      plan.getAggregatorList().forEach(agg ->
          context.addAggregation(agg.getName(), agg.toString()));

      return visitChildren(plan, context);
    }

    @Override
    public PlanAnalysis visitSort(LogicalSort plan, AnalysisContext context) {
      plan.getSortList().forEach(sort ->
          context.addSortField(sort.toString()));
      return visitChildren(plan, context);
    }

    @Override
    public PlanAnalysis visitLimit(LogicalLimit plan, AnalysisContext context) {
      context.setLimit(plan.getLimit());
      return visitChildren(plan, context);
    }

    @Override
    public PlanAnalysis visitNode(LogicalPlan plan, AnalysisContext context) {
      return visitChildren(plan, context);
    }

    private PlanAnalysis visitChildren(LogicalPlan plan, AnalysisContext context) {
      // Visit child plans
      for (LogicalPlan child : plan.getChild()) {
        child.accept(this, context);
      }

      // Build analysis result
      return buildAnalysis(context);
    }

    private PlanAnalysis buildAnalysis(AnalysisContext context) {
      PlanAnalysis analysis = new PlanAnalysis();
      analysis.setTable(context.getTable());
      analysis.setRelationName(context.getRelationName());
      analysis.setFilterConditions(context.getFilterConditions());
      analysis.setProjections(context.getProjections());
      analysis.setHasAggregation(context.isHasAggregation());
      analysis.setGroupByFields(context.getGroupByFields());
      analysis.setAggregations(context.getAggregations());
      analysis.setSortFields(context.getSortFields());
      analysis.setLimit(context.getLimit());

      // Determine if plan is distributable
      boolean distributable = context.getTable() != null;
      String reason = distributable ? null : "No table found in plan";

      analysis.setDistributable(distributable);
      analysis.setReason(reason);

      // Create output schema (simplified for Phase 1)
      Schema outputSchema = createOutputSchema(context);
      analysis.setOutputSchema(outputSchema);

      return analysis;
    }

    private Schema createOutputSchema(AnalysisContext context) {
      List<Column> columns = new ArrayList<>();

      if (context.isHasAggregation()) {
        // Add group by columns
        context.getGroupByFields().forEach(field ->
            columns.add(new Column(field, null, null))); // Type will be determined at runtime

        // Add aggregation columns
        context.getAggregations().forEach((name, func) ->
            columns.add(new Column(name, null, null)));
      } else {
        // Add projection columns or all fields
        if (context.getProjections().isEmpty()) {
          columns.add(new Column("*", null, null));
        } else {
          context.getProjections().forEach((alias, expr) ->
              columns.add(new Column(alias, null, null)));
        }
      }

      return new Schema(columns);
    }
  }

  /**
   * Context object for plan analysis.
   */
  private static class AnalysisContext {
    private Table table;
    private String relationName;
    private List<String> filterConditions = new ArrayList<>();
    private Map<String, String> projections = new HashMap<>();
    private boolean hasAggregation = false;
    private List<String> groupByFields = new ArrayList<>();
    private Map<String, String> aggregations = new HashMap<>();
    private List<String> sortFields = new ArrayList<>();
    private Integer limit;

    // Getters and setters
    public Table getTable() { return table; }
    public void setTable(Table table) { this.table = table; }

    public String getRelationName() { return relationName; }
    public void setRelationName(String relationName) { this.relationName = relationName; }

    public List<String> getFilterConditions() { return filterConditions; }
    public void addFilterCondition(String condition) { filterConditions.add(condition); }

    public Map<String, String> getProjections() { return projections; }
    public void addProjection(String alias, String expression) { projections.put(alias, expression); }

    public boolean isHasAggregation() { return hasAggregation; }
    public void setHasAggregation(boolean hasAggregation) { this.hasAggregation = hasAggregation; }

    public List<String> getGroupByFields() { return groupByFields; }
    public void addGroupByField(String field) { groupByFields.add(field); }

    public Map<String, String> getAggregations() { return aggregations; }
    public void addAggregation(String name, String function) { aggregations.put(name, function); }

    public List<String> getSortFields() { return sortFields; }
    public void addSortField(String field) { sortFields.add(field); }

    public Integer getLimit() { return limit; }
    public void setLimit(Integer limit) { this.limit = limit; }
  }

  /**
   * Result of logical plan analysis.
   */
  private static class PlanAnalysis {
    private Table table;
    private String relationName;
    private List<String> filterConditions = new ArrayList<>();
    private Map<String, String> projections = new HashMap<>();
    private boolean hasAggregation = false;
    private List<String> groupByFields = new ArrayList<>();
    private Map<String, String> aggregations = new HashMap<>();
    private List<String> sortFields = new ArrayList<>();
    private Integer limit;
    private boolean distributable;
    private String reason;
    private Schema outputSchema;

    // Getters and setters
    public Table getTable() { return table; }
    public void setTable(Table table) { this.table = table; }

    public String getRelationName() { return relationName; }
    public void setRelationName(String relationName) { this.relationName = relationName; }

    public List<String> getFilterConditions() { return filterConditions; }
    public void setFilterConditions(List<String> filterConditions) { this.filterConditions = filterConditions; }

    public Map<String, String> getProjections() { return projections; }
    public void setProjections(Map<String, String> projections) { this.projections = projections; }

    public boolean hasAggregation() { return hasAggregation; }
    public void setHasAggregation(boolean hasAggregation) { this.hasAggregation = hasAggregation; }

    public List<String> getGroupByFields() { return groupByFields; }
    public void setGroupByFields(List<String> groupByFields) { this.groupByFields = groupByFields; }

    public Map<String, String> getAggregations() { return aggregations; }
    public void setAggregations(Map<String, String> aggregations) { this.aggregations = aggregations; }

    public List<String> getSortFields() { return sortFields; }
    public void setSortFields(List<String> sortFields) { this.sortFields = sortFields; }

    public Integer getLimit() { return limit; }
    public void setLimit(Integer limit) { this.limit = limit; }

    public boolean isDistributable() { return distributable; }
    public void setDistributable(boolean distributable) { this.distributable = distributable; }

    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }

    public Schema getOutputSchema() { return outputSchema; }
    public void setOutputSchema(Schema outputSchema) { this.outputSchema = outputSchema; }
  }

  // Placeholder task operators - will be implemented in task 4
  private static class LuceneScanOperator extends TaskOperator {
    public LuceneScanOperator(String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      throw new UnsupportedOperationException("LuceneScanOperator implementation pending");
    }

    @Override
    public Map<String, org.opensearch.sql.data.type.ExprType> getOutputSchema() {
      return Map.of();
    }

    @Override
    public double estimateCost(long inputSize) {
      return inputSize * 0.1; // Simple cost model
    }
  }

  private static class PartialAggregationOperator extends TaskOperator {
    public PartialAggregationOperator(String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      throw new UnsupportedOperationException("PartialAggregationOperator implementation pending");
    }

    @Override
    public Map<String, org.opensearch.sql.data.type.ExprType> getOutputSchema() {
      return Map.of();
    }

    @Override
    public double estimateCost(long inputSize) {
      return inputSize * 0.2;
    }
  }

  private static class FinalAggregationOperator extends TaskOperator {
    public FinalAggregationOperator(String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      throw new UnsupportedOperationException("FinalAggregationOperator implementation pending");
    }

    @Override
    public Map<String, org.opensearch.sql.data.type.ExprType> getOutputSchema() {
      return Map.of();
    }

    @Override
    public double estimateCost(long inputSize) {
      return inputSize * 0.05;
    }
  }

  private static class ResultCollectionOperator extends TaskOperator {
    public ResultCollectionOperator(String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      throw new UnsupportedOperationException("ResultCollectionOperator implementation pending");
    }

    @Override
    public Map<String, org.opensearch.sql.data.type.ExprType> getOutputSchema() {
      return Map.of();
    }

    @Override
    public double estimateCost(long inputSize) {
      return inputSize * 0.01;
    }
  }
}