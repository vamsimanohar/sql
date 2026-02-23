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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/**
 * Converts Calcite RelNode trees into multi-stage distributed execution plans.
 *
 * <p>The Calcite-based distributed physical planner analyzes PPL queries that have been converted
 * to Calcite RelNode trees and breaks them into stages that can be executed across multiple nodes
 * in parallel:
 *
 * <ul>
 *   <li><strong>Stage 1 (SCAN)</strong>: Direct shard access with filters and projections
 *   <li><strong>Stage 2 (PROCESS)</strong>: Partial aggregations on each node
 *   <li><strong>Stage 3 (FINALIZE)</strong>: Global aggregation on coordinator
 * </ul>
 *
 * <p><strong>Phase 1 Support:</strong> Simple aggregation queries with filters
 *
 * <pre>
 * search source=logs-* | where status >= 400 | stats count(), avg(latency) by service
 * </pre>
 *
 * <p>This maps to Calcite RelNode tree:
 *
 * <pre>
 * LogicalAggregate(group=[{service}], agg#0=[COUNT()], agg#1=[AVG(latency)])
 *   LogicalFilter(condition=[>=(status, 400)])
 *     LogicalTableScan(table=[[logs-*]])
 * </pre>
 */
@Log4j2
@RequiredArgsConstructor
public class CalciteDistributedPhysicalPlanner {

  /** Interface for discovering data partitions in tables */
  public interface PartitionDiscovery {
    /**
     * Discovers data partitions for a given table name.
     *
     * @param tableName Table name to discover partitions for
     * @return List of data partitions (shards, files, etc.)
     */
    List<DataPartition> discoverPartitions(String tableName);
  }

  private final PartitionDiscovery partitionDiscovery;

  /**
   * Converts a Calcite RelNode into a distributed physical plan.
   *
   * @param relNode The Calcite RelNode to convert
   * @param context The Calcite plan context
   * @return Multi-stage distributed execution plan
   */
  public DistributedPhysicalPlan plan(RelNode relNode, CalcitePlanContext context) {
    String planId = "calcite-distributed-plan-" + UUID.randomUUID().toString().substring(0, 8);
    log.info("Creating Calcite-based distributed physical plan: {}", planId);

    try {
      // Analyze the RelNode tree to determine distributed execution strategy
      RelNodeAnalysis analysis = analyzeRelNode(relNode, context);

      if (!analysis.isDistributable()) {
        log.debug("RelNode not suitable for distributed execution: {}", analysis.getReason());
        throw new UnsupportedOperationException(
            "RelNode not suitable for distributed execution: " + analysis.getReason());
      }

      // Create execution stages based on RelNode analysis
      List<ExecutionStage> stages = createExecutionStages(analysis);

      // Build the final distributed plan
      DistributedPhysicalPlan distributedPlan =
          DistributedPhysicalPlan.create(planId, stages, analysis.getOutputSchema());

      // Phase 1A: Store RelNode and context for local execution
      distributedPlan.setLocalExecutionContext(relNode, context);

      log.info("Created Calcite distributed plan {} with {} stages", planId, stages.size());
      return distributedPlan;

    } catch (Exception e) {
      log.error("Failed to create Calcite distributed physical plan for: {}", relNode, e);
      throw new RuntimeException("Failed to create Calcite distributed physical plan", e);
    }
  }

  /** Analyzes the RelNode tree to determine distributed execution feasibility. */
  private RelNodeAnalysis analyzeRelNode(RelNode relNode, CalcitePlanContext context) {
    RelNodeAnalyzer analyzer = new RelNodeAnalyzer();
    return analyzer.analyze(relNode, context);
  }

  /** Creates execution stages from the RelNode analysis. */
  private List<ExecutionStage> createExecutionStages(RelNodeAnalysis analysis) {
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

  /** Creates Stage 1: Distributed scanning with filters and projections. */
  private ExecutionStage createScanStage(RelNodeAnalysis analysis) {
    String stageId = "calcite-scan-stage-" + UUID.randomUUID().toString().substring(0, 8);

    // Discover partitions for the target table
    List<DataPartition> partitions = partitionDiscovery.discoverPartitions(analysis.getTableName());

    // Create work units for each partition (shard)
    List<WorkUnit> workUnits =
        partitions.stream()
            .map(partition -> createScanWorkUnit(partition, analysis))
            .collect(Collectors.toList());

    log.debug("Created Calcite scan stage {} with {} work units", stageId, workUnits.size());

    return ExecutionStage.createScanStage(stageId, workUnits);
  }

  /** Creates a scan work unit for a specific partition. */
  private WorkUnit createScanWorkUnit(DataPartition partition, RelNodeAnalysis analysis) {
    String workUnitId = "calcite-scan-" + partition.getPartitionId();

    // Create scan operator with filter and projection
    TaskOperator scanOperator =
        new CalciteLuceneScanOperator(
            workUnitId + "-op",
            TaskOperator.OperatorType.SCAN,
            Map.of(
                "filters", analysis.getFilterConditions(),
                "projections", analysis.getProjections(),
                "indexName", partition.getIndexName(),
                "shardId", partition.getShardId(),
                "relNodeInfo", analysis.getRelNodeInfo()));

    return WorkUnit.createScanUnit(workUnitId, partition, scanOperator, partition.getNodeId());
  }

  /** Creates Stage 2: Partial aggregation processing. */
  private ExecutionStage createPartialAggregationStage(
      RelNodeAnalysis analysis, String scanStageId) {
    String stageId = "calcite-partial-agg-stage-" + UUID.randomUUID().toString().substring(0, 8);

    // Create work units for partial aggregation (one per node that has data)
    // For Phase 1, we'll create a simple work unit per expected node
    List<WorkUnit> workUnits =
        IntStream.range(0, 3) // Assume 3 data nodes for now
            .mapToObj(i -> createPartialAggregationWorkUnit(i, analysis, scanStageId))
            .collect(Collectors.toList());

    log.debug(
        "Created Calcite partial aggregation stage {} with {} work units",
        stageId,
        workUnits.size());

    return ExecutionStage.createProcessStage(
        stageId, workUnits, List.of(scanStageId), ExecutionStage.DataExchangeType.NONE);
  }

  /** Creates a partial aggregation work unit. */
  private WorkUnit createPartialAggregationWorkUnit(
      int nodeIndex, RelNodeAnalysis analysis, String scanStageId) {
    String workUnitId = "calcite-partial-agg-" + nodeIndex;

    TaskOperator aggregationOperator =
        new CalcitePartialAggregationOperator(
            workUnitId + "-op",
            TaskOperator.OperatorType.PARTIAL_AGGREGATE,
            Map.of(
                "groupByFields", analysis.getGroupByFields(),
                "aggregations", analysis.getAggregations(),
                "nodeIndex", nodeIndex,
                "relNodeInfo", analysis.getRelNodeInfo()));

    return WorkUnit.createProcessUnit(workUnitId, aggregationOperator, List.of(scanStageId));
  }

  /** Creates Stage 3: Final aggregation. */
  private ExecutionStage createFinalAggregationStage(
      RelNodeAnalysis analysis, String processStageId) {
    String stageId = "calcite-final-agg-stage-" + UUID.randomUUID().toString().substring(0, 8);
    String workUnitId = "calcite-final-agg";

    TaskOperator finalOperator =
        new CalciteFinalAggregationOperator(
            workUnitId + "-op",
            TaskOperator.OperatorType.FINAL_AGGREGATE,
            Map.of(
                "groupByFields", analysis.getGroupByFields(),
                "aggregations", analysis.getAggregations(),
                "relNodeInfo", analysis.getRelNodeInfo()));

    WorkUnit finalWorkUnit =
        WorkUnit.createFinalizeUnit(workUnitId, finalOperator, List.of(processStageId));

    log.debug("Created Calcite final aggregation stage {}", stageId);

    return ExecutionStage.createFinalizeStage(stageId, finalWorkUnit, List.of(processStageId));
  }

  /** Creates a result collection stage for non-aggregation queries. */
  private ExecutionStage createResultCollectionStage(RelNodeAnalysis analysis, String scanStageId) {
    String stageId = "calcite-collect-stage-" + UUID.randomUUID().toString().substring(0, 8);
    String workUnitId = "calcite-collect-results";

    Map<String, Object> operatorConfig = new HashMap<>();
    if (analysis.getLimit() != null) {
      operatorConfig.put("limit", analysis.getLimit());
    }
    operatorConfig.put("sortFields", analysis.getSortFields());
    operatorConfig.put("relNodeInfo", analysis.getRelNodeInfo());

    TaskOperator collectOperator =
        new CalciteResultCollectionOperator(
            workUnitId + "-op", TaskOperator.OperatorType.FINALIZE, operatorConfig);

    WorkUnit collectWorkUnit =
        WorkUnit.createFinalizeUnit(workUnitId, collectOperator, List.of(scanStageId));

    log.debug("Created Calcite result collection stage {}", stageId);

    return ExecutionStage.createFinalizeStage(stageId, collectWorkUnit, List.of(scanStageId));
  }

  /** Analyzer that extracts distributed execution information from RelNode trees. */
  private static class RelNodeAnalyzer {

    public RelNodeAnalysis analyze(RelNode relNode, CalcitePlanContext context) {
      RelNodeAnalysis analysis = new RelNodeAnalysis();

      // Walk the RelNode tree and extract information
      analyzeNode(relNode, analysis, context);

      // Determine if the plan is distributable
      boolean distributable = analysis.getTableName() != null;
      String reason = distributable ? null : "No table found in RelNode tree";

      analysis.setDistributable(distributable);
      analysis.setReason(reason);

      // Create output schema (simplified for Phase 1)
      Schema outputSchema = createOutputSchema(analysis);
      analysis.setOutputSchema(outputSchema);

      return analysis;
    }

    private void analyzeNode(RelNode node, RelNodeAnalysis analysis, CalcitePlanContext context) {
      if (node instanceof TableScan) {
        analyzeTableScan((TableScan) node, analysis);
      } else if (node instanceof Filter) {
        analyzeFilter((Filter) node, analysis);
      } else if (node instanceof Project) {
        analyzeProject((Project) node, analysis);
      } else if (node instanceof Aggregate) {
        analyzeAggregate((Aggregate) node, analysis);
      } else if (node instanceof Sort) {
        analyzeSort((Sort) node, analysis);
      }

      // Store RelNode information for later use in operators
      analysis.getRelNodeInfo().put(node.getClass().getSimpleName(), node.getDigest());

      // Recursively analyze inputs
      for (RelNode input : node.getInputs()) {
        analyzeNode(input, analysis, context);
      }
    }

    private void analyzeTableScan(TableScan tableScan, RelNodeAnalysis analysis) {
      List<String> qualifiedName = tableScan.getTable().getQualifiedName();
      // Last segment is the actual index name (e.g., [default, accounts] -> accounts)
      String tableName = qualifiedName.get(qualifiedName.size() - 1);
      analysis.setTableName(tableName);
      log.debug("Found table scan: {}", tableName);
    }

    private void analyzeFilter(Filter filter, RelNodeAnalysis analysis) {
      String condition = filter.getCondition().toString();
      analysis.addFilterCondition(condition);
      log.debug("Found filter: {}", condition);
    }

    private void analyzeProject(Project project, RelNodeAnalysis analysis) {
      project
          .getProjects()
          .forEach(
              expr -> {
                String exprStr = expr.toString();
                analysis.addProjection(exprStr, exprStr);
              });
      log.debug("Found projection with {} expressions", project.getProjects().size());
    }

    private void analyzeAggregate(Aggregate aggregate, RelNodeAnalysis analysis) {
      analysis.setHasAggregation(true);

      // Extract group by fields
      aggregate
          .getGroupSet()
          .forEach(
              groupIndex -> {
                String fieldName = "field_" + groupIndex; // Simplified for Phase 1
                analysis.addGroupByField(fieldName);
              });

      // Extract aggregation calls
      aggregate
          .getAggCallList()
          .forEach(
              aggCall -> {
                String aggName = aggCall.getAggregation().getName();
                String aggExpr = aggCall.toString();
                analysis.addAggregation(aggName, aggExpr);
              });

      log.debug(
          "Found aggregation with {} groups and {} agg calls",
          aggregate.getGroupCount(),
          aggregate.getAggCallList().size());
    }

    private void analyzeSort(Sort sort, RelNodeAnalysis analysis) {
      if (sort.getCollation() != null) {
        sort.getCollation()
            .getFieldCollations()
            .forEach(
                field -> {
                  String fieldName = "field_" + field.getFieldIndex();
                  analysis.addSortField(fieldName);
                });
      }

      if (sort.fetch != null) {
        // Extract limit from fetch
        analysis.setLimit(100); // Simplified for Phase 1
      }

      log.debug("Found sort with collation: {}", sort.getCollation());
    }

    private Schema createOutputSchema(RelNodeAnalysis analysis) {
      List<Column> columns = new ArrayList<>();

      if (analysis.hasAggregation()) {
        // Add group by columns
        analysis.getGroupByFields().forEach(field -> columns.add(new Column(field, null, null)));

        // Add aggregation columns
        analysis
            .getAggregations()
            .forEach((name, func) -> columns.add(new Column(name, null, null)));
      } else {
        // Add projection columns or all fields
        if (analysis.getProjections().isEmpty()) {
          columns.add(new Column("*", null, null));
        } else {
          analysis
              .getProjections()
              .forEach((alias, expr) -> columns.add(new Column(alias, null, null)));
        }
      }

      return new Schema(columns);
    }
  }

  /** Analysis result for RelNode trees. */
  private static class RelNodeAnalysis {
    private String tableName;
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
    private Map<String, String> relNodeInfo = new HashMap<>();

    // Getters and setters
    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public List<String> getFilterConditions() {
      return filterConditions;
    }

    public void addFilterCondition(String condition) {
      filterConditions.add(condition);
    }

    public Map<String, String> getProjections() {
      return projections;
    }

    public void addProjection(String alias, String expression) {
      projections.put(alias, expression);
    }

    public boolean hasAggregation() {
      return hasAggregation;
    }

    public void setHasAggregation(boolean hasAggregation) {
      this.hasAggregation = hasAggregation;
    }

    public List<String> getGroupByFields() {
      return groupByFields;
    }

    public void addGroupByField(String field) {
      groupByFields.add(field);
    }

    public Map<String, String> getAggregations() {
      return aggregations;
    }

    public void addAggregation(String name, String function) {
      aggregations.put(name, function);
    }

    public List<String> getSortFields() {
      return sortFields;
    }

    public void addSortField(String field) {
      sortFields.add(field);
    }

    public Integer getLimit() {
      return limit;
    }

    public void setLimit(Integer limit) {
      this.limit = limit;
    }

    public boolean isDistributable() {
      return distributable;
    }

    public void setDistributable(boolean distributable) {
      this.distributable = distributable;
    }

    public String getReason() {
      return reason;
    }

    public void setReason(String reason) {
      this.reason = reason;
    }

    public Schema getOutputSchema() {
      return outputSchema;
    }

    public void setOutputSchema(Schema outputSchema) {
      this.outputSchema = outputSchema;
    }

    public Map<String, String> getRelNodeInfo() {
      return relNodeInfo;
    }
  }

  // Phase 1A: Operators are no-op shells. Execution is handled by DistributedTaskScheduler
  // local mode via Calcite. Phase 1B+ will implement per-shard Lucene scanning and
  // true distributed aggregation operators.

  private static class CalciteLuceneScanOperator extends TaskOperator {
    public CalciteLuceneScanOperator(
        String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      // Phase 1A: No-op - execution handled by DistributedTaskScheduler local mode
      // Phase 1B+: Will implement per-shard Lucene scanning
      TaskResult result = new TaskResult();
      result.setOutputData(input != null ? input.getInputData() : List.of());
      result.setRecordCount(0);
      return result;
    }

    @Override
    public Map<String, org.opensearch.sql.data.type.ExprType> getOutputSchema() {
      return Map.of();
    }

    @Override
    public double estimateCost(long inputSize) {
      return inputSize * 0.1;
    }
  }

  private static class CalcitePartialAggregationOperator extends TaskOperator {
    public CalcitePartialAggregationOperator(
        String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      // Phase 1A: No-op pass-through - Calcite handles aggregation internally
      TaskResult result = new TaskResult();
      result.setOutputData(input != null ? input.getInputData() : List.of());
      result.setRecordCount(0);
      result.setPartialResult(true);
      return result;
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

  private static class CalciteFinalAggregationOperator extends TaskOperator {
    public CalciteFinalAggregationOperator(
        String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      // Phase 1A: No-op pass-through - Calcite handles aggregation internally
      TaskResult result = new TaskResult();
      result.setOutputData(input != null ? input.getInputData() : List.of());
      result.setRecordCount(0);
      return result;
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

  private static class CalciteResultCollectionOperator extends TaskOperator {
    public CalciteResultCollectionOperator(
        String operatorId, OperatorType operatorType, Map<String, Object> config) {
      super(operatorId, operatorType, config);
    }

    @Override
    public TaskResult execute(TaskContext context, TaskInput input) {
      // Phase 1A: No-op pass-through - results collected by scheduler
      TaskResult result = new TaskResult();
      result.setOutputData(input != null ? input.getInputData() : List.of());
      result.setRecordCount(0);
      return result;
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
