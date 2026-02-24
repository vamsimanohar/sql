/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.sql.executor.ExecutionEngine.Schema;

/**
 * Analysis result extracted from a Calcite RelNode tree for distributed planning.
 *
 * <p>Contains table name, filter conditions, projections, aggregation info, sort/limit info, and
 * join metadata needed to create distributed execution stages.
 */
public class RelNodeAnalysis {
  private String tableName;
  private List<String> filterConditions = new ArrayList<>();
  private Map<String, String> projections = new HashMap<>();
  private boolean hasAggregation = false;
  private boolean hasJoin = false;
  private String leftTableName;
  private String rightTableName;
  private List<String> groupByFields = new ArrayList<>();
  private Map<String, String> aggregations = new HashMap<>();
  private List<String> sortFields = new ArrayList<>();
  private Integer limit;
  private boolean distributable;
  private String reason;
  private Schema outputSchema;
  private Map<String, String> relNodeInfo = new HashMap<>();

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

  public boolean hasJoin() {
    return hasJoin;
  }

  public void setHasJoin(boolean hasJoin) {
    this.hasJoin = hasJoin;
  }

  public String getLeftTableName() {
    return leftTableName;
  }

  public void setLeftTableName(String leftTableName) {
    this.leftTableName = leftTableName;
  }

  public String getRightTableName() {
    return rightTableName;
  }

  public void setRightTableName(String rightTableName) {
    this.rightTableName = rightTableName;
  }
}
