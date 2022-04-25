/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexAgg;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexScan;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalPlanOptimizerFactory;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.OpenSearchRequest;
import org.opensearch.sql.prometheus.request.system.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.prometheus.storage.script.aggregation.AggregationQueryBuilder;
import org.opensearch.sql.prometheus.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.prometheus.storage.script.filter.PromFilterQuery;
import org.opensearch.sql.prometheus.storage.script.filter.PromQLFilterQueryBuilder;
import org.opensearch.sql.prometheus.storage.script.sort.SortQueryBuilder;
import org.opensearch.sql.prometheus.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/** OpenSearch table (index) implementation. */
public class PrometheusIndex implements Table {

  private final PrometheusClient prometheusService;

  private final Settings settings;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final OpenSearchRequest.IndexName indexName;

  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor.
   */
  public PrometheusIndex(PrometheusClient prometheusService, Settings settings, String indexName) {
    this.prometheusService = prometheusService;
    this.settings = settings;
    this.indexName = new OpenSearchRequest.IndexName(indexName);
  }

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      cachedFieldTypes = new PrometheusDescribeMetricRequest(prometheusService, indexName).getFieldTypes();
    }
    return cachedFieldTypes;
  }

  /**
   * TODO: Push down operations to index scan operator as much as possible in future.
   */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    PrometheusMetricScan indexScan = new PrometheusMetricScan( prometheusService, settings, indexName,
            new OpenSearchExprValueFactory(getFieldTypes()));

    /*
     * Visit logical plan with index scan as context so logical operators visited, such as
     * aggregation, filter, will accumulate (push down) OpenSearch query and aggregation DSL on
     * index scan.
     */
    return plan.accept(new PrometheusDefaultImplementor(indexScan), indexScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return PrometheusLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class PrometheusDefaultImplementor
          extends DefaultImplementor<PrometheusMetricScan> {
    private final PrometheusMetricScan indexScan;
    private HashSet<String> allowedProjectList = new HashSet<>(){{
      add("@value");
      add("@timestamp");
      add("metric");
    }};

    @Override
    public PhysicalPlan visitNode(LogicalPlan plan, PrometheusMetricScan context) {
      if (plan instanceof PrometheusLogicalIndexScan) {
        return visitIndexScan((PrometheusLogicalIndexScan) plan, context);
      } else if (plan instanceof PrometheusLogicalIndexAgg) {
        return visitIndexAggregation((PrometheusLogicalIndexAgg) plan, context);
      } else {
        throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
                plan.getClass()));
      }
    }

    /**
     * Implement ElasticsearchLogicalIndexScan.
     */
    public PhysicalPlan visitIndexScan(PrometheusLogicalIndexScan node,
                                       PrometheusMetricScan context) {
      if (null != node.getSortList()) {
        final SortQueryBuilder builder = new SortQueryBuilder();
        context.pushDownSort(node.getSortList().stream()
                .map(sort -> builder.build(sort.getValue(), sort.getKey()))
                .collect(Collectors.toList()));
      }
      OpenSearchRequest request = context.getRequest();
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();

      if(node.getRelationName().toLowerCase(Locale.ROOT).contains(".promql(")) {
        Pattern p = Pattern.compile("`([^`]+)`");
        Matcher m = p.matcher(node.getRelationName());
        if(m.find()) {
          promQlBuilder.append(m.group(1));
        }
        return indexScan;
      }
      promQlBuilder.append(node.getRelationName().split("\\.")[1]);
      if (null != node.getFilter()) {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
        QueryBuilder query = queryBuilder.build(node.getFilter());
        context.pushDown(query);
        PromQLFilterQueryBuilder promQLFilterQueryBuilder = new PromQLFilterQueryBuilder();
        PromFilterQuery filterQuery = promQLFilterQueryBuilder.build(node.getFilter());
        if(filterQuery.getPromQl()!=null && filterQuery.getPromQl().length() > 0) {
          filterQuery.setPromQl(new StringBuilder().append("{").append(filterQuery.getPromQl()).append("}"));
          promQlBuilder.append(filterQuery.getPromQl());
        }

        if(filterQuery.getStartTime()!= null) {
          request.setStartTime(filterQuery.getStartTime());
        }
        if(filterQuery.getEndTime() != null) {
          request.setEndTime(filterQuery.getEndTime());
        }
      }
      if (node.getLimit() != null) {
        context.pushDownLimit(node.getLimit(), node.getOffset());
      }

      if (node.hasProjects()) {
        context.pushDownProjects(node.getProjectList());
      }

      if (node.getFilter() != null) {

      }
      promQlBuilder.insert(0, "(");
      promQlBuilder.append(")");

      return indexScan;
    }

    /**
     * Implement ElasticsearchLogicalIndexAgg.
     */
    public PhysicalPlan visitIndexAggregation(PrometheusLogicalIndexAgg node,
                                              PrometheusMetricScan context) {
      OpenSearchRequest request = context.getRequest();
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();
      promQlBuilder.append(node.getRelationName().split("\\.")[1]);
      if (node.getFilter() != null) {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
                new DefaultExpressionSerializer());
        PromQLFilterQueryBuilder promQLFilterQueryBuilder = new PromQLFilterQueryBuilder();
        PromFilterQuery filterQuery = promQLFilterQueryBuilder.build(node.getFilter());
        QueryBuilder query = queryBuilder.build(node.getFilter());
        context.pushDown(query);
        if(filterQuery.getPromQl()!=null && filterQuery.getPromQl().length() > 0) {
          filterQuery.setPromQl(new StringBuilder().append("{").append(filterQuery.getPromQl()).append("}"));
          promQlBuilder.append(filterQuery.getPromQl());
        }


        if(filterQuery.getStartTime()!= null) {
          request.setStartTime(filterQuery.getStartTime());
        }
        if(filterQuery.getEndTime() != null) {
          request.setEndTime(filterQuery.getEndTime());
        }
      }
      promQlBuilder.insert(0, "(");
      promQlBuilder.append(")");
      AggregationQueryBuilder builder =
              new AggregationQueryBuilder(new DefaultExpressionSerializer());
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder =
              builder.buildAggregationBuilder(node.getAggregatorList(),
                      node.getGroupByList(), node.getSortList());
      context.pushDownAggregation(aggregationBuilder);
      context.pushTypeMapping(
              builder.buildTypeMapping(node.getAggregatorList(),
                      node.getGroupByList()));
      StringBuilder aggregateQuery = new StringBuilder();
      if(!node.getAggregatorList().isEmpty()) {
        aggregateQuery.insert(0, node.getAggregatorList().get(0).getFunctionName().getFunctionName() + " ");
        if(!node.getGroupByList().isEmpty()){
          Optional<SpanExpression> spanExpression = node.getGroupByList().stream().map(NamedExpression::getDelegated)
                  .filter(delegated -> delegated instanceof SpanExpression)
                  .map(delegated -> (SpanExpression) delegated)
                  .findFirst();
          spanExpression.ifPresent(expression -> request.setStep(expression.getValue().toString() + expression.getUnit().getName()));
          long groupByCount = node.getGroupByList().stream().map(NamedExpression::getDelegated)
                  .filter(delegated -> !(delegated instanceof SpanExpression)).count();
          if (groupByCount > 0) {
            aggregateQuery.append("by (");
            for (int i = 0; i < node.getGroupByList().size(); i++) {
              NamedExpression expression = node.getGroupByList().get(i);
              if(expression.getDelegated() instanceof SpanExpression)
                continue;
              if (i == node.getGroupByList().size() - 1) {
                aggregateQuery.append(expression.getName());
              } else {
                aggregateQuery.append(expression.getName()).append(", ");
              }
            }
            aggregateQuery.append(")");
          }
        }
      }
      promQlBuilder.insert(0, aggregateQuery);

      return indexScan;
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, PrometheusMetricScan context) {
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();
      if(node.getRelationName().toLowerCase(Locale.ROOT).contains(".promql(")) {
        Pattern p = Pattern.compile("`([^`]+)`");
        Matcher m = p.matcher(node.getRelationName());
        if(m.find()) {
          promQlBuilder.append(m.group(1));
        }
      }
      else {
        promQlBuilder.append(node.getRelationName().split("\\.")[1]);
      }
      return indexScan;
    }

    @Override
    public PhysicalPlan visitProject(LogicalProject node, PrometheusMetricScan context) {
      List<NamedExpression> finalProjectList = node.getProjectList().stream()
              .filter(expression -> !expression.getIsColumn() || allowedProjectList.contains(expression.getName()))
              .collect(Collectors.toList());
      if(!finalProjectList.stream().anyMatch(expression ->  expression.getName().equals("metric"))) {
        finalProjectList.add(new NamedExpression("metric", new ReferenceExpression("metric", ExprCoreType.STRING), true));
      }
      return new ProjectOperator(visitChild(node, context), finalProjectList,
              node.getNamedParseExpressions());
    }

  }
}
