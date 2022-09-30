/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.METRIC;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.storage.Table;

/**
 * Prometheus table (metric) implementation.
 */
public class PrometheusMetricTable implements Table {

  private final PrometheusClient prometheusClient;

  private final Optional<String> metricName;

  private final Optional<PrometheusQueryRequest> prometheusQueryRequest;


  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor only with metric name.
   */
  public PrometheusMetricTable(PrometheusClient prometheusService, @Nonnull String metricName) {
    this.prometheusClient = prometheusService;
    this.metricName = Optional.of(metricName);
    this.prometheusQueryRequest = Optional.empty();
  }

  /**
   * Constructor for entire promQl Request.
   */
  public PrometheusMetricTable(PrometheusClient prometheusService,
                               @Nonnull PrometheusQueryRequest prometheusQueryRequest) {
    this.prometheusClient = prometheusService;
    this.metricName = Optional.empty();
    this.prometheusQueryRequest = Optional.of(prometheusQueryRequest);
  }

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      cachedFieldTypes =
          new PrometheusDescribeMetricRequest(prometheusClient, metricName.orElse(null)).getFieldTypes();
    }
    return cachedFieldTypes;
  }

  /**
   * TODO: Push down operations to index scan operator as much as possible in future.
   */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    PrometheusMetricScan metricScan =
        new PrometheusMetricScan(prometheusClient);
    prometheusQueryRequest.ifPresent(metricScan::setRequest);
    return plan.accept(new PrometheusDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

  /**
   * Default Implementor of Logical plan for prometheus.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  public static class PrometheusDefaultImplementor
      extends DefaultImplementor<PrometheusMetricScan> {

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node,
                                             PrometheusMetricScan context) {
      return context;
    }

    // Since getFieldTypes include labels
    // we are explicitly specifying the output column names;
    @Override
    public PhysicalPlan visitProject(LogicalProject node, PrometheusMetricScan context) {
      List<NamedExpression> finalProjectList = new ArrayList<>();
      finalProjectList.add(
          new NamedExpression(METRIC, new ReferenceExpression(METRIC, ExprCoreType.STRING)));
      finalProjectList.add(
          new NamedExpression(TIMESTAMP,
              new ReferenceExpression(TIMESTAMP, ExprCoreType.TIMESTAMP)));
      finalProjectList.add(
          new NamedExpression(VALUE, new ReferenceExpression(VALUE, ExprCoreType.DOUBLE)));
      return new ProjectOperator(visitChild(node, context), finalProjectList,
          node.getNamedParseExpressions());
    }

  }


}