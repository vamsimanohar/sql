/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.tablefunctions;

import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.function.ConnectorTableFunction;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.FunctionSignature;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;
import org.opensearch.sql.storage.Table;

public class QueryRangeTableFunction implements ConnectorTableFunction {

  private final PrometheusClient prometheusClient;

  private final FunctionSignature functionSignature;

  public static final String QUERY = "query";
  public static final String STARTTIME = "starttime";
  public static final String ENDTIME = "endtime";
  public static final String STEP = "step";


  public QueryRangeTableFunction(PrometheusClient prometheusClient) {
    this.prometheusClient = prometheusClient;
    this.functionSignature = new FunctionSignature(FunctionName.of("query_range"),
        List.of(STRING, LONG, LONG, LONG),
        List.of(QUERY, STARTTIME, ENDTIME, STEP));
  }

  public FunctionSignature getFunctionSignature() {
    return this.functionSignature;
  }

  @Override
  public Table apply(List<Expression> arguments) {
    return new PrometheusMetricTable(prometheusClient, buildQueryFromQueryRangeFunction(arguments));
  }


  private PrometheusQueryRequest buildQueryFromQueryRangeFunction(List<Expression> arguments) {

    PrometheusQueryRequest prometheusQueryRequest = new PrometheusQueryRequest();
    arguments.forEach(arg -> {
      String argName = ((NamedArgumentExpression) arg).getArgName();
      Expression argValue = ((NamedArgumentExpression) arg).getValue();
      ExprValue literalValue = argValue.valueOf(null);
      switch (argName) {
        case QUERY:
          prometheusQueryRequest
              .getPromQl().append((String) value(literalValue));
          break;
        case STARTTIME:
          prometheusQueryRequest.setStartTime(((Number) value(literalValue)).longValue());
          break;
        case ENDTIME:
          prometheusQueryRequest.setEndTime(((Number) value(literalValue)).longValue());
          break;
        case STEP:
          prometheusQueryRequest.setStep(value(literalValue).toString());
          break;
        default:
          throw new SemanticCheckException(String.format("Invalid Function Argument:%s", argName));
      }
    });
    return prometheusQueryRequest;
  }

  private Object value(ExprValue literal) {
    if (literal.type().equals(ExprCoreType.TIMESTAMP)) {
      return literal.timestampValue().toEpochMilli();
    } else {
      return literal.value();
    }
  }


}
