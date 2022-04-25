/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.script.aggregation.dsl;

import static java.util.Collections.emptyMap;
import static org.opensearch.script.Script.DEFAULT_SCRIPT_TYPE;

import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.opensearch.script.Script;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.prometheus.storage.script.ScriptUtils;
import org.opensearch.sql.prometheus.storage.serialization.ExpressionSerializer;
import org.opensearch.sql.prometheus.storage.script.ExpressionScriptEngine;

/**
 * Abstract Aggregation Builder.
 */
@RequiredArgsConstructor
public class AggregationBuilderHelper {

  private final ExpressionSerializer serializer;

  /**
   * Build AggregationBuilder from Expression.
   *
   * @param expression Expression
   * @return AggregationBuilder
   */
  public <T> T build(Expression expression, Function<String, T> fieldBuilder,
                 Function<Script, T> scriptBuilder) {
    if (expression instanceof ReferenceExpression) {
      String fieldName = ((ReferenceExpression) expression).getAttr();
      return fieldBuilder.apply(ScriptUtils.convertTextToKeyword(fieldName, expression.type()));
    } else if (expression instanceof FunctionExpression) {
      return scriptBuilder.apply(new Script(
          DEFAULT_SCRIPT_TYPE, ExpressionScriptEngine.EXPRESSION_LANG_NAME, serializer.serialize(expression),
          emptyMap()));
    } else {
      throw new IllegalStateException(String.format("metric aggregation doesn't support "
          + "expression %s", expression));
    }
  }
}
