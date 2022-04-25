/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

import javax.naming.Name;

/**
 * Named expression that represents expression with name.
 * Please see more details in associated unresolved expression operator
 * {@link org.opensearch.sql.ast.expression.Alias}.
 */
@AllArgsConstructor
@EqualsAndHashCode
@Getter
@RequiredArgsConstructor
public class NamedExpression implements Expression {

  /**
   * Expression name.
   */
  private final String name;

  /**
   * Expression that being named.
   */
  private final Expression delegated;

  /**
   * Optional alias.
   */
  private String alias;

  /**
   * New attribute to derive if it needs to be projected.
   */
  private Boolean isColumn = false;


  public NamedExpression(String name, Expression delegated, String alias) {
    this.name = name;
    this.delegated = delegated;
    this.alias = alias;
  }

  public NamedExpression(String name, Expression delegated, Boolean isColumn) {
    this.name = name;
    this.delegated = delegated;
    this.isColumn = isColumn;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return delegated.valueOf(valueEnv);
  }

  @Override
  public ExprType type() {
    return delegated.type();
  }

  /**
   * Get expression name using name or its alias (if it's present).
   * @return  expression name
   */
  public String getNameOrAlias() {
    return Strings.isNullOrEmpty(alias) ? name : alias;
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNamed(this, context);
  }

  @Override
  public String toString() {
    return getNameOrAlias();
  }

}
