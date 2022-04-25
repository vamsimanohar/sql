/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.expression.Expression;

/**
 * Logical Relation represent the data source.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalSourceNativeQuery extends LogicalPlan {

  @Getter
  private final String catalogName;

  @Getter
  private final Map<String, Literal> arguments;

  /**
   * Constructor of LogicalRelation.
   */
  public LogicalSourceNativeQuery(String catalogName, Map<String, Literal> arguments) {
    super(ImmutableList.of());
    this.catalogName = catalogName;
    this.arguments = arguments;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNativeQuery(this, context);
  }
}
