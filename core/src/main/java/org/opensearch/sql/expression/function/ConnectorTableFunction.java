/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.expression.function;

import java.util.List;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.storage.Table;

public interface ConnectorTableFunction {

  /**
   * Returns Functions Signature to do basic validation in Analyzer
   * Layer. Ideally Function Signature consists
   * of default values and if the argument is optional.
   * We will extend Function Signature in future.
   *
   * @return FunctionSignature.
   */
  FunctionSignature getFunctionSignature();

  /**
   * Apply provided arguments and return Table;
   * Which takes of further implementation and execution of the plan.
   *
   * @param arguments arguments.
   * @return Table table.
   */
  Table apply(List<Expression> arguments);

}
