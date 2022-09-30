/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import java.util.Optional;
import org.opensearch.sql.expression.function.ConnectorTableFunction;

/**
 * Storage engine for different storage to provide data access API implementation.
 */
public interface StorageEngine {

  /**
   * Get {@link Table} from storage engine.
   */
  Table getTable(String name);

  /**
   * Default Implementation Returns Null
   *
   * @param name
   * @return
   */
  default Optional<ConnectorTableFunction> getFunction(String name) {
    return null;
  }

}
