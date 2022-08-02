/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.storage;

import java.util.List;

/**
 * Storage engine for different storage to provide data access API implementation.
 */
public interface StorageEngine {

  /**
   * Get {@link Table} from storage engine.
   */
  Table getTable(String name);

  /**
   * Get list of all table names from the storage engine.
   */
  default List<String> getTableNames() {
    return null;
  }

}
