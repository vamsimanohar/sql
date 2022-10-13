/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.CatalogSchemaName;
import org.opensearch.sql.analysis.model.SchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.storage.system.OpenSearchSystemIndex;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;

/** OpenSearch storage engine implementation. */
@RequiredArgsConstructor
public class OpenSearchStorageEngine implements StorageEngine {

  /** OpenSearch client connection. */
  private final OpenSearchClient client;

  private final Settings settings;

  @Override
  public Table getTable(CatalogSchemaName catalogSchemaName, String name) {
    if (isSystemIndex(name)) {
      return new OpenSearchSystemIndex(client, name);
    } else if (SchemaName.INFORMATION_SCHEMA_NAME.equals(catalogSchemaName.getSchemaName())) {
      return resolveInformationSchemaTable(catalogSchemaName, name);
    } else {
      return new OpenSearchIndex(client, settings, name);
    }
  }

  private Table resolveInformationSchemaTable(CatalogSchemaName catalogSchemaName,
                                              String tableName) {
    if (SystemIndexUtils.TABLE_NAME_FOR_TABLES_INFO.equals(tableName)) {
      return new OpenSearchSystemIndex(client, SystemIndexUtils.TABLE_INFO);
    } else {
      throw new SemanticCheckException(
          String.format("Information Schema doesn't contain %s table", tableName));
    }
  }

}
