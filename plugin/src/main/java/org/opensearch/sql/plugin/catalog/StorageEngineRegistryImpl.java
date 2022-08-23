/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.catalog.StorageEngineRegistry;
import org.opensearch.sql.catalog.model.CatalogMetadata;
import org.opensearch.sql.catalog.model.ConnectorType;
import org.opensearch.sql.storage.StorageEngine;

/**
 * This class is responsible for creating instances of connectors to the catalogs.
 */
public class StorageEngineRegistryImpl implements StorageEngineRegistry {

  private static final Logger LOG = LogManager.getLogger();

  private static Map<String, StorageEngine> storageEngineMap = new HashMap<>();

  /**
   * This function reads catalogMetadata and loads connectors to the data stores.
   * This will be invoked during start up and also when settings are updated.
   *
   * @param catalogs catalogs.
   */
  public void loadConnectors(List<CatalogMetadata> catalogs) {
    storageEngineMap = new HashMap<>();
    for (CatalogMetadata catalog : catalogs) {
      String catalogName = catalog.getName();
      StorageEngine storageEngine = createStorageEngine(catalog);
      storageEngineMap.put(catalogName, storageEngine);
    }
  }

  @Override
  public Map<String, StorageEngine> getStorageEngineMap() {
    return Collections.unmodifiableMap(storageEngineMap);
  }

  private StorageEngine createStorageEngine(CatalogMetadata catalog) {
    StorageEngine storageEngine;
    ConnectorType connector = catalog.getConnector();
    switch (connector) {
      case PROMETHEUS:
        storageEngine = null;
        break;
      default:
        LOG.info(
            "Unknown connector \"{}\". "
                + "Please re-upload catalog configuration with a supported connector.",
            connector);
        throw new IllegalStateException(
            "Unknown connector. Connector doesn't exist in the list of supported.");
    }
    return storageEngine;
  }


}