/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.plugin.model.CatalogMetadata;
import org.opensearch.sql.plugin.model.ConnectorType;
import org.opensearch.sql.storage.StorageEngine;

/**
 * This class manages catalogs and responsible for creating connectors to these catalogs.
 */
public class CatalogServiceImpl implements CatalogService {

  private static final CatalogServiceImpl INSTANCE = new CatalogServiceImpl();

  private static final Logger LOG = LogManager.getLogger();

  public static final String OPEN_SEARCH = "opensearch";

  private Map<String, StorageEngine> storageEngineMap = new HashMap<>();

  public static CatalogServiceImpl getInstance() {
    return INSTANCE;
  }

  private CatalogServiceImpl() {
  }

  /**
   * This function reads settings and loads connectors to the data stores.
   * This will be invoked during start up and also when settings are updated.
   *
   * @param settings settings.
   */
  public void loadConnectors(Settings settings) {
    doPrivileged(() -> {
      Boolean isPPLEnabled = (Boolean) OpenSearchSettings.PPL_ENABLED_SETTING.get(settings);
      if (isPPLEnabled) {
        InputStream inputStream = CatalogSettings.CATALOG_CONFIG.get(settings);
        if (inputStream != null) {
          ObjectMapper objectMapper = new ObjectMapper();
          objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          try {
            List<CatalogMetadata> catalogs =
                objectMapper.readValue(inputStream, new TypeReference<>() {
                });
            LOG.info(catalogs.toString());
            validateCatalogs(catalogs);
            constructConnectors(catalogs);
          } catch (IOException e) {
            LOG.error("Catalog Configuration File uploaded is malformed. Verify and re-upload.");
            throw new IllegalArgumentException(
                "Malformed Catalog Configuration Json" + e.getMessage());
          }
        }
      } else {
        LOG.info("PPL federation is not enabled.");
      }
      return null;
    });
  }

  @Override
  public StorageEngine getStorageEngine(String catalog) {
    if (catalog == null || !storageEngineMap.containsKey(catalog)) {
      return storageEngineMap.get(OPEN_SEARCH);
    }
    return storageEngineMap.get(catalog);
  }

  @Override
  public Set<String> getCatalogs() {
    Set<String> catalogs = storageEngineMap.keySet();
    catalogs.remove(OPEN_SEARCH);
    return catalogs;
  }

  @Override
  public void registerOpenSearchStorageEngine(StorageEngine storageEngine) {
    storageEngineMap.put(OPEN_SEARCH, storageEngine);
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

  private StorageEngine createStorageEngine(CatalogMetadata catalog) throws URISyntaxException {
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

  private void constructConnectors(List<CatalogMetadata> catalogs) throws URISyntaxException {
    storageEngineMap = new HashMap<>();
    for (CatalogMetadata catalog : catalogs) {
      String catalogName = catalog.getName();
      StorageEngine storageEngine = createStorageEngine(catalog);
      storageEngineMap.put(catalogName, storageEngine);
    }
  }

  /**
   * This can be moved to a different validator class
   * when we introduce more connectors.
   *
   * @param catalogs catalogs.
   */
  private void validateCatalogs(List<CatalogMetadata> catalogs) {

    Set<String> catalogNameSet = new HashSet<>();
    Set<String> duplicateCatalogNames = catalogs
        .stream()
        .map(CatalogMetadata::getName)
        .filter(catalogName -> !catalogNameSet.add(catalogName))
        .collect(Collectors.toSet());
    if (!duplicateCatalogNames.isEmpty()) {
      LOG.error("Duplicate catalog names are not allowed. Found following duplicates: {}",
          duplicateCatalogNames.toString());
      throw new IllegalArgumentException(
          "Duplicate catalog names are not allowed. Found following duplicates: "
              + duplicateCatalogNames);
    }
  }


}