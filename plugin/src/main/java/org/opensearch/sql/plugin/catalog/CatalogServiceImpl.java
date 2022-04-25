/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.catalog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import okhttp3.OkHttpClient;
import org.apache.commons.math3.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.client.PrometheusClientImpl;
import org.opensearch.sql.prometheus.config.PrometheusConfig;
import org.opensearch.sql.prometheus.planner.executor.PrometheusExecutionEngine;
import org.opensearch.sql.prometheus.planner.executor.protector.PrometheusExecutionProtector;
import org.opensearch.sql.prometheus.storage.PrometheusStorageEngine;
import org.opensearch.sql.storage.StorageEngine;


public class CatalogServiceImpl implements CatalogService {

  private Map<String, StorageEngine> storageEngineMap;
  private Map<String, ExecutionEngine> executionEngineMap;

  private static final Logger LOG = LogManager.getLogger();


  /**
   * PPLCatalogService manages connectors
   * and returns storage engine and execution engine based on connector.
   *
   * @param settings settings.
   */
  public CatalogServiceImpl(Settings settings) {
    storageEngineMap = new HashMap<>();
    executionEngineMap = new HashMap<>();
    this.loadConnectors(settings);
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
      Boolean isFederationEnabled = CatalogSettings.FEDERATION_ENABLED.get(settings);
      if (isPPLEnabled && isFederationEnabled) {
        InputStream inputStream = CatalogSettings.CATALOG_CONFIG.get(settings);
        if (inputStream != null) {
          ObjectMapper objectMapper = new ObjectMapper();
          objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          try {
            ArrayNode catalogs = objectMapper.readValue(inputStream, ArrayNode.class);
            constructConnectors(catalogs);
          } catch (IOException e) {
            throw new IllegalArgumentException(
                "Malformed Catalog Configuration Json" + e.getMessage());
          }
        }
      }
      return null;
    });
  }

  @Override
  public Optional<StorageEngine> getStorageEngine(String catalog) {
    return Optional.ofNullable(storageEngineMap.get(catalog));
  }

  @Override
  public Optional<ExecutionEngine> getExecutionEngine(String catalog) {
    return Optional.ofNullable(executionEngineMap.get(catalog));
  }

  @Override
  public Set<String> getCatalogs() {
    return storageEngineMap.keySet();
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

  private Pair<StorageEngine, ExecutionEngine> createStorageEngineAndExecutionEngine(
      JsonNode catalog) throws URISyntaxException {
    StorageEngine storageEngine;
    ExecutionEngine executionEngine;
    switch (catalog.get("connector").asText()) {
      case "prometheus":
        PrometheusClient
            prometheusClient =
            new PrometheusClientImpl(new OkHttpClient(), new URI(catalog.get("uri").asText()));
        PrometheusConfig prometheusConfig = new PrometheusConfig();
        if (catalog.has("defaultTimeRange")) {
          prometheusConfig.setDefaultTimeRange(catalog.get("defaultTimeRange").asLong());
        }
        storageEngine = new PrometheusStorageEngine(prometheusClient, prometheusConfig);
        executionEngine = new PrometheusExecutionEngine(prometheusClient,
            new PrometheusExecutionProtector(
                new org.opensearch.sql.prometheus.monitor.OpenSearchResourceMonitor(
                    new org.opensearch.sql.prometheus.monitor.OpenSearchMemoryHealthy())));
        break;
      default:
        throw new IllegalStateException(
            "Unknown catalog. Please upload the required catalog configuration");
    }
    return new Pair<>(storageEngine, executionEngine);
  }

  private void constructConnectors(ArrayNode catalogs) throws URISyntaxException {
    storageEngineMap = new HashMap<>();
    executionEngineMap = new HashMap<>();
    for (JsonNode catalog : catalogs) {
      String catalogName = catalog.get("name").asText();
      if (storageEngineMap.containsKey(catalogName)) {
        throw new IllegalArgumentException("Catalogs with same name are not allowed.");
      }
      Pair<StorageEngine, ExecutionEngine> pair
          = createStorageEngineAndExecutionEngine(catalog);
      storageEngineMap.put(catalogName, pair.getFirst());
      executionEngineMap.put(catalogName, pair.getSecond());
    }
  }

}
