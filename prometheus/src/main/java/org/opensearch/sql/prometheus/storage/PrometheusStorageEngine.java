/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.config.PrometheusConfig;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

/**
 * OpenSearch storage engine implementation.
 */
@RequiredArgsConstructor
public class PrometheusStorageEngine implements StorageEngine {

  private final PrometheusClient prometheusService;

  private final PrometheusConfig prometheusConfig;

  @Override
  public Table getTable(String name) {
    return new PrometheusIndex(prometheusService, name, prometheusConfig);
  }

  @Override
  public List<String> getTableNames() {
    try {
      return Arrays.asList(prometheusService.getMetrics());
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Error in fetching metrics from Prometheus Instance." + e.getMessage());
    }
  }
}
