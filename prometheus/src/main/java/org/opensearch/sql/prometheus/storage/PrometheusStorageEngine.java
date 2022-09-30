/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.opensearch.sql.expression.function.ConnectorTableFunction;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.tablefunctions.QueryRangeTableFunction;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;


/**
 * Prometheus storage engine implementation.
 */
public class PrometheusStorageEngine implements StorageEngine {

  private final Map<FunctionName, ConnectorTableFunction> functionMap;

  private final PrometheusClient prometheusClient;

  public PrometheusStorageEngine(PrometheusClient prometheusClient) {
    this.prometheusClient = prometheusClient;
    functionMap = new HashMap<>();
    functionMap.put(FunctionName.of("query_range"), new QueryRangeTableFunction(prometheusClient));
  }

  @Override
  public Table getTable(String name) {
    return null;
  }

  @Override
  public Optional<ConnectorTableFunction> getFunction(String name) {
    return Optional.ofNullable(functionMap.get(FunctionName.of(name)));
  }

}
