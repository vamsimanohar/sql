/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql.config;

import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.analysis.ExpressionAnalyzer;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.catalog.DefaultCatalogService;
import org.opensearch.sql.catalog.StorageEngineRegistry;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.sql.SQLService;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.storage.StorageEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * SQL service configuration for Spring container initialization.
 */
@Configuration
@Import({ExpressionConfig.class})
public class SQLServiceConfig {

  @Autowired
  private StorageEngine storageEngine;

  @Autowired
  private ExecutionEngine executionEngine;

  @Autowired
  private StorageEngineRegistry storageEngineRegistry;

  @Autowired
  private BuiltinFunctionRepository functionRepository;

  @Bean
  public Analyzer analyzer(CatalogService catalogService) {
    return new Analyzer(new ExpressionAnalyzer(functionRepository), catalogService);
  }

  /**
   * Catalogservice Bean.
   *
   * @return CatalogService.
   */
  @Bean
  public CatalogService catalogService() {
    CatalogService catalogService = new DefaultCatalogService(storageEngineRegistry);
    catalogService.registerOpenSearchStorageEngine(storageEngine);
    return catalogService;
  }

  /**
   * The registration of OpenSearch storage engine happens here because
   * OpenSearchStorageEngine is dependent on NodeClient.
   *
   * @return SQLService.
   */
  @Bean
  public SQLService sqlService(Analyzer analyzer) {
    return new SQLService(new SQLSyntaxParser(), analyzer, executionEngine,
        functionRepository);
  }

}

