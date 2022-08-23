/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl.config;

import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.catalog.DefaultCatalogService;
import org.opensearch.sql.catalog.StorageEngineRegistry;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.expression.config.ExpressionConfig;
import org.opensearch.sql.expression.function.BuiltinFunctionRepository;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.storage.StorageEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({ExpressionConfig.class})
public class PPLServiceConfig {

  @Autowired
  private StorageEngine storageEngine;

  @Autowired
  private ExecutionEngine executionEngine;

  @Autowired
  private StorageEngineRegistry storageEngineRegistry;

  @Autowired
  private BuiltinFunctionRepository functionRepository;

  /**
   * CatalogService Bean.
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
   * @return PPLService.
   */
  @Bean
  public PPLService pplService(CatalogService catalogService) {
    return new PPLService(new PPLSyntaxParser(), executionEngine,
            functionRepository, catalogService);
  }

}
