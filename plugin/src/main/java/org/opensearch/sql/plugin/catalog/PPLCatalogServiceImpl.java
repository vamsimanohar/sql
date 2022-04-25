package org.opensearch.sql.plugin.catalog;

import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.storage.StorageEngine;

import java.util.Map;

public class PPLCatalogServiceImpl implements CatalogService {

    private Map<String, StorageEngine> storageEngineMap;
    private Map<String, ExecutionEngine> executionEngineMap;

    @Override
    public void registerCatalogs(Map<String, StorageEngine> storageEngineMap, Map<String, ExecutionEngine> executionEngineMap) {
        this.executionEngineMap = executionEngineMap;
        this.storageEngineMap = storageEngineMap;
    }

    @Override
    public StorageEngine getStorageEngine(String catalog) {
        return storageEngineMap.get(catalog);
    }

    @Override
    public ExecutionEngine getExecutionEngine(String catalog) {
        return executionEngineMap.get(catalog);
    }
}
