package org.opensearch.sql.catalog;

import org.json.JSONObject;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.storage.StorageEngine;

import java.util.List;
import java.util.Map;

public interface CatalogService {

    void registerCatalogs(Map<String, StorageEngine> storageEngineMap, Map<String, ExecutionEngine> executionEngineMap);

    StorageEngine getStorageEngine(String catalog);

    ExecutionEngine getExecutionEngine(String catalog);

}
