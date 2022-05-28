package org.opensearch.sql.plugin.catalog;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.math3.util.Pair;
import org.opensearch.common.settings.Settings;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.storage.StorageEngine;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class PPLCatalogService implements CatalogService {

    private Map<String, StorageEngine> storageEngineMap;
    private Map<String, ExecutionEngine> executionEngineMap;


    public PPLCatalogService(Settings settings) {
        storageEngineMap = new HashMap<>();
        executionEngineMap = new HashMap<>();
        this.loadConnectors(settings);
    }

    public void loadConnectors(Settings settings) {
        doPrivileged(() -> {
            Boolean isPPLEnabled = (Boolean) OpenSearchSettings.PPL_ENABLED_SETTING.get(settings);
            Boolean isFederationEnabled = PPLCatalogSettings.FEDERATION_ENABLED.get(settings);
            if (isPPLEnabled && isFederationEnabled) {
                InputStream inputStream = PPLCatalogSettings.CATALOG_CONFIG.get(settings);
                if (inputStream != null) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                    try {
                        ArrayNode catalogs = objectMapper.readValue(inputStream, ArrayNode.class);
                        constructConnectors(catalogs);
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Malformed Catalog Configuration Json" + e.getMessage());
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

    private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
        try {
            return SecurityAccess.doPrivileged(action);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to perform privileged action", e);
        }
    }

    private Pair<StorageEngine, ExecutionEngine> createStorageEngineAndExecutionEngine(JsonNode catalog) {
        System.out.println("Constructed connector for catalog :: " + catalog.toString());
        return null;
    }

    private void constructConnectors(ArrayNode catalogs) {
        storageEngineMap = new HashMap<>();
        executionEngineMap = new HashMap<>();
        for (JsonNode catalog : catalogs) {
            String catalogName = catalog.get("name").asText();
            if (storageEngineMap.containsKey(catalogName)) {
                throw new IllegalArgumentException("Catalogs with same name are not allowed.");
            }
            Pair<StorageEngine, ExecutionEngine> pair
                    = createStorageEngineAndExecutionEngine(catalog);
            if(pair != null) {
                storageEngineMap.put(catalogName, pair.getFirst());
                executionEngineMap.put(catalogName, pair.getSecond());
            }
        }
    }

}
