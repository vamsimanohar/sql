package org.opensearch.sql.storage;

import lombok.RequiredArgsConstructor;

import java.util.Map;

@RequiredArgsConstructor
public class StorageEngineFactory {

    private final Map<String, StorageEngine> storageEngineMap;

    public StorageEngine getStorageEngine(String connector) {
        return storageEngineMap.get(connector);
    }

}
