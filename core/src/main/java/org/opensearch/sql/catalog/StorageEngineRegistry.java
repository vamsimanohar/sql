package org.opensearch.sql.catalog;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.catalog.model.CatalogMetadata;
import org.opensearch.sql.storage.StorageEngine;

public interface StorageEngineRegistry {

  void loadConnectors(List<CatalogMetadata> catalogMetadata);

  Map<String, StorageEngine> getStorageEngineMap();

}
