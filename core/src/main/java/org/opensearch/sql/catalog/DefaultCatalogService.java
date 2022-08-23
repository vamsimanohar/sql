package org.opensearch.sql.catalog;

import static org.opensearch.sql.catalog.CatalogConstants.OPENSEARCH;

import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.storage.StorageEngine;

@RequiredArgsConstructor
public class DefaultCatalogService implements CatalogService {

  private final StorageEngineRegistry storageEngineRegistry;

  private StorageEngine openSearchStorageEngine;

  @Override
  public StorageEngine getStorageEngine(String catalog) {
    Map<String,StorageEngine> storageEngineMap = storageEngineRegistry.getStorageEngineMap();
    if (catalog == null || !storageEngineMap.containsKey(catalog)) {
      return openSearchStorageEngine;
    }
    return storageEngineMap.get(catalog);
  }

  @Override
  public Set<String> getCatalogs() {
    Map<String,StorageEngine> storageEngineMap = storageEngineRegistry.getStorageEngineMap();
    Set<String> catalogs = storageEngineMap.keySet();
    catalogs.remove(OPENSEARCH);
    return catalogs;
  }

  @Override
  public void registerOpenSearchStorageEngine(StorageEngine storageEngine) {
    this.openSearchStorageEngine = storageEngine;
  }

}
