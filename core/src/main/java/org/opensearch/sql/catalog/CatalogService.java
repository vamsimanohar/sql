package org.opensearch.sql.catalog;

import java.util.Optional;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.storage.StorageEngine;


public interface CatalogService {


  Optional<StorageEngine> getStorageEngine(String catalog);
  
  Optional<ExecutionEngine> getExecutionEngine(String catalog);

}