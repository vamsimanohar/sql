package org.opensearch.sql.datasource;

import org.opensearch.sql.datasource.model.DataSourceMetadata;

public interface DataSourceAuthorizer {
  void authorize(DataSourceMetadata dataSourceMetadata);

}
