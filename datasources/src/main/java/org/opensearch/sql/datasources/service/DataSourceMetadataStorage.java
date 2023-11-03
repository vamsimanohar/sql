/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.datasources.service;

import java.util.List;
import java.util.Optional;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;

/**
 * Interface for DataSourceMetadata Storage which will be only used by DataSourceService for
 * Storage.
 */
public interface DataSourceMetadataStorage {

  /**
   * Returns all dataSource Metadata objects. The returned objects won't contain any of the
   * credential info.
   *
   * @return list of {@link DataSourceMetadata}.
   */
  List<DataSourceMetadata> getDataSourceMetadata();

  /**
   * Gets {@link DataSourceMetadata} corresponding to the datasourceName from underlying storage.
   *
   * @param datasourceName name of the {@link DataSource}.
   */
  Optional<DataSourceMetadata> getDataSourceMetadata(String datasourceName);

  /**
   * Stores {@link DataSourceMetadata} in underlying storage.
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}.
   */
  void createDataSourceMetadata(DataSourceMetadata dataSourceMetadata);

  /**
   * Updates {@link DataSourceMetadata} in underlying storage.
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}.
   */
  void updateDataSourceMetadata(DataSourceMetadata dataSourceMetadata);

  /**
   * Deletes {@link DataSourceMetadata} corresponding to the datasourceName from underlying storage.
   *
   * @param datasourceName name of the {@link DataSource}.
   */
  void deleteDataSourceMetadata(String datasourceName);

  /**
   * Creates query result index for the given datasource. We have introduced this method to handle
   * the issue: https://github.com/opensearch-project/sql/issues/2268 Once a long term solution is
   * finalized, we can make a decision to remove or have it.
   *
   * @param dataSourceMetadata {@link DataSourceMetadata}.
   */
  void createDataSourceResultIndex(DataSourceMetadata dataSourceMetadata);
}
