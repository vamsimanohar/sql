/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.xml.crypto.Data;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.datasource.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceInterfaceType;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.storage.DataSourceFactory;

/**
 * Default implementation of {@link DataSourceService}. It is per-jvm single instance.
 *
 * <p>{@link DataSourceService} is constructed by the list of {@link DataSourceFactory} at service
 * bootstrap time. The set of {@link DataSourceFactory} is immutable. Client could add {@link
 * DataSource} defined by {@link DataSourceMetadata} at any time. {@link DataSourceService} use
 * {@link DataSourceFactory} to create {@link DataSource}.
 */
public class DataSourceServiceImpl implements DataSourceService {

  private static String DATASOURCE_NAME_REGEX = "[@*A-Za-z]+?[*a-zA-Z_\\-0-9]*";

  private final DataSourceLoaderCache dataSourceLoaderCache;

  private final DataSourceMetadataStorage dataSourceMetadataStorage;

  private final DataSourceUserAuthorizationHelper dataSourceUserAuthorizationHelper;

  /**
   * Construct from the set of {@link DataSourceFactory} at bootstrap time.
   */
  public DataSourceServiceImpl(Set<DataSourceFactory> dataSourceFactories,
                               DataSourceMetadataStorage dataSourceMetadataStorage,
                               DataSourceUserAuthorizationHelper
                                   dataSourceUserAuthorizationHelper) {
    this.dataSourceMetadataStorage = dataSourceMetadataStorage;
    this.dataSourceUserAuthorizationHelper = dataSourceUserAuthorizationHelper;
    this.dataSourceLoaderCache = new DataSourceLoaderCacheImpl(dataSourceFactories);
  }

  @Override
  public Set<DataSourceMetadata> getDataSourceMetadata(boolean isDefaultDataSourceRequired) {
    List<DataSourceMetadata> dataSourceMetadataList
        = this.dataSourceMetadataStorage.getDataSourceMetadata();
    Set<DataSourceMetadata> dataSourceMetadataSet = new HashSet<>(dataSourceMetadataList);
    if (isDefaultDataSourceRequired) {
      dataSourceMetadataSet.add(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());
    }
    removeAuthInfo(dataSourceMetadataSet);
    return dataSourceMetadataSet;
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String datasourceName) {
    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = getDataSourceMetadataFromName(datasourceName);
    if (dataSourceMetadataOptional.isEmpty()) {
      throw new IllegalArgumentException("DataSource with name: " + datasourceName
          + " doesn't exist.");
    }
    removeAuthInfo(dataSourceMetadataOptional.get());
    return dataSourceMetadataOptional.get();
  }


  @Override
  public DataSource getDataSource(String dataSourceName) {
    Optional<DataSourceMetadata>
        dataSourceMetadataOptional = getDataSourceMetadataFromName(dataSourceName);
    if (dataSourceMetadataOptional.isEmpty()) {
      throw new DataSourceNotFoundException(
          String.format("DataSource with name %s doesn't exist.", dataSourceName));
    } else {
      DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
      this.dataSourceUserAuthorizationHelper
          .authorizeDataSource(dataSourceMetadata);
      return dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
    }
  }

  @Override
  public void createDataSource(DataSourceMetadata metadata) {
    validateDataSourceMetaData(metadata);
    if (!metadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      this.dataSourceLoaderCache.getOrLoadDataSource(metadata);
      this.dataSourceMetadataStorage.createDataSourceMetadata(metadata);
    }
  }

  @Override
  public void updateDataSource(DataSourceMetadata dataSourceMetadata) {
    validateDataSourceMetaData(dataSourceMetadata);
    if (!dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      this.dataSourceLoaderCache.getOrLoadDataSource(dataSourceMetadata);
      this.dataSourceMetadataStorage.updateDataSourceMetadata(dataSourceMetadata);
    } else {
      throw new UnsupportedOperationException(
          "Not allowed to update default datasource :" + DEFAULT_DATASOURCE_NAME);
    }
  }

  @Override
  public void deleteDataSource(String dataSourceName) {
    if (dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      throw new UnsupportedOperationException(
          "Not allowed to delete default datasource :" + DEFAULT_DATASOURCE_NAME);
    } else {
      this.dataSourceMetadataStorage.deleteDataSourceMetadata(dataSourceName);
    }
  }

  @Override
  public Boolean dataSourceExists(String dataSourceName) {
    return DEFAULT_DATASOURCE_NAME.equals(dataSourceName)
        || this.dataSourceMetadataStorage.getDataSourceMetadata(dataSourceName).isPresent();
  }

  @Override
  public DataSourceInterfaceType datasourceInterfaceType() {
    return DataSourceInterfaceType.API;
  }


  /**
   * This can be moved to a different validator class when we introduce more connectors.
   *
   * @param metadata {@link DataSourceMetadata}.
   */
  private void validateDataSourceMetaData(DataSourceMetadata metadata) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(metadata.getName()),
        "Missing Name Field from a DataSource. Name is a required parameter.");
    Preconditions.checkArgument(
        metadata.getName().matches(DATASOURCE_NAME_REGEX),
        StringUtils.format(
            "DataSource Name: %s contains illegal characters. Allowed characters: a-zA-Z0-9_-*@.",
            metadata.getName()));
    Preconditions.checkArgument(
        !Objects.isNull(metadata.getProperties()),
        "Missing properties field in datasource configuration."
            + " Properties are required parameters.");
  }

  private Optional<DataSourceMetadata> getDataSourceMetadataFromName(String dataSourceName) {
    if (dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      return Optional.of(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());
    } else {
      return this.dataSourceMetadataStorage.getDataSourceMetadata(dataSourceName);
    }
  }


  // It is advised to avoid sending any kind credential
  // info in api response from security point of view.
  private void removeAuthInfo(Set<DataSourceMetadata> dataSourceMetadataSet) {
    dataSourceMetadataSet.forEach(this::removeAuthInfo);
  }

  private void removeAuthInfo(DataSourceMetadata dataSourceMetadata) {
    HashMap<String, String> safeProperties
        = new HashMap<>(dataSourceMetadata.getProperties());
    safeProperties
        .entrySet()
        .removeIf(entry -> entry.getKey().contains("auth"));
    dataSourceMetadata.setProperties(safeProperties);
  }
}
