/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.datasource.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.datasource.model.DataSource;
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

  private final ConcurrentHashMap<DataSourceMetadata, DataSource> dataSourceMap;

  private final Map<DataSourceType, DataSourceFactory> dataSourceFactoryMap;

  private final DataSourceMetadataStorage dataSourceMetadataStorage;

  private final DataSourceUserAuthorizationHelper dataSourceUserAuthorizationHelper;

  /**
   * Construct from the set of {@link DataSourceFactory} at bootstrap time.
   */
  public DataSourceServiceImpl(Set<DataSourceFactory> dataSourceFactories,
                               DataSourceMetadataStorage dataSourceMetadataStorage,
                               DataSourceUserAuthorizationHelper
                                   dataSourceUserAuthorizationHelper) {
    dataSourceFactoryMap =
        dataSourceFactories.stream()
            .collect(Collectors.toMap(DataSourceFactory::getDataSourceType, f -> f));
    dataSourceMap = new ConcurrentHashMap<>();
    this.dataSourceMetadataStorage = dataSourceMetadataStorage;
    this.dataSourceUserAuthorizationHelper = dataSourceUserAuthorizationHelper;
  }

  @Override
  public Set<DataSourceMetadata> getDataSourceMetadataSet(Boolean isDefaultRequired) {
    List<DataSourceMetadata> dataSourceMetadataList
        = this.dataSourceMetadataStorage.getDataSourceMetadata();
    Set<DataSourceMetadata> dataSourceMetadataSet = new HashSet<>(dataSourceMetadataList);
    if (isDefaultRequired) {
      dataSourceMetadataSet.add(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());
    }
    return removeAuthInfo(dataSourceMetadataSet);
  }

  @Override
  public DataSourceMetadata getDataSourceMetadataSet(String datasourceName) {
    Optional<DataSourceMetadata> dataSourceMetadataOptional
        = this.dataSourceMetadataStorage.getDataSourceMetadata(datasourceName);
    if (dataSourceMetadataOptional.isEmpty()) {
      throw new IllegalArgumentException("DataSource with name: " + datasourceName
          + " doesn't exist.");
    }
    return removeAuthInfo(dataSourceMetadataOptional.get());
  }


  @Override
  public DataSource getDataSource(String dataSourceName) {
    Optional<DataSourceMetadata>
        dataSourceMetadataOptional = getDataSourceMetadata(dataSourceName);
    if (dataSourceMetadataOptional.isEmpty()) {
      throw new DataSourceNotFoundException(
          String.format("DataSource with name %s doesn't exist.", dataSourceName));
    } else {
      DataSourceMetadata dataSourceMetadata = dataSourceMetadataOptional.get();
      authorizeDataSource(dataSourceMetadata);
      return getDataSourceFromMetadata(dataSourceMetadata);
    }
  }

  @Override
  public void createDataSource(DataSourceMetadata metadata) {
    validateDataSourceMetaData(metadata);
    if (!metadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      this.dataSourceMetadataStorage.createDataSourceMetadata(metadata);
    }
    dataSourceMap.put(metadata,
        dataSourceFactoryMap.get(metadata.getConnector()).createDataSource(metadata));
  }

  @Override
  public void updateDataSource(DataSourceMetadata dataSourceMetadata) {
    validateDataSourceMetaData(dataSourceMetadata);
    if (dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      throw new UnsupportedOperationException(
          "Not allowed to update default datasource :" + DEFAULT_DATASOURCE_NAME);
    } else {
      this.dataSourceMetadataStorage.updateDataSourceMetadata(dataSourceMetadata);
    }
    clearDataSourceWithSameName(dataSourceMetadata.getName());
    dataSourceMap.put(dataSourceMetadata,
        dataSourceFactoryMap.get(dataSourceMetadata.getConnector())
            .createDataSource(dataSourceMetadata));
  }

  @Override
  public void deleteDataSource(String dataSourceName) {
    if (dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      throw new UnsupportedOperationException(
          "Not allowed to delete default datasource :" + DEFAULT_DATASOURCE_NAME);
    } else {
      this.dataSourceMetadataStorage.deleteDataSourceMetadata(dataSourceName);
      clearDataSourceWithSameName(dataSourceName);
    }
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
    this.dataSourceFactoryMap.get(metadata.getConnector())
        .validateDataSourceConfigProperties(metadata.getProperties());
  }

  private Optional<DataSourceMetadata> getDataSourceMetadata(String dataSourceName) {
    if (dataSourceName.equals(DEFAULT_DATASOURCE_NAME)) {
      return Optional.of(DataSourceMetadata.defaultOpenSearchDataSourceMetadata());
    } else {
      return this.dataSourceMetadataStorage.getDataSourceMetadata(dataSourceName);
    }
  }

  private DataSource getDataSourceFromMetadata(DataSourceMetadata dataSourceMetadata) {
    if (!dataSourceMap.containsKey(dataSourceMetadata)) {
      clearDataSourceWithSameName(dataSourceMetadata.getName());
      dataSourceMap.put(dataSourceMetadata,
          dataSourceFactoryMap.get(dataSourceMetadata.getConnector())
              .createDataSource(dataSourceMetadata));
    }
    return dataSourceMap.get(dataSourceMetadata);
  }

  private void clearDataSourceWithSameName(String dataSourceName) {
    dataSourceMap.entrySet()
        .removeIf(entry -> entry.getKey().getName().equals(dataSourceName));
  }

  private void authorizeDataSource(DataSourceMetadata dataSourceMetadata) {
    if (this.dataSourceUserAuthorizationHelper.isAuthorizationRequired()
        && !dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      boolean isAuthorized = false;
      for (String role : this.dataSourceUserAuthorizationHelper.getUserRoles()) {
        if (dataSourceMetadata.getAllowedRoles().contains(role)
            || role.equals("all_access")) {
          isAuthorized = true;
          break;
        }
      }
      if (!isAuthorized) {
        throw new SecurityException(
            String.format("User is not authorized to access datasource %s. "
                    + "User should be mapped to any of the roles in %s for access.",
                dataSourceMetadata.getName(), dataSourceMetadata.getAllowedRoles().toString()));
      }
    }
  }


  private Set<DataSourceMetadata> removeAuthInfo(Set<DataSourceMetadata> dataSourceMetadataSet) {
    return dataSourceMetadataSet.stream()
        .map(this::removeAuthInfo)
        .collect(Collectors.toSet());
  }

  private DataSourceMetadata removeAuthInfo(DataSourceMetadata dataSourceMetadata) {
    HashMap<String, String> filteredPropertiesMap = new HashMap<>();
    dataSourceMetadata.getProperties().entrySet()
      .stream()
      .filter(entry -> !entry.getKey().contains("auth"))
      .forEach(entry -> filteredPropertiesMap.put(entry.getKey(), entry.getValue()));
    DataSourceMetadata result = new DataSourceMetadata();
    result.setName(dataSourceMetadata.getName());
    result.setConnector(dataSourceMetadata.getConnector());
    result.setAllowedRoles(dataSourceMetadata.getAllowedRoles());
    result.setProperties(filteredPropertiesMap);
    return result;
  }

}
