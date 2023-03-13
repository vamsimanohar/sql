/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.datasource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.encryptor.Encryptor;
import org.opensearch.sql.datasource.DataSourceMetadataStorage;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.auth.AuthenticationType;
import org.opensearch.sql.opensearch.security.SecurityAccess;

public class OpenSearchDataSourceMetadataStorage implements DataSourceMetadataStorage {

  public static final String DATASOURCE_INDEX_NAME = ".ql-datasources";
  private static final String DATASOURCE_INDEX_MAPPING_FILE_NAME = "datasources-index-mapping.yml";
  private static final String DATASOURCE_INDEX_SETTINGS_FILE_NAME
      = "datasources-index-settings.yml";
  private static final Logger LOG = LogManager.getLogger();
  private final Client client;
  private final ClusterService clusterService;

  private final Encryptor encryptor;

  /**
   * This class implements DataSourceMetadataStorage interface
   * using OpenSearch as underlying storage.
   *
   * @param client opensearch NodeClient.
   * @param clusterService ClusterService.
   * @param encryptor Encryptor.
   */
  public OpenSearchDataSourceMetadataStorage(Client client, ClusterService clusterService,
                                             Encryptor encryptor) {
    this.client = client;
    this.clusterService = clusterService;
    this.encryptor = encryptor;
  }

  @Override
  public List<DataSourceMetadata> getDataSourceMetadata() {
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
      return Collections.emptyList();
    }
    return searchInElasticSearch(QueryBuilders.matchAllQuery());
  }

  @Override
  public Optional<DataSourceMetadata> getDataSourceMetadata(String datasourceName) {
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    return searchInElasticSearch(QueryBuilders.termQuery("name", datasourceName))
        .stream()
        .findFirst()
        .map(this::decryptConfidentialInfo);
  }

  @Override
  public void createDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {
    encryptConfidentialData(dataSourceMetadata);
    if (!this.clusterService.state().routingTable().hasIndex(DATASOURCE_INDEX_NAME)) {
      createDataSourcesIndex();
    }
    IndexRequest indexRequest = new IndexRequest(DATASOURCE_INDEX_NAME);
    indexRequest.id(dataSourceMetadata.getName());
    indexRequest.create(true);
    ObjectMapper objectMapper = new ObjectMapper();
    ActionFuture<IndexResponse> indexResponseActionFuture;
    try (ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext()
        .stashContext()) {
      indexRequest.source(objectMapper.writeValueAsString(dataSourceMetadata), XContentType.JSON);
      indexResponseActionFuture = client.index(indexRequest);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    IndexResponse indexResponse = indexResponseActionFuture.actionGet();
    if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)) {
      LOG.debug("DatasourceMetadata : {}  successfully created", dataSourceMetadata.getName());
    }
  }

  @Override
  public void updateDataSourceMetadata(DataSourceMetadata dataSourceMetadata) {
    encryptConfidentialData(dataSourceMetadata);
    UpdateRequest updateRequest
        = new UpdateRequest(DATASOURCE_INDEX_NAME, dataSourceMetadata.getName());
    ObjectMapper objectMapper = new ObjectMapper();
    ActionFuture<UpdateResponse> updateResponseActionFuture;
    try (ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext()
        .stashContext()) {
      updateRequest.doc(objectMapper.writeValueAsString(dataSourceMetadata), XContentType.JSON);
      updateResponseActionFuture = client.update(updateRequest);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    UpdateResponse updateResponse = updateResponseActionFuture.actionGet();
    if (updateResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
      LOG.debug("DatasourceMetadata : {}  successfully updated", dataSourceMetadata.getName());
    }
  }

  @Override
  public void deleteDataSourceMetadata(String datasourceName) {
    DeleteRequest deleteRequest = new DeleteRequest(DATASOURCE_INDEX_NAME);
    deleteRequest.id(datasourceName);
    ActionFuture<DeleteResponse> deleteResponseActionFuture;
    try (ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext()
        .stashContext()) {
       deleteResponseActionFuture = client.delete(deleteRequest);
    }
    DeleteResponse deleteResponse = deleteResponseActionFuture.actionGet();
    if(deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)) {
      LOG.debug("DatasourceMetadata : {}  successfully deleted", datasourceName);
    }
  }

  private void createDataSourcesIndex() {
    try {
      InputStream mappingFileStream = OpenSearchDataSourceMetadataStorage.class.getClassLoader()
          .getResourceAsStream(DATASOURCE_INDEX_MAPPING_FILE_NAME);
      InputStream settingsFileStream = OpenSearchDataSourceMetadataStorage.class.getClassLoader()
          .getResourceAsStream(DATASOURCE_INDEX_SETTINGS_FILE_NAME);
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(DATASOURCE_INDEX_NAME);
      createIndexRequest
          .mapping(IOUtils.toString(mappingFileStream, StandardCharsets.UTF_8),
              XContentType.YAML)
          .settings(IOUtils.toString(settingsFileStream, StandardCharsets.UTF_8),
              XContentType.YAML);
      ActionFuture<CreateIndexResponse> createIndexResponseActionFuture;
      try (ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext()
          .stashContext()) {
        createIndexResponseActionFuture = client.admin().indices().create(createIndexRequest);
      }
      CreateIndexResponse createIndexResponse = createIndexResponseActionFuture.actionGet();
      if (createIndexResponse.isAcknowledged()) {
        LOG.info("Index: {} creation Acknowledged", DATASOURCE_INDEX_NAME);
      } else {
        throw new IllegalStateException(
            String.format("Index: %s creation failed", DATASOURCE_INDEX_NAME));
      }
    } catch (Throwable e) {
      throw new RuntimeException(
          "Internal server error while creating" + DATASOURCE_INDEX_NAME + " index");
    }
  }

  private List<DataSourceMetadata> searchInElasticSearch(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(DATASOURCE_INDEX_NAME);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    ActionFuture<SearchResponse> searchResponseActionFuture;
    try (ThreadContext.StoredContext storedContext = client.threadPool().getThreadContext()
        .stashContext()) {
      searchResponseActionFuture = client.search(searchRequest);
    }
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException(
          "Internal server error while fetching datasource metadata information");
    } else {
      ObjectMapper objectMapper = new ObjectMapper();
      return Arrays.stream(searchResponse
              .getHits()
              .getHits())
          .map(SearchHit::getSourceAsString)
          .map(datasourceString -> SecurityAccess.doPrivileged(
                    () -> objectMapper.readValue(datasourceString, DataSourceMetadata.class)))
          .collect(Collectors.toList());
    }
  }


  private DataSourceMetadata decryptConfidentialInfo(DataSourceMetadata dataSourceMetadata) {
    Map<String, String> propertiesMap = dataSourceMetadata.getProperties();
    Optional<AuthenticationType> authTypeOptional
        = propertiesMap.keySet().stream().filter(s -> s.endsWith("auth.type"))
        .findFirst()
        .map(propertiesMap::get)
        .map(AuthenticationType::get);
    switch (authTypeOptional.get()) {
      case BASICAUTH:
        Optional<String> passwordKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.password"))
            .findFirst();
        propertiesMap.put(passwordKey.get(),
            this.encryptor.decrypt(propertiesMap.get(passwordKey.get())));
        break;
      case AWSSIGV4AUTH:
        Optional<String> accessKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.access_key"))
            .findFirst();
        propertiesMap.put(accessKey.get(),
            this.encryptor.decrypt(propertiesMap.get(accessKey.get())));
        Optional<String> secretKey = propertiesMap.keySet().stream()
            .filter(s -> s.endsWith("auth.secret_key"))
            .findFirst();
        propertiesMap.put(secretKey.get(),
            this.encryptor.decrypt(propertiesMap.get(secretKey.get())));
        break;
      default:
        break;
    }
    return dataSourceMetadata;
  }


  private void encryptConfidentialData(DataSourceMetadata dataSourceMetadata) {
    Map<String, String> propertiesMap = dataSourceMetadata.getProperties();
    Optional<AuthenticationType> authTypeOptional
        = propertiesMap.keySet().stream().filter(s -> s.endsWith("auth.type"))
        .findFirst()
        .map(propertiesMap::get)
        .map(AuthenticationType::get);
    if (authTypeOptional.isPresent()) {
      switch (authTypeOptional.get()) {
        case BASICAUTH:
          Optional<String> usernameKey = propertiesMap.keySet().stream()
              .filter(s -> s.endsWith("auth.username"))
              .findFirst();
          propertiesMap.put(usernameKey.get(),
              this.encryptor.encrypt(propertiesMap.get(usernameKey.get())));
          Optional<String> passwordKey = propertiesMap.keySet().stream()
              .filter(s -> s.endsWith("auth.password"))
              .findFirst();
          propertiesMap.put(passwordKey.get(),
              this.encryptor.encrypt(propertiesMap.get(passwordKey.get())));
          break;
        case AWSSIGV4AUTH:
          Optional<String> accessKey = propertiesMap.keySet().stream()
              .filter(s -> s.endsWith("auth.access_key"))
              .findFirst();
          propertiesMap.put(accessKey.get(),
              this.encryptor.encrypt(propertiesMap.get(accessKey.get())));
          Optional<String> secretKey = propertiesMap.keySet().stream()
              .filter(s -> s.endsWith("auth.secret_key"))
              .findFirst();
          propertiesMap.put(secretKey.get(),
              this.encryptor.encrypt(propertiesMap.get(secretKey.get())));
          break;
        default:
          break;
      }
    }

  }

}