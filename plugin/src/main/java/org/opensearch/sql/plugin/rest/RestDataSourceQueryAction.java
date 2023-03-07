/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestRequest.Method.POST;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.model.CreateDataSourceActionRequest;
import org.opensearch.sql.plugin.model.CreateDataSourceActionResponse;
import org.opensearch.sql.plugin.transport.datasource.TransportCreateDataSourceAction;

public class RestDataSourceQueryAction extends BaseRestHandler {

  public static final String DATASOURCE_ACTIONS = "datasource_actions";
  public static final String BASE_DATASOURCE_ACTION_URL = "/_plugins/_query/_datasources";

  @Override
  public String getName() {
    return DATASOURCE_ACTIONS;
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(

        /*
         *
         * Create a new datasource.
         * Request URL: POST
         * Request body:
         * Ref [org.opensearch.sql.plugin.transport.datasource.model.CreateDataSourceActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.plugin.transport.datasource.model.CreateDataSourceActionResponse]
         */
        new Route(POST, BASE_DATASOURCE_ACTION_URL)
    );
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) {
    switch (restRequest.method()) {
      case POST:
        return executePostRequest(restRequest, nodeClient);
      default:
        return restChannel
            -> restChannel.sendResponse(new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED,
            String.valueOf(restRequest.method())));
    }
  }

  private RestChannelConsumer executePostRequest(RestRequest restRequest,
                                                 NodeClient nodeClient) {
    ObjectMapper objectMapper = new ObjectMapper();
    DataSourceMetadata dataSourceMetadata
        = SecurityAccess.doPrivileged(() ->
          objectMapper.readValue(restRequest.content().utf8ToString(),
          DataSourceMetadata.class));
    return restChannel -> nodeClient.execute(TransportCreateDataSourceAction.ACTION_TYPE,
        new CreateDataSourceActionRequest(dataSourceMetadata),
        new ActionListener<>() {
          @Override
          public void onResponse(CreateDataSourceActionResponse createDataSourceActionResponse) {
            restChannel.sendResponse(
                new BytesRestResponse(RestStatus.OK, "application/json; charset=UTF-8",
                    createDataSourceActionResponse.getResult()));
          }

          @Override
          public void onFailure(Exception e) {
            restChannel.sendResponse(
                new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR,
                    "application/json; charset=UTF-8",
                    e.getMessage()));
          }
        });
  }

}