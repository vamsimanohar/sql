package org.opensearch.sql.plugin.rest;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.legacy.utils.LogUtils;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.plugin.catalog.CatalogServiceImpl;

public class RestCatalogQueryAction extends BaseRestHandler {

  public static final String CATALOGS_API_ENDPOINT = "/_plugins/_catalogs";

  @Override
  public String getName() {
    return "catalog_query_action";
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) {
    LogUtils.addRequestId();
    return channel -> sendResponse(channel, RestStatus.OK,
        CatalogServiceImpl.getInstance().getCatalogs().toString());
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(new Route(RestRequest.Method.GET, CATALOGS_API_ENDPOINT));
  }

  private void sendResponse(RestChannel channel, RestStatus status, String content) {
    channel.sendResponse(new BytesRestResponse(status, "application/json; charset=UTF-8", content));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }

}
