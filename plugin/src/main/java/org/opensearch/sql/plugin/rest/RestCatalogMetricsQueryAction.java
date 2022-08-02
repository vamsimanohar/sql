package org.opensearch.sql.plugin.rest;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.legacy.utils.LogUtils;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.catalog.CatalogServiceImpl;

public class RestCatalogMetricsQueryAction extends BaseRestHandler{

  public static final String CATALOG_NAME_PARAMETER = "catalogName";
  public static final String CATALOG_METRICS_API_ENDPOINT = String.format("/_plugins/_catalogs/{%s}/_metrics", CATALOG_NAME_PARAMETER);


  @Override
  public String getName() {
    return "catalog_metrics_query_action";
  }

  @Override
  protected BaseRestHandler.RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) {
    LogUtils.addRequestId();
    System.out.println(restRequest.toString());
    String catalogName = restRequest.param(CATALOG_NAME_PARAMETER);
    return channel -> sendResponse(channel, RestStatus.OK,
        doPrivileged(() -> CatalogServiceImpl.getInstance().getStorageEngine(catalogName).get().getTableNames()).toString());
  }

  @Override
  public List<RestHandler.Route> routes() {
    return ImmutableList.of(new RestHandler.Route(RestRequest.Method.GET, CATALOG_METRICS_API_ENDPOINT));
  }

  private void sendResponse(RestChannel channel, RestStatus status, String content) {
    channel.sendResponse(new BytesRestResponse(status, "application/json; charset=UTF-8", content));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

}
