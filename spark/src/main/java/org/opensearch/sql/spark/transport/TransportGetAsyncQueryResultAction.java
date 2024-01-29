/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport;

import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Guice;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.Module;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryExecutionResponse;
import org.opensearch.sql.spark.asyncquery.model.AsyncQueryResult;
import org.opensearch.sql.spark.transport.config.AsyncExecutorServiceModule;
import org.opensearch.sql.spark.transport.format.AsyncQueryResultResponseFormatter;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetAsyncQueryResultAction
    extends HandledTransportAction<
        GetAsyncQueryResultActionRequest, GetAsyncQueryResultActionResponse> {

  private final Injector injector;

  public static final String NAME = "cluster:admin/opensearch/ql/async_query/result";
  public static final ActionType<GetAsyncQueryResultActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, GetAsyncQueryResultActionResponse::new);

  @Inject
  public TransportGetAsyncQueryResultAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService) {
    super(NAME, transportService, actionFilters, GetAsyncQueryResultActionRequest::new);
    List<Module> modules = new ArrayList<>();
    modules.add(
        b -> {
          b.bind(NodeClient.class).toInstance(client);
          b.bind(org.opensearch.sql.common.setting.Settings.class)
              .toInstance(new OpenSearchSettings(clusterService.getClusterSettings()));
          b.bind(DataSourceService.class).toInstance(dataSourceService);
        });
    modules.add(new AsyncExecutorServiceModule());
    this.injector = Guice.createInjector();
  }

  @Override
  protected void doExecute(
      Task task,
      GetAsyncQueryResultActionRequest request,
      ActionListener<GetAsyncQueryResultActionResponse> listener) {
    try {
      String jobId = request.getQueryId();
      AsyncQueryExecutorService asyncQueryExecutorService =
          injector.getInstance(AsyncQueryExecutorService.class);
      AsyncQueryExecutionResponse asyncQueryExecutionResponse =
          asyncQueryExecutorService.getAsyncQueryResults(jobId);
      ResponseFormatter<AsyncQueryResult> formatter =
          new AsyncQueryResultResponseFormatter(JsonResponseFormatter.Style.PRETTY);
      String responseContent =
          formatter.format(
              new AsyncQueryResult(
                  asyncQueryExecutionResponse.getStatus(),
                  asyncQueryExecutionResponse.getSchema(),
                  asyncQueryExecutionResponse.getResults(),
                  Cursor.None,
                  asyncQueryExecutionResponse.getError()));
      listener.onResponse(new GetAsyncQueryResultActionResponse(responseContent));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
