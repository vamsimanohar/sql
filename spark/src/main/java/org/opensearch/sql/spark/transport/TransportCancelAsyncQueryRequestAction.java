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
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.spark.asyncquery.AsyncQueryExecutorService;
import org.opensearch.sql.spark.transport.config.AsyncExecutorServiceModule;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportCancelAsyncQueryRequestAction
    extends HandledTransportAction<CancelAsyncQueryActionRequest, CancelAsyncQueryActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/ql/async_query/delete";
  private final Injector injector;
  public static final ActionType<CancelAsyncQueryActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, CancelAsyncQueryActionResponse::new);

  @Inject
  public TransportCancelAsyncQueryRequestAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService) {
    super(NAME, transportService, actionFilters, CancelAsyncQueryActionRequest::new);
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
      CancelAsyncQueryActionRequest request,
      ActionListener<CancelAsyncQueryActionResponse> listener) {
    try {
      AsyncQueryExecutorService asyncQueryExecutorService =
          injector.getInstance(AsyncQueryExecutorService.class);
      String jobId = asyncQueryExecutorService.cancelQuery(request.getQueryId());
      listener.onResponse(
          new CancelAsyncQueryActionResponse(
              String.format("Deleted async query with id: %s", jobId)));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
