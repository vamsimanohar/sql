/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.transport.datasource;

import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.Set;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.DataSourceServiceImpl;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.model.CreateDataSourceActionResponse;
import org.opensearch.sql.plugin.model.DeleteDataSourceActionRequest;
import org.opensearch.sql.plugin.model.DeleteDataSourceActionResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportDeleteDataSourceAction
    extends HandledTransportAction<DeleteDataSourceActionRequest, DeleteDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/datasources/delete";
  public static final ActionType<DeleteDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, DeleteDataSourceActionResponse::new);

  private DataSourceService dataSourceService;
  private Client client;

  @Inject
  public TransportDeleteDataSourceAction(TransportService transportService,
                                      ActionFilters actionFilters,
                                      NodeClient client,
                                      DataSourceServiceImpl dataSourceService) {
    super(TransportDeleteDataSourceAction.NAME, transportService, actionFilters,
        DeleteDataSourceActionRequest::new);
    this.client = client;
    this.dataSourceService = dataSourceService;
  }

  @Override
  protected void doExecute(Task task, DeleteDataSourceActionRequest request,
                           ActionListener<DeleteDataSourceActionResponse> actionListener) {
    Metrics.getInstance().getNumericalMetric(MetricName.DATASOURCE_REQ_COUNT).increment();
    SecurityAccess.doPrivileged(() -> {
      dataSourceService.deleteDataSource(request.getDatasourceName());
      return null;
    });
    actionListener.onResponse(new DeleteDataSourceActionResponse("Deleted DataSource with name "
        + request.getDatasourceName()));
  }

}