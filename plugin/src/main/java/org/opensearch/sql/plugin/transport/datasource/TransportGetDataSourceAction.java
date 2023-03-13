/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.transport.datasource;

import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.Collections;
import java.util.HashSet;
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
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.model.GetDataSourceActionRequest;
import org.opensearch.sql.plugin.model.GetDataSourceActionResponse;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportGetDataSourceAction
    extends HandledTransportAction<GetDataSourceActionRequest, GetDataSourceActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/datasources/read";
  public static final ActionType<GetDataSourceActionResponse>
      ACTION_TYPE = new ActionType<>(NAME, GetDataSourceActionResponse::new);

  private DataSourceService dataSourceService;
  private Client client;

  @Inject
  public TransportGetDataSourceAction(TransportService transportService,
                                      ActionFilters actionFilters,
                                      NodeClient client,
                                      DataSourceServiceImpl dataSourceService) {
    super(TransportGetDataSourceAction.NAME, transportService, actionFilters,
        GetDataSourceActionRequest::new);
    this.client = client;
    this.dataSourceService = dataSourceService;
  }

  @Override
  protected void doExecute(Task task, GetDataSourceActionRequest request,
                           ActionListener<GetDataSourceActionResponse> actionListener) {
    try {
      Set<DataSourceMetadata> dataSourceMetadataSet;
      if (request.getDataSourceName() == null) {
        dataSourceMetadataSet =
            SecurityAccess.doPrivileged(() -> dataSourceService.getDataSourceMetadataSet(false));
      } else {
        DataSourceMetadata dataSourceMetadata
            = SecurityAccess.doPrivileged(() -> dataSourceService
            .getDataSourceMetadataSet(request.getDataSourceName()));
        dataSourceMetadataSet = Collections.singleton(dataSourceMetadata);
      }
      String responseContent =
          new JsonResponseFormatter<Set<DataSourceMetadata>>(PRETTY) {
            @Override
            protected Object buildJsonObject(Set<DataSourceMetadata> response) {
              return response;
            }
          }.format(dataSourceMetadataSet);
      actionListener.onResponse(new GetDataSourceActionResponse(responseContent));
    } catch (Exception e) {
      actionListener.onFailure(e);
    }

  }
}