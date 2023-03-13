/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.model;


import java.io.IOException;
import lombok.Getter;
import org.json.JSONObject;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;

public class UpdateDataSourceActionRequest
    extends ActionRequest {

  @Getter
  private DataSourceMetadata dataSourceMetadata;

  /** Constructor of UpdateDataSourceActionRequest from StreamInput. */
  public UpdateDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public UpdateDataSourceActionRequest(DataSourceMetadata dataSourceMetadata) {
    this.dataSourceMetadata = dataSourceMetadata;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}