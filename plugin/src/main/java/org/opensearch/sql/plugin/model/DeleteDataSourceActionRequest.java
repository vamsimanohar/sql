/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.model;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

public class DeleteDataSourceActionRequest extends ActionRequest {

  @Getter
  private String datasourceName;

  /** Constructor of DeleteDataSourceActionRequest from StreamInput. */
  public DeleteDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public DeleteDataSourceActionRequest(String datasourceName) {
    this.datasourceName = datasourceName;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

}