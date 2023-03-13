/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.model;

import java.io.IOException;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;

@NoArgsConstructor
public class GetDataSourceActionRequest extends ActionRequest {

  @Getter
  private String dataSourceName;

  /** Constructor of GetDataSourceActionRequest from StreamInput. */
  public GetDataSourceActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public GetDataSourceActionRequest(String dataSourceName) {
    this.dataSourceName = dataSourceName;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

}