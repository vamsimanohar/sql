/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import com.google.gson.Gson;
import lombok.Data;

@Data
public class IndexDetails {
  private String indexName;
  private FullyQualifiedTableName fullyQualifiedTableName;

  @Override
  public String toString() {
    return new Gson().toJson(this);
  }
}
