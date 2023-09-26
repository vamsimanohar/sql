/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher.model;

import com.google.gson.Gson;
import java.util.Arrays;
import lombok.Data;

@Data
public class FullyQualifiedTableName {
  private String datasourceName;
  private String schemaName;
  private String tableName;

  public FullyQualifiedTableName(String fullyQualifiedName) {
    String[] parts = fullyQualifiedName.split("\\.");
    if (parts.length >= 3) {
      datasourceName = parts[0];
      schemaName = parts[1];
      tableName = String.join(".", Arrays.copyOfRange(parts, 2, parts.length));
    } else if (parts.length == 2) {
      schemaName = parts[0];
      tableName = parts[1];
    } else if (parts.length == 1) {
      tableName = parts[0];
    }
  }

  @Override
  public String toString() {
    return new Gson().toJson(this);
  }
}
