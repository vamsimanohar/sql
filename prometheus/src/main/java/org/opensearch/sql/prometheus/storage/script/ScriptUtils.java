/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.script;

import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.data.type.OpenSearchDataType;

/**
 * Script Utils.
 */
@UtilityClass
public class ScriptUtils {

  /**
   * Text field doesn't have doc value (exception thrown even when you call "get")
   * Limitation: assume inner field name is always "keyword".
   */
  public static String convertTextToKeyword(String fieldName, ExprType fieldType) {
    if (fieldType == OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD) {
      return fieldName + ".keyword";
    }
    return fieldName;
  }
}
