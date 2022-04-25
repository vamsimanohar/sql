/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.request.system;

import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.prometheus.client.OpenSearchClient.META_CLUSTER_NAME;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.data.type.OpenSearchDataType;
import org.opensearch.sql.prometheus.request.OpenSearchRequest;

/**
 * Describe index meta data request.
 */
public class PrometheusDescribeMetricRequest implements OpenSearchSystemRequest {

  private static final String DEFAULT_TABLE_CAT = "opensearch";

  private static final Integer DEFAULT_NUM_PREC_RADIX = 10;

  private static final Integer DEFAULT_NULLABLE = 2;

  private static final String DEFAULT_IS_AUTOINCREMENT = "NO";

  /**
   * Type mapping from OpenSearch data type to expression type in our type system in query
   * engine. TODO: geo, ip etc.
   */
  private static final Map<String, ExprType> OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING =
      ImmutableMap.<String, ExprType>builder()
          .put("text", OpenSearchDataType.OPENSEARCH_TEXT)
          .put("text_keyword", OpenSearchDataType.OPENSEARCH_TEXT_KEYWORD)
          .put("keyword", ExprCoreType.STRING)
          .put("byte", ExprCoreType.BYTE)
          .put("short", ExprCoreType.SHORT)
          .put("integer", ExprCoreType.INTEGER)
          .put("long", ExprCoreType.LONG)
          .put("float", ExprCoreType.FLOAT)
          .put("half_float", ExprCoreType.FLOAT)
          .put("scaled_float", ExprCoreType.DOUBLE)
          .put("double", ExprCoreType.DOUBLE)
          .put("boolean", ExprCoreType.BOOLEAN)
          .put("nested", ExprCoreType.ARRAY)
          .put("object", ExprCoreType.STRUCT)
          .put("date", ExprCoreType.TIMESTAMP)
          .put("date_nanos", ExprCoreType.TIMESTAMP)
          .put("ip", OpenSearchDataType.OPENSEARCH_IP)
          .put("geo_point", OpenSearchDataType.OPENSEARCH_GEO_POINT)
          .put("binary", OpenSearchDataType.OPENSEARCH_BINARY)
          .build();

  private final PrometheusClient iPrometheusService;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final OpenSearchRequest.IndexName indexName;

  public PrometheusDescribeMetricRequest(PrometheusClient iPrometheusService, String indexName) {
    this(iPrometheusService, new OpenSearchRequest.IndexName(indexName));
  }

  public PrometheusDescribeMetricRequest(PrometheusClient iPrometheusService,
                                         OpenSearchRequest.IndexName indexName) {
    this.iPrometheusService = iPrometheusService;
    this.indexName = indexName;
  }

  /**
   * search all the index in the data store.
   *
   * @return list of {@link ExprValue}
   */
  @Override
  public List<ExprValue> search() {
    return new ArrayList<>();
  }

  /**
   * Get the mapping of field and type.
   *
   * @return mapping of field and type.
   */
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();

    String[] labels = AccessController.doPrivileged((PrivilegedAction<String[]>)  ()-> {
      if(!indexName.getIndexNames()[0].toLowerCase().contains(".promql(")) {
        try {
          return iPrometheusService
                  .getLabels(indexName.getIndexNames()[0].split("\\.")[1]);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return null;
    });
    fieldTypes.put("@value", ExprCoreType.DOUBLE);
    fieldTypes.put("@timestamp", ExprCoreType.TIMESTAMP);
    fieldTypes.put("metric", ExprCoreType.STRING);
    if(labels!=null) {
      for (String label : labels) {
        fieldTypes.put(label, ExprCoreType.STRING);
      }
    }

    return fieldTypes;
  }

  private ExprType transformESTypeToExprType(String openSearchType) {
    return OPENSEARCH_TYPE_TO_EXPR_TYPE_MAPPING.getOrDefault(openSearchType, ExprCoreType.UNKNOWN);
  }

  private ExprTupleValue row(String fieldName, String fieldType, int position, String clusterName) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CAT", stringValue(clusterName));
    valueMap.put("TABLE_NAME", stringValue(indexName.toString()));
    valueMap.put("COLUMN_NAME", stringValue(fieldName));
    // todo
    valueMap.put("TYPE_NAME", stringValue(fieldType));
    valueMap.put("NUM_PREC_RADIX", integerValue(DEFAULT_NUM_PREC_RADIX));
    valueMap.put("NULLABLE", integerValue(DEFAULT_NULLABLE));
    // There is no deterministic position of column in table
    valueMap.put("ORDINAL_POSITION", integerValue(position));
    // TODO Defaulting to unknown, need to check this
    valueMap.put("IS_NULLABLE", stringValue(""));
    // Defaulting to "NO"
    valueMap.put("IS_AUTOINCREMENT", stringValue(DEFAULT_IS_AUTOINCREMENT));
    // TODO Defaulting to unknown, need to check
    valueMap.put("IS_GENERATEDCOLUMN", stringValue(""));
    return new ExprTupleValue(valueMap);
  }

  private String clusterName(Map<String, String> meta) {
    return meta.getOrDefault(META_CLUSTER_NAME, DEFAULT_TABLE_CAT);
  }

  @Override
  public String toString() {
    return "OpenSearchDescribeIndexRequest{"
        + "indexName='" + indexName + '\''
        + '}';
  }
}
