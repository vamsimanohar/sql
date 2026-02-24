/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprIpValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.ExecutionEngine.Schema;
import org.opensearch.sql.executor.ExecutionEngine.Schema.Column;

/**
 * Builds {@link QueryResponse} from JDBC {@link ResultSet} using schema from RelNode. Uses {@link
 * TemporalValueNormalizer} for date conversion and handles ArrayImpl to List conversion.
 */
@Log4j2
public final class QueryResponseBuilder {

  private QueryResponseBuilder() {}

  /**
   * Builds a QueryResponse from a JDBC ResultSet. Reads all rows and maps them to ExprValue tuples
   * using the original RelNode's output field names for column naming.
   */
  public static QueryResponse buildQueryResponseFromResultSet(ResultSet rs, RelNode originalRelNode)
      throws Exception {
    ResultSetMetaData metaData = rs.getMetaData();
    int columnCount = metaData.getColumnCount();

    List<String> outputFieldNames = originalRelNode.getRowType().getFieldNames();
    List<RelDataTypeField> fieldTypes = originalRelNode.getRowType().getFieldList();

    // Pre-compute which columns are time-based or IP type
    int precomputeLen = Math.min(columnCount, fieldTypes.size());
    boolean[] isTimeBased = new boolean[precomputeLen];
    boolean[] isIpType = new boolean[precomputeLen];
    ExprType[] resolvedTypes = new ExprType[precomputeLen];
    for (int i = 0; i < precomputeLen; i++) {
      RelDataType relType = fieldTypes.get(i).getType();
      isTimeBased[i] = OpenSearchTypeFactory.isTimeBasedType(relType);
      if (relType.getSqlTypeName() != SqlTypeName.ANY) {
        resolvedTypes[i] = OpenSearchTypeFactory.convertRelDataTypeToExprType(relType);
        isIpType[i] = resolvedTypes[i] == ExprCoreType.IP;
      }
    }

    // Read all rows
    List<ExprValue> values = new ArrayList<>();
    while (rs.next()) {
      Map<String, ExprValue> exprRow = new LinkedHashMap<>();
      for (int i = 0; i < columnCount && i < outputFieldNames.size(); i++) {
        Object val = rs.getObject(i + 1); // JDBC is 1-indexed
        // Handle Calcite ArrayImpl (from take(), arrays in aggregation/patterns output)
        if (val instanceof java.sql.Array) {
          try {
            Object arrayData = ((java.sql.Array) val).getArray();
            if (arrayData instanceof Object[] objArr) {
              List<Object> list = new ArrayList<>(objArr.length);
              for (Object elem : objArr) {
                if (elem instanceof java.sql.Array nestedArr) {
                  Object nestedData = nestedArr.getArray();
                  if (nestedData instanceof Object[] nestedObjArr) {
                    List<Object> nestedList = new ArrayList<>(nestedObjArr.length);
                    Collections.addAll(nestedList, nestedObjArr);
                    list.add(nestedList);
                  } else {
                    list.add(elem);
                  }
                } else {
                  list.add(elem);
                }
              }
              val = list;
            }
          } catch (Exception e) {
            log.warn("[Distributed Engine] Failed to convert SQL Array: {}", e.getMessage());
          }
        }
        if (i < isTimeBased.length && isTimeBased[i] && val != null) {
          exprRow.put(
              outputFieldNames.get(i),
              TemporalValueNormalizer.convertToTimestampExprValue(val, resolvedTypes[i]));
        } else if (i < isIpType.length && isIpType[i] && val instanceof String) {
          exprRow.put(outputFieldNames.get(i), new ExprIpValue((String) val));
        } else {
          exprRow.put(outputFieldNames.get(i), ExprValueUtils.fromObjectValue(val));
        }
      }
      values.add(ExprTupleValue.fromExprValueMap(exprRow));
    }

    // Build schema from original RelNode row type
    List<Column> columns = new ArrayList<>();
    for (int i = 0; i < fieldTypes.size(); i++) {
      RelDataTypeField field = fieldTypes.get(i);
      ExprType exprType;
      if (field.getType().getSqlTypeName() == SqlTypeName.ANY) {
        if (!values.isEmpty()) {
          ExprValue firstVal = values.getFirst().tupleValue().get(field.getName());
          exprType = firstVal != null ? firstVal.type() : ExprCoreType.UNDEFINED;
        } else {
          exprType = ExprCoreType.UNDEFINED;
        }
      } else {
        exprType = OpenSearchTypeFactory.convertRelDataTypeToExprType(field.getType());
      }
      columns.add(new Column(field.getName(), null, exprType));
    }

    Schema schema = new Schema(columns);
    return new QueryResponse(schema, values, null);
  }
}
