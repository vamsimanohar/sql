/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.operator;

import java.util.List;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;

/**
 * Converts serialized filter conditions to Lucene queries using the local shard's field mappings.
 *
 * <p>Each filter condition is a Map with keys:
 *
 * <ul>
 *   <li>"field" (String) - field name
 *   <li>"op" (String) - operator: EQ, NEQ, GT, GTE, LT, LTE
 *   <li>"value" (Object) - comparison value
 * </ul>
 *
 * <p>Multiple conditions are combined with AND. The converter uses MapperService to resolve field
 * types and creates appropriate Lucene queries:
 *
 * <ul>
 *   <li>Keyword fields: TermQuery / TermRangeQuery
 *   <li>Numeric fields: LongPoint / IntPoint / DoublePoint range queries
 *   <li>Text fields: TermQuery on the field directly
 * </ul>
 */
@Log4j2
public class FilterToLuceneConverter {

  private FilterToLuceneConverter() {}

  /**
   * Converts a list of filter conditions to a single Lucene query.
   *
   * @param conditions filter conditions (null or empty means match all)
   * @param mapperService the shard's mapper service for field type resolution
   * @return a Lucene Query
   */
  public static Query convert(List<Map<String, Object>> conditions, MapperService mapperService) {
    if (conditions == null || conditions.isEmpty()) {
      return new MatchAllDocsQuery();
    }

    if (conditions.size() == 1) {
      return convertSingle(conditions.get(0), mapperService);
    }

    // Multiple conditions: AND them together
    BooleanQuery.Builder bool = new BooleanQuery.Builder();
    for (Map<String, Object> condition : conditions) {
      Query q = convertSingle(condition, mapperService);
      bool.add(q, BooleanClause.Occur.FILTER);
    }
    return bool.build();
  }

  private static Query convertSingle(Map<String, Object> condition, MapperService mapperService) {
    String field = (String) condition.get("field");
    String op = (String) condition.get("op");
    Object value = condition.get("value");

    if (field == null || op == null) {
      log.warn("[Filter] Invalid filter condition: {}", condition);
      return new MatchAllDocsQuery();
    }

    // Resolve field type from shard mapping
    MappedFieldType fieldType = mapperService.fieldType(field);
    if (fieldType == null) {
      // Try with .keyword suffix for text fields
      fieldType = mapperService.fieldType(field + ".keyword");
      if (fieldType != null) {
        field = field + ".keyword";
      } else {
        log.warn("[Filter] Field '{}' not found in mapping, skipping filter", field);
        return new MatchAllDocsQuery();
      }
    }

    log.debug(
        "[Filter] Converting: field={}, op={}, value={}, fieldType={}",
        field,
        op,
        value,
        fieldType.getClass().getSimpleName());

    return switch (op) {
      case "EQ" -> buildEqualityQuery(field, value, fieldType);
      case "NEQ" -> buildNegationQuery(buildEqualityQuery(field, value, fieldType));
      case "GT" -> buildRangeQuery(field, value, fieldType, false, false);
      case "GTE" -> buildRangeQuery(field, value, fieldType, true, false);
      case "LT" -> buildRangeQuery(field, value, fieldType, false, true);
      case "LTE" -> buildRangeQuery(field, value, fieldType, true, true);
      default -> {
        log.warn("[Filter] Unknown operator: {}", op);
        yield new MatchAllDocsQuery();
      }
    };
  }

  private static Query buildEqualityQuery(String field, Object value, MappedFieldType fieldType) {
    if (fieldType instanceof NumberFieldMapper.NumberFieldType numType) {
      return buildNumericExactQuery(field, value, numType);
    } else if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
      return new TermQuery(new Term(field, value.toString()));
    } else if (fieldType instanceof TextFieldMapper.TextFieldType) {
      // For text fields, use the analyzed field
      return new TermQuery(new Term(field, value.toString().toLowerCase()));
    } else {
      // Generic fallback: term query
      return new TermQuery(new Term(field, value.toString()));
    }
  }

  private static Query buildNegationQuery(Query inner) {
    BooleanQuery.Builder bool = new BooleanQuery.Builder();
    bool.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
    bool.add(inner, BooleanClause.Occur.MUST_NOT);
    return bool.build();
  }

  /**
   * Builds a range query for the given field and value.
   *
   * @param inclusive whether the bound is inclusive (>= or <=)
   * @param isUpper if true, value is the upper bound; if false, value is the lower bound
   */
  private static Query buildRangeQuery(
      String field, Object value, MappedFieldType fieldType, boolean inclusive, boolean isUpper) {

    if (fieldType instanceof NumberFieldMapper.NumberFieldType numType) {
      return buildNumericRangeQuery(field, value, numType, inclusive, isUpper);
    } else if (fieldType instanceof KeywordFieldMapper.KeywordFieldType) {
      return buildKeywordRangeQuery(field, value, inclusive, isUpper);
    } else {
      // Generic fallback: keyword range
      return buildKeywordRangeQuery(field, value, inclusive, isUpper);
    }
  }

  private static Query buildNumericExactQuery(
      String field, Object value, NumberFieldMapper.NumberFieldType numType) {
    String typeName = numType.typeName();
    return switch (typeName) {
      case "long" -> LongPoint.newExactQuery(field, toLong(value));
      case "integer" -> IntPoint.newExactQuery(field, toInt(value));
      case "double" -> DoublePoint.newExactQuery(field, toDouble(value));
      case "float" -> FloatPoint.newExactQuery(field, toFloat(value));
      default -> LongPoint.newExactQuery(field, toLong(value));
    };
  }

  private static Query buildNumericRangeQuery(
      String field,
      Object value,
      NumberFieldMapper.NumberFieldType numType,
      boolean inclusive,
      boolean isUpper) {
    String typeName = numType.typeName();
    return switch (typeName) {
      case "long" -> buildLongRange(field, toLong(value), inclusive, isUpper);
      case "integer" -> buildIntRange(field, toInt(value), inclusive, isUpper);
      case "double" -> buildDoubleRange(field, toDouble(value), inclusive, isUpper);
      case "float" -> buildFloatRange(field, toFloat(value), inclusive, isUpper);
      default -> buildLongRange(field, toLong(value), inclusive, isUpper);
    };
  }

  private static Query buildLongRange(
      String field, long value, boolean inclusive, boolean isUpper) {
    if (isUpper) {
      long upper = inclusive ? value : value - 1;
      return LongPoint.newRangeQuery(field, Long.MIN_VALUE, upper);
    } else {
      long lower = inclusive ? value : value + 1;
      return LongPoint.newRangeQuery(field, lower, Long.MAX_VALUE);
    }
  }

  private static Query buildIntRange(String field, int value, boolean inclusive, boolean isUpper) {
    if (isUpper) {
      int upper = inclusive ? value : value - 1;
      return IntPoint.newRangeQuery(field, Integer.MIN_VALUE, upper);
    } else {
      int lower = inclusive ? value : value + 1;
      return IntPoint.newRangeQuery(field, lower, Integer.MAX_VALUE);
    }
  }

  private static Query buildDoubleRange(
      String field, double value, boolean inclusive, boolean isUpper) {
    if (isUpper) {
      double upper = inclusive ? value : Math.nextDown(value);
      return DoublePoint.newRangeQuery(field, Double.NEGATIVE_INFINITY, upper);
    } else {
      double lower = inclusive ? value : Math.nextUp(value);
      return DoublePoint.newRangeQuery(field, lower, Double.POSITIVE_INFINITY);
    }
  }

  private static Query buildFloatRange(
      String field, float value, boolean inclusive, boolean isUpper) {
    if (isUpper) {
      float upper = inclusive ? value : Math.nextDown(value);
      return FloatPoint.newRangeQuery(field, Float.NEGATIVE_INFINITY, upper);
    } else {
      float lower = inclusive ? value : Math.nextUp(value);
      return FloatPoint.newRangeQuery(field, lower, Float.POSITIVE_INFINITY);
    }
  }

  private static Query buildKeywordRangeQuery(
      String field, Object value, boolean inclusive, boolean isUpper) {
    BytesRef bytesVal = new BytesRef(value.toString());
    if (isUpper) {
      return new TermRangeQuery(field, null, bytesVal, true, inclusive);
    } else {
      return new TermRangeQuery(field, bytesVal, null, inclusive, true);
    }
  }

  private static long toLong(Object value) {
    if (value instanceof Number n) return n.longValue();
    return Long.parseLong(value.toString());
  }

  private static int toInt(Object value) {
    if (value instanceof Number n) return n.intValue();
    return Integer.parseInt(value.toString());
  }

  private static double toDouble(Object value) {
    if (value instanceof Number n) return n.doubleValue();
    return Double.parseDouble(value.toString());
  }

  private static float toFloat(Object value) {
    if (value instanceof Number n) return n.floatValue();
    return Float.parseFloat(value.toString());
  }
}
