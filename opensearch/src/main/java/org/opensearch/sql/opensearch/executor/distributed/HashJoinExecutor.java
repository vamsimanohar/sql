/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.rel.core.JoinRelType;

/**
 * Hash join algorithm: build, probe, combine rows for all join types. Also handles post-join
 * filtering and sorting. All methods are stateless — static.
 */
@Log4j2
public final class HashJoinExecutor {

  private HashJoinExecutor() {}

  /**
   * Performs a hash join between left and right row sets. Builds a hash table on the right side
   * (build side) and probes with the left side (probe side).
   *
   * <p>Supports: INNER, LEFT, RIGHT, FULL, SEMI, ANTI join types. NULL keys never match (SQL
   * semantics).
   */
  public static List<List<Object>> performHashJoin(
      List<List<Object>> leftRows,
      List<List<Object>> rightRows,
      List<Integer> leftKeyIndices,
      List<Integer> rightKeyIndices,
      JoinRelType joinType,
      int leftFieldCount,
      int rightFieldCount) {

    Map<Object, List<List<Object>>> hashTable = buildHashTable(rightRows, rightKeyIndices);

    List<List<Object>> result = new ArrayList<>();
    Set<Integer> matchedRightIndices = new HashSet<>();

    for (List<Object> leftRow : leftRows) {
      Object leftKey = extractJoinKey(leftRow, leftKeyIndices);

      if (leftKey == null) {
        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
          result.add(combineRowsWithNullRight(leftRow, rightFieldCount));
        } else if (joinType == JoinRelType.ANTI) {
          result.add(new ArrayList<>(leftRow));
        }
        continue;
      }

      List<List<Object>> matchingRightRows = hashTable.get(leftKey);
      boolean hasMatch = matchingRightRows != null && !matchingRightRows.isEmpty();

      switch (joinType) {
        case INNER -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
              trackMatchedRightRows(rightRows, rightRow, matchedRightIndices);
            }
          }
        }
        case LEFT -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
            }
          } else {
            result.add(combineRowsWithNullRight(leftRow, rightFieldCount));
          }
        }
        case RIGHT -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
              trackMatchedRightRows(rightRows, rightRow, matchedRightIndices);
            }
          }
        }
        case FULL -> {
          if (hasMatch) {
            for (List<Object> rightRow : matchingRightRows) {
              result.add(combineRows(leftRow, rightRow));
              trackMatchedRightRows(rightRows, rightRow, matchedRightIndices);
            }
          } else {
            result.add(combineRowsWithNullRight(leftRow, rightFieldCount));
          }
        }
        case SEMI -> {
          if (hasMatch) {
            result.add(new ArrayList<>(leftRow));
          }
        }
        case ANTI -> {
          if (!hasMatch) {
            result.add(new ArrayList<>(leftRow));
          }
        }
        default -> throw new UnsupportedOperationException("Unsupported join type: " + joinType);
      }
    }

    // For RIGHT and FULL joins: emit unmatched right rows
    if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
      for (int i = 0; i < rightRows.size(); i++) {
        if (!matchedRightIndices.contains(i)) {
          result.add(combineRowsWithNullLeft(leftFieldCount, rightRows.get(i)));
        }
      }
    }

    return result;
  }

  /**
   * Builds a hash table from the given rows using the specified key indices. Rows with null keys
   * are excluded (never match during probe).
   */
  static Map<Object, List<List<Object>>> buildHashTable(
      List<List<Object>> rows, List<Integer> keyIndices) {
    Map<Object, List<List<Object>>> hashTable = new HashMap<>();
    for (List<Object> row : rows) {
      Object key = extractJoinKey(row, keyIndices);
      if (key != null) {
        hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
      }
    }
    return hashTable;
  }

  /**
   * Extracts the join key from a row. For single-column keys, returns the normalized value. For
   * composite keys (multiple columns), returns a List of normalized values. Returns null if any key
   * column is null.
   */
  static Object extractJoinKey(List<Object> row, List<Integer> keyIndices) {
    if (keyIndices.size() == 1) {
      int idx = keyIndices.get(0);
      Object val = idx < row.size() ? row.get(idx) : null;
      return normalizeJoinKeyValue(val);
    }

    List<Object> compositeKey = new ArrayList<>(keyIndices.size());
    for (int idx : keyIndices) {
      Object val = idx < row.size() ? row.get(idx) : null;
      if (val == null) {
        return null;
      }
      compositeKey.add(normalizeJoinKeyValue(val));
    }
    return compositeKey;
  }

  /**
   * Normalizes a join key value for consistent hash/equals behavior. Converts all integer numeric
   * types to Long and Float to Double.
   */
  static Object normalizeJoinKeyValue(Object val) {
    if (val == null) {
      return null;
    }
    if (val instanceof Integer || val instanceof Short || val instanceof Byte) {
      return ((Number) val).longValue();
    }
    if (val instanceof Float) {
      return ((Float) val).doubleValue();
    }
    return val;
  }

  /** Combines a left row and right row into a single joined row (left + right). */
  static List<Object> combineRows(List<Object> leftRow, List<Object> rightRow) {
    List<Object> combined = new ArrayList<>(leftRow.size() + rightRow.size());
    combined.addAll(leftRow);
    combined.addAll(rightRow);
    return combined;
  }

  /** Creates a joined row with left data and nulls for right side (used in LEFT/FULL joins). */
  static List<Object> combineRowsWithNullRight(List<Object> leftRow, int rightFieldCount) {
    List<Object> combined = new ArrayList<>(leftRow.size() + rightFieldCount);
    combined.addAll(leftRow);
    combined.addAll(Collections.nCopies(rightFieldCount, null));
    return combined;
  }

  /** Creates a joined row with nulls for left side and right data (used in RIGHT/FULL joins). */
  static List<Object> combineRowsWithNullLeft(int leftFieldCount, List<Object> rightRow) {
    List<Object> combined = new ArrayList<>(leftFieldCount + rightRow.size());
    combined.addAll(Collections.nCopies(leftFieldCount, null));
    combined.addAll(rightRow);
    return combined;
  }

  /**
   * Tracks the index of a matched right row for RIGHT/FULL join. Finds the row by reference in the
   * original list.
   */
  static void trackMatchedRightRows(
      List<List<Object>> rightRows, List<Object> matchedRow, Set<Integer> matchedIndices) {
    for (int i = 0; i < rightRows.size(); i++) {
      if (rightRows.get(i) == matchedRow) {
        matchedIndices.add(i);
      }
    }
  }

  // ========== Post-join operations ==========

  /**
   * Applies post-join filter conditions on the coordinator. Evaluates each row against the filter
   * conditions and returns only matching rows.
   */
  public static List<List<Object>> applyPostJoinFilters(
      List<List<Object>> rows, List<Map<String, Object>> filters, List<String> fieldNames) {
    List<List<Object>> filtered = new ArrayList<>();
    for (List<Object> row : rows) {
      if (matchesFilters(row, filters, fieldNames)) {
        filtered.add(row);
      }
    }
    return filtered;
  }

  /** Evaluates whether a row matches all filter conditions. */
  @SuppressWarnings("unchecked")
  static boolean matchesFilters(
      List<Object> row, List<Map<String, Object>> filters, List<String> fieldNames) {
    for (Map<String, Object> filter : filters) {
      String field = (String) filter.get("field");
      String op = (String) filter.get("op");
      Object filterValue = filter.get("value");

      int fieldIndex = fieldNames.indexOf(field);
      if (fieldIndex < 0 || fieldIndex >= row.size()) {
        return false;
      }

      Object rowValue = row.get(fieldIndex);
      if (rowValue == null) {
        return false;
      }

      int cmp;
      if (rowValue instanceof Comparable && filterValue instanceof Comparable) {
        try {
          cmp = ((Comparable<Object>) rowValue).compareTo(filterValue);
        } catch (ClassCastException e) {
          if (rowValue instanceof Number && filterValue instanceof Number) {
            cmp =
                Double.compare(
                    ((Number) rowValue).doubleValue(), ((Number) filterValue).doubleValue());
          } else {
            cmp = rowValue.toString().compareTo(filterValue.toString());
          }
        }
      } else {
        cmp = rowValue.toString().compareTo(filterValue.toString());
      }

      boolean passes =
          switch (op) {
            case "EQ" -> cmp == 0;
            case "NEQ" -> cmp != 0;
            case "GT" -> cmp > 0;
            case "GTE" -> cmp >= 0;
            case "LT" -> cmp < 0;
            case "LTE" -> cmp <= 0;
            default -> true;
          };

      if (!passes) {
        return false;
      }
    }
    return true;
  }

  /**
   * Sorts merged rows on the coordinator using the extracted sort keys. Uses a Comparator chain
   * that handles null values and ascending/descending direction.
   */
  @SuppressWarnings("unchecked")
  public static void sortRows(List<List<Object>> rows, List<SortKey> sortKeys) {
    if (sortKeys.isEmpty() || rows.size() <= 1) {
      return;
    }

    Comparator<List<Object>> comparator =
        (row1, row2) -> {
          for (SortKey key : sortKeys) {
            Object v1 = key.fieldIndex() < row1.size() ? row1.get(key.fieldIndex()) : null;
            Object v2 = key.fieldIndex() < row2.size() ? row2.get(key.fieldIndex()) : null;

            if (v1 == null && v2 == null) {
              continue;
            }
            if (v1 == null) {
              return key.nullsLast() ? 1 : -1;
            }
            if (v2 == null) {
              return key.nullsLast() ? -1 : 1;
            }

            int cmp;
            if (v1 instanceof Comparable && v2 instanceof Comparable) {
              try {
                cmp = ((Comparable<Object>) v1).compareTo(v2);
              } catch (ClassCastException e) {
                cmp = v1.toString().compareTo(v2.toString());
              }
            } else {
              cmp = v1.toString().compareTo(v2.toString());
            }

            if (cmp != 0) {
              return key.descending() ? -cmp : cmp;
            }
          }
          return 0;
        };

    rows.sort(comparator);
  }
}
