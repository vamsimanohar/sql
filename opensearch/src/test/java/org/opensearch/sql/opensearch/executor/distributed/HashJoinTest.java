/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class HashJoinTest {

  private DistributedTaskScheduler scheduler;

  @BeforeEach
  void setUp() {
    scheduler =
        new DistributedTaskScheduler(
            mock(TransportService.class), mock(ClusterService.class), mock(Client.class));
  }

  // ===== INNER JOIN =====

  @Test
  void inner_join_returns_only_matching_rows() {
    List<List<Object>> left = rows(row(1L, "Alice"), row(2L, "Bob"), row(3L, "Carol"));
    List<List<Object>> right = rows(row(1L, "TX"), row(2L, "CA"), row(4L, "NY"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.INNER, 2, 2);

    assertEquals(2, result.size());
    assertEquals(List.of(1L, "Alice", 1L, "TX"), result.get(0));
    assertEquals(List.of(2L, "Bob", 2L, "CA"), result.get(1));
  }

  @Test
  void inner_join_with_duplicate_keys_produces_cross_product() {
    List<List<Object>> left = rows(row(1L, "A"), row(1L, "B"));
    List<List<Object>> right = rows(row(1L, "X"), row(1L, "Y"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.INNER, 2, 2);

    // 2 left x 2 right = 4 rows
    assertEquals(4, result.size());
  }

  // ===== LEFT JOIN =====

  @Test
  void left_join_includes_unmatched_left_rows_with_nulls() {
    List<List<Object>> left = rows(row(1L, "Alice"), row(2L, "Bob"), row(3L, "Carol"));
    List<List<Object>> right = rows(row(1L, "TX"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.LEFT, 2, 2);

    assertEquals(3, result.size());
    assertEquals(List.of(1L, "Alice", 1L, "TX"), result.get(0));
    // Bob has no match -> right side nulls
    assertEquals(Arrays.asList(2L, "Bob", null, null), result.get(1));
    assertEquals(Arrays.asList(3L, "Carol", null, null), result.get(2));
  }

  // ===== RIGHT JOIN =====

  @Test
  void right_join_includes_unmatched_right_rows_with_nulls() {
    List<List<Object>> left = rows(row(1L, "Alice"));
    List<List<Object>> right = rows(row(1L, "TX"), row(2L, "CA"), row(3L, "NY"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.RIGHT, 2, 2);

    assertEquals(3, result.size());
    // Matched: Alice + TX
    assertEquals(List.of(1L, "Alice", 1L, "TX"), result.get(0));
    // Unmatched right rows: nulls + right
    assertEquals(Arrays.asList(null, null, 2L, "CA"), result.get(1));
    assertEquals(Arrays.asList(null, null, 3L, "NY"), result.get(2));
  }

  // ===== SEMI JOIN =====

  @Test
  void semi_join_returns_left_rows_with_match_only_left_columns() {
    List<List<Object>> left = rows(row(1L, "Alice"), row(2L, "Bob"), row(3L, "Carol"));
    List<List<Object>> right = rows(row(1L, "TX"), row(1L, "CA"), row(3L, "NY"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.SEMI, 2, 2);

    // Semi join: only left columns, one row per left even if multiple right matches
    assertEquals(2, result.size());
    assertEquals(List.of(1L, "Alice"), result.get(0));
    assertEquals(List.of(3L, "Carol"), result.get(1));
  }

  // ===== ANTI JOIN =====

  @Test
  void anti_join_returns_left_rows_with_no_match() {
    List<List<Object>> left = rows(row(1L, "Alice"), row(2L, "Bob"), row(3L, "Carol"));
    List<List<Object>> right = rows(row(1L, "TX"), row(3L, "NY"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.ANTI, 2, 2);

    assertEquals(1, result.size());
    assertEquals(List.of(2L, "Bob"), result.get(0));
  }

  // ===== FULL JOIN =====

  @Test
  void full_join_includes_unmatched_rows_from_both_sides() {
    List<List<Object>> left = rows(row(1L, "Alice"), row(2L, "Bob"));
    List<List<Object>> right = rows(row(2L, "CA"), row(3L, "NY"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.FULL, 2, 2);

    assertEquals(3, result.size());
    // Alice: no match -> left + nulls
    assertEquals(Arrays.asList(1L, "Alice", null, null), result.get(0));
    // Bob + CA: matched
    assertEquals(List.of(2L, "Bob", 2L, "CA"), result.get(1));
    // NY: no match -> nulls + right
    assertEquals(Arrays.asList(null, null, 3L, "NY"), result.get(2));
  }

  // ===== NULL KEY HANDLING =====

  @Test
  void null_keys_never_match_in_inner_join() {
    List<List<Object>> left = rows(row(null, "Alice"), row(1L, "Bob"));
    List<List<Object>> right = rows(row(null, "TX"), row(1L, "CA"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.INNER, 2, 2);

    // Only Bob(1) matches CA(1); nulls don't match
    assertEquals(1, result.size());
    assertEquals(List.of(1L, "Bob", 1L, "CA"), result.get(0));
  }

  @Test
  void null_keys_preserved_in_left_join() {
    List<List<Object>> left = rows(row(null, "Alice"), row(1L, "Bob"));
    List<List<Object>> right = rows(row(1L, "CA"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.LEFT, 2, 2);

    assertEquals(2, result.size());
    // Alice has null key -> unmatched with right nulls
    assertEquals(Arrays.asList(null, "Alice", null, null), result.get(0));
    assertEquals(List.of(1L, "Bob", 1L, "CA"), result.get(1));
  }

  // ===== COMPOSITE KEY =====

  @Test
  void composite_key_join_matches_on_multiple_columns() {
    List<List<Object>> left = rows(row(1L, "A", "data1"), row(1L, "B", "data2"));
    List<List<Object>> right = rows(row(1L, "A", "right1"), row(1L, "C", "right2"));

    // Join on columns 0 and 1
    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0, 1), keys(0, 1), JoinRelType.INNER, 3, 3);

    assertEquals(1, result.size());
    assertEquals(List.of(1L, "A", "data1", 1L, "A", "right1"), result.get(0));
  }

  // ===== EMPTY TABLE =====

  @Test
  void inner_join_with_empty_left_returns_empty() {
    List<List<Object>> left = rows();
    List<List<Object>> right = rows(row(1L, "TX"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.INNER, 2, 2);

    assertTrue(result.isEmpty());
  }

  @Test
  void inner_join_with_empty_right_returns_empty() {
    List<List<Object>> left = rows(row(1L, "Alice"));
    List<List<Object>> right = rows();

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.INNER, 2, 2);

    assertTrue(result.isEmpty());
  }

  @Test
  void left_join_with_empty_right_returns_all_left_with_nulls() {
    List<List<Object>> left = rows(row(1L, "Alice"), row(2L, "Bob"));
    List<List<Object>> right = rows();

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.LEFT, 2, 2);

    assertEquals(2, result.size());
    assertEquals(Arrays.asList(1L, "Alice", null, null), result.get(0));
    assertEquals(Arrays.asList(2L, "Bob", null, null), result.get(1));
  }

  @Test
  void right_join_with_empty_left_returns_all_right_with_nulls() {
    List<List<Object>> left = rows();
    List<List<Object>> right = rows(row(1L, "TX"), row(2L, "CA"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.RIGHT, 2, 2);

    assertEquals(2, result.size());
    assertEquals(Arrays.asList(null, null, 1L, "TX"), result.get(0));
    assertEquals(Arrays.asList(null, null, 2L, "CA"), result.get(1));
  }

  // ===== TYPE COERCION =====

  @Test
  void integer_and_long_keys_match_after_normalization() {
    // Left side has Integer keys, right side has Long keys
    List<List<Object>> left = rows(row(1, "Alice"), row(2, "Bob"));
    List<List<Object>> right = rows(row(1L, "TX"), row(2L, "CA"));

    List<List<Object>> result =
        scheduler.performHashJoin(left, right, keys(0), keys(0), JoinRelType.INNER, 2, 2);

    // Both should match after normalization (Integer → Long)
    assertEquals(2, result.size());
  }

  // ===== EXTRACT JOIN KEY =====

  @Test
  void extract_join_key_single_column() {
    List<Object> row = row(42L, "Alice");
    Object key = scheduler.extractJoinKey(row, keys(0));
    assertEquals(42L, key);
  }

  @Test
  void extract_join_key_composite() {
    List<Object> row = row(42L, "Alice", "data");
    Object key = scheduler.extractJoinKey(row, keys(0, 1));
    assertEquals(List.of(42L, "Alice"), key);
  }

  @Test
  void extract_join_key_returns_null_for_null_value() {
    List<Object> row = row(null, "Alice");
    Object key = scheduler.extractJoinKey(row, keys(0));
    assertEquals(null, key);
  }

  // ===== Helpers =====

  private static List<Object> row(Object... values) {
    List<Object> r = new ArrayList<>(values.length);
    for (Object v : values) {
      r.add(v);
    }
    return r;
  }

  private static List<List<Object>> rows(List<Object>... rowArray) {
    List<List<Object>> result = new ArrayList<>();
    for (List<Object> r : rowArray) {
      result.add(r);
    }
    return result;
  }

  private static List<Integer> keys(int... indices) {
    List<Integer> result = new ArrayList<>();
    for (int i : indices) {
      result.add(i);
    }
    return result;
  }
}
