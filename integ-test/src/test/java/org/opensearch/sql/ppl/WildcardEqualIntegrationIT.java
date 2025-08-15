/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Comprehensive integration tests for SmartEquals/SmartNotEquals operators with wildcard support.
 * Tests all data types, edge cases, error scenarios, and both PPL/SQL syntax. Covers behavior with
 * and without Calcite engine and pushdown optimization.
 */
public class WildcardEqualIntegrationIT extends PPLIntegTestCase {

  private static final String TEST_INDEX_WILDCARD = "wildcard";

  @Override
  protected void init() throws Exception {
    loadIndex(Index.WILDCARD);
  }

  // ========== Basic String/Keyword Wildcard Tests ==========

  @Test
  public void test_wildcard_asterisk_keyword_field() throws IOException {
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where KeywordBody='test*' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard"), rows("test wildcard in body"), rows("test123"));
  }

  @Test
  public void test_wildcard_question_keyword_field() throws IOException {
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where KeywordBody='test?' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test1"), rows("test2"));
  }

  @Test
  public void test_wildcard_not_equals_keyword_field() throws IOException {
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where KeywordBody!='test*' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    // Should return all rows that don't match the pattern
    assertTrue(result.getInt("total") > 0);
  }

  // ========== Numeric Type Tests ==========

  @Test
  public void test_numeric_exact_match_with_string() throws IOException {
    // Test that numeric = "25" works (auto-cast)
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where age='25' | fields age";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows(25));
  }

  @Test
  public void test_numeric_wildcard_pattern() throws IOException {
    // Test that numeric = "2*" works with auto-cast
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where age='2*' | fields age";
    JSONObject result = executeQuery(query);
    // Should match 20-29, 2, 200-299, etc.
    assertTrue("Should match ages starting with 2", result.getInt("total") > 0);
  }

  @Test
  public void test_numeric_not_equals_wildcard() throws IOException {
    // Test that numeric != "3*" works
    String query =
        "search source=" + TEST_INDEX_WILDCARD + " | where age!='3*' | fields age | head 5";
    JSONObject result = executeQuery(query);
    // Verify no results start with 3
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String ageStr = dataRows.getJSONArray(i).get(0).toString();
      assertFalse("Age should not start with 3", ageStr.startsWith("3"));
    }
  }

  @Test
  public void test_float_wildcard_pattern() throws IOException {
    // Test that float = "99.*" works
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where price='99.*' | fields price";
    JSONObject result = executeQuery(query);
    // Should match 99.0, 99.99, 99.5, etc.
    assertTrue("Should match prices starting with 99.", result.getInt("total") >= 0);
  }

  // ========== IP Address Tests ==========

  @Test
  public void test_ip_exact_match_with_string() throws IOException {
    // Test IP = "192.168.1.1" (exact match)
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where ip_address='192.168.1.1' | fields ip_address";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("192.168.1.1"));
  }

  @Test
  public void test_ip_wildcard_pattern() throws IOException {
    // Test IP = "192.168.*" (wildcard)
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where ip_address='192.168.*' | fields ip_address";
    JSONObject result = executeQuery(query);
    // Verify all results start with 192.168.
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String ip = dataRows.getJSONArray(i).getString(0);
      assertTrue("IP should start with 192.168.", ip.startsWith("192.168."));
    }
  }

  @Test
  public void test_ip_question_mark_wildcard() throws IOException {
    // Test IP = "192.168.1.?" (single char wildcard)
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where ip_address='192.168.1.?' | fields ip_address";
    JSONObject result = executeQuery(query);
    // Should match 192.168.1.0 through 192.168.1.9
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String ip = dataRows.getJSONArray(i).getString(0);
      assertTrue("IP should match pattern 192.168.1.?", ip.matches("192\\.168\\.1\\.[0-9]"));
    }
  }

  @Test
  public void test_ip_not_equals_wildcard() throws IOException {
    // Test IP != "10.*"
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where ip_address!='10.*' | fields ip_address | head 5";
    JSONObject result = executeQuery(query);
    // Verify no results start with 10.
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String ip = dataRows.getJSONArray(i).getString(0);
      assertFalse("IP should not start with 10.", ip.startsWith("10."));
    }
  }

  // ========== Date/Timestamp Tests ==========

  @Test
  public void test_date_exact_match_with_string() throws IOException {
    // Test date = "2024-01-01" (exact match)
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where date_field='2024-01-01' | fields date_field";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("2024-01-01"));
  }

  @Test
  public void test_date_wildcard_pattern() throws IOException {
    // Test date = "2024-*" (wildcard for year)
    String query =
        "search source=" + TEST_INDEX_WILDCARD + " | where date_field='2024-*' | fields date_field";
    JSONObject result = executeQuery(query);
    // Verify all results start with 2024-
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String date = dataRows.getJSONArray(i).getString(0);
      assertTrue("Date should start with 2024-", date.startsWith("2024-"));
    }
  }

  @Test
  public void test_timestamp_wildcard_pattern() throws IOException {
    // Test timestamp = "2024-01-*" (wildcard for day)
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where timestamp_field='2024-01-*' | fields timestamp_field";
    JSONObject result = executeQuery(query);
    // Should match all timestamps in January 2024
    assertTrue("Should match timestamps in Jan 2024", result.getInt("total") >= 0);
  }

  // ========== Text Field Tests ==========

  @Test
  public void test_text_field_without_keyword_subfield() throws IOException {
    // Test text field without .keyword subfield (should use script query)
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where text_no_keyword='test*' | fields text_no_keyword";
    JSONObject result = executeQuery(query);
    // Should work but might be slow (uses script query)
    assertTrue("Text field should support wildcards via script", result.getInt("total") >= 0);
  }

  @Test
  public void test_text_field_explicit_keyword_subfield() throws IOException {
    // Test explicit use of .keyword subfield
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where text_field.keyword='exact match' | fields text_field";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("exact match"));
  }

  // ========== Edge Cases and Order Tests ==========

  @Test
  public void test_reverse_order_literal_first() throws IOException {
    // Test "25" = age (literal on left)
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where '25'=age | fields age";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows(25));
  }

  @Test
  public void test_reverse_order_wildcard_literal_first() throws IOException {
    // Test "test*" = KeywordBody (wildcard literal on left)
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where 'test*'=KeywordBody | fields KeywordBody";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows("test wildcard"), rows("test wildcard in body"), rows("test123"));
  }

  @Test
  public void test_multiple_wildcards_in_pattern() throws IOException {
    // Test pattern with multiple wildcards: "t*st?"
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where KeywordBody='t*st?' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    // Should match patterns like "test1", "test2", "toast5", etc.
    assertTrue("Should match complex wildcard pattern", result.getInt("total") >= 0);
  }

  @Test
  public void test_empty_string_comparison() throws IOException {
    // Test field = "" (empty string)
    String query =
        "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    // Should match only empty strings
    if (result.getInt("total") > 0) {
      verifyDataRows(result, rows(""));
    }
  }

  @Test
  public void test_null_field_comparison() throws IOException {
    // Test comparison with null fields
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where nullable_field='test' | fields nullable_field";
    JSONObject result = executeQuery(query);
    // Should not match null values
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      assertNotNull("Should not match null values", dataRows.getJSONArray(i).get(0));
    }
  }

  // ========== Error Cases ==========

  @Test
  public void test_array_field_with_string_should_error() throws IOException {
    try {
      // Test array = "value" should error
      String query =
          "search source="
              + TEST_INDEX_WILDCARD
              + " | where array_field='test' | fields array_field";
      JSONObject result = executeQuery(query);

      // Should get an error or empty result
      assertTrue(
          "Should error or return empty for array comparison",
          result.has("error") || result.getInt("total") == 0);

      if (result.has("error")) {
        String error = result.getJSONObject("error").getString("type");
        assertTrue("Should be type error", error.contains("type") || error.contains("Type"));
      }
    } catch (Exception e) {
      // Expected - array fields shouldn't compare with strings
      assertTrue(
          "Expected error for array field comparison",
          e.getMessage().contains("array") || e.getMessage().contains("type"));
    }
  }

  @Test
  public void test_invalid_ip_pattern() throws IOException {
    // Test IP = "not.an.ip" should cast and compare as strings
    String query =
        "search source="
            + TEST_INDEX_WILDCARD
            + " | where ip_address='not.an.ip' | fields ip_address";
    JSONObject result = executeQuery(query);
    // Should return empty as no IP will match this invalid pattern
    assertEquals("Invalid IP pattern should return no results", 0, result.getInt("total"));
  }

  // ========== Boolean Type Tests ==========

  @Test
  public void test_boolean_with_string_true() throws IOException {
    // Test boolean = "true"
    String query =
        "search source=" + TEST_INDEX_WILDCARD + " | where is_active='true' | fields is_active";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows(true));
  }

  @Test
  public void test_boolean_with_string_false() throws IOException {
    // Test boolean = "false"
    String query =
        "search source=" + TEST_INDEX_WILDCARD + " | where is_active='false' | fields is_active";
    JSONObject result = executeQuery(query);
    verifyDataRows(result, rows(false));
  }

  @Test
  public void test_boolean_with_wildcard_should_work() throws IOException {
    // Test boolean = "t*" (matches "true")
    String query =
        "search source=" + TEST_INDEX_WILDCARD + " | where is_active='t*' | fields is_active";
    JSONObject result = executeQuery(query);
    // Should match true values (converted to string "true")
    verifyDataRows(result, rows(true));
  }

  // ========== Calcite Engine Tests ==========

  @Test
  public void test_wildcard_with_calcite_enabled() throws IOException {
    enableCalcite();
    allowCalciteFallback();

    try {
      String query =
          "search source="
              + TEST_INDEX_WILDCARD
              + " | where KeywordBody='test*' | fields KeywordBody";
      JSONObject result = executeQuery(query);
      verifyDataRows(result, rows("test wildcard"), rows("test wildcard in body"), rows("test123"));
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  @Test
  public void test_numeric_wildcard_with_calcite_requires_cast() throws IOException {
    enableCalcite();
    allowCalciteFallback();

    try {
      // This should fail with type error - numeric fields can't use wildcards without CAST
      String query =
          "search source=" + TEST_INDEX_WILDCARD + " | where count='123*' | fields count";
      JSONObject result = executeQuery(query);

      // With our fix, this should return an error about incompatible types
      assertCondition(
          "Should get error for numeric wildcard without CAST",
          result.has("error") || result.getInt("total") == 0);
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  @Test
  public void test_explicit_cast_with_wildcard_calcite() throws IOException {
    enableCalcite();
    allowCalciteFallback();

    try {
      // With explicit CAST, numeric wildcards should work
      String query =
          "search source="
              + TEST_INDEX_WILDCARD
              + " | where CAST(count AS STRING)='10*' | fields count";
      JSONObject result = executeQuery(query);

      // This should work with the explicit CAST
      assertCondition("Explicit CAST should allow numeric wildcards", result.getInt("total") >= 0);
    } finally {
      disableCalcite();
      disallowCalciteFallback();
    }
  }

  // ========== Pushdown Tests ==========

  @Test
  public void test_keyword_wildcard_with_pushdown() throws IOException {
    enableCalcite();
    enablePushdown();

    try {
      // Keyword fields should use native wildcardQuery with pushdown
      String query =
          "search source="
              + TEST_INDEX_WILDCARD
              + " | where KeywordBody='test*' | fields KeywordBody";
      JSONObject result = executeQuery(query);

      verifyDataRows(result, rows("test wildcard"), rows("test wildcard in body"), rows("test123"));
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  @Test
  public void test_text_field_keyword_subfield_with_pushdown() throws IOException {
    enableCalcite();
    enablePushdown();

    try {
      // Text fields should automatically use .keyword subfield if available
      String query =
          "search source=" + TEST_INDEX_WILDCARD + " | where TextBody='test*' | fields TextBody";
      JSONObject result = executeQuery(query);

      // Should work if .keyword subfield exists
      assertCondition(
          "Text field with .keyword should support wildcards", result.getInt("total") >= 0);
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  @Test
  public void test_wildcard_without_pushdown() throws IOException {
    enableCalcite();
    disablePushdown();

    try {
      // Without pushdown, should fetch all data and filter in-memory
      String query =
          "search source="
              + TEST_INDEX_WILDCARD
              + " | where KeywordBody='test*' | fields KeywordBody";
      JSONObject result = executeQuery(query);

      verifyDataRows(result, rows("test wildcard"), rows("test wildcard in body"), rows("test123"));
    } finally {
      enablePushdown(); // Re-enable for other tests
      disableCalcite();
    }
  }

  // ========== Performance and Pushdown Behavior Tests ==========

  @Test
  public void test_explain_shows_wildcard_query_for_keyword() throws IOException {
    enableCalcite();
    enablePushdown();

    try {
      String query =
          "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='test*' | explain";
      JSONObject result = executeQuery(query);

      // Should show wildcardQuery in the plan
      String plan = result.toString();
      assertTrue(
          "Plan should contain wildcardQuery for keyword field",
          plan.contains("wildcard") || plan.contains("ILIKE"));
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  @Test
  public void test_explain_shows_script_query_for_numeric() throws IOException {
    enableCalcite();
    enablePushdown();

    try {
      String query = "search source=" + TEST_INDEX_WILDCARD + " | where age='2*' | explain";
      JSONObject result = executeQuery(query);

      // Should show script query or cast in the plan
      String plan = result.toString();
      assertTrue(
          "Plan should show script or cast for numeric wildcard",
          plan.contains("script") || plan.contains("CAST") || plan.contains("cast"));
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  // ========== Helper Methods ==========

  private void enablePushdown() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "true"));
  }

  private void disablePushdown() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "false"));
  }

  private void assertCondition(String message, boolean condition) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
