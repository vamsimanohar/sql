/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.junit.Assert.*;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.common.setting.Settings;

/**
 * Comprehensive integration tests for SmartEquals/SmartNotEquals operators.
 *
 * Test Coverage:
 * 1. String fields with wildcard patterns
 * 2. Numeric fields with string comparison and wildcards
 * 3. Both orders (field=value, value=field)
 * 4. NOT EQUALS operator
 * 5. Edge cases and error scenarios
 * 6. Performance characteristics (pushdown vs in-memory)
 * 7. Calcite engine interaction
 */
public class SmartEqualsComprehensiveIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    // Load the predefined test indices
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.WILDCARD);
  }

  // ========== STRING/KEYWORD WILDCARD TESTS USING ACCOUNT INDEX ==========

  @Test
  public void test_string_asterisk_wildcard() throws IOException {
    // Test firstname='A*' to match names starting with A
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname='A*' | fields firstname";
    JSONObject result = executeQuery(query);
    
    // Should match: Amber (and any other names starting with A)
    JSONArray dataRows = result.getJSONArray("datarows");
    assertTrue("Should find names starting with A", dataRows.length() > 0);
    for (int i = 0; i < dataRows.length(); i++) {
      String name = dataRows.getJSONArray(i).getString(0);
      assertTrue("Name should start with A", name.startsWith("A"));
    }
  }

  @Test
  public void test_string_question_wildcard() throws IOException {
    // Test for names with specific length using ?
    // firstname='?????' matches 5-character names
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname='?????' | fields firstname";
    JSONObject result = executeQuery(query);
    
    // Should match 5-character names like "Amber"
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String name = dataRows.getJSONArray(i).getString(0);
      assertEquals("Name should be 5 characters", 5, name.length());
    }
  }

  @Test
  public void test_string_multiple_wildcards() throws IOException {
    // Test pattern with multiple wildcards
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname='*m*' | fields firstname";
    JSONObject result = executeQuery(query);
    
    // Should match names containing 'm' or 'M'
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String name = dataRows.getJSONArray(i).getString(0);
      assertTrue("Name should contain 'm'", 
                 name.toLowerCase().contains("m"));
    }
  }

  // ========== NUMERIC TYPE TESTS ==========

  @Test
  public void test_integer_exact_string_match() throws IOException {
    // Test that age='32' works (auto-cast)
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where age='32' | fields age, firstname";
    JSONObject result = executeQuery(query);
    
    // Should match exact age 32
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      int age = dataRows.getJSONArray(i).getInt(0);
      assertEquals("Age should be 32", 32, age);
    }
  }

  @Test
  public void test_integer_wildcard_pattern() throws IOException {
    // Test that age='3*' works with auto-cast
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where age='3*' | fields age, firstname";
    JSONObject result = executeQuery(query);
    
    // Should match ages starting with 3 (30-39)
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String ageStr = String.valueOf(dataRows.getJSONArray(i).getInt(0));
      assertTrue("Age should start with 3", ageStr.startsWith("3"));
    }
  }

  @Test
  public void test_account_number_wildcard() throws IOException {
    // Test account_number='1*' to match account numbers starting with 1
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where account_number='1*' | fields account_number";
    JSONObject result = executeQuery(query);
    
    // Should match account numbers starting with 1
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String accountNum = String.valueOf(dataRows.getJSONArray(i).getInt(0));
      assertTrue("Account number should start with 1", accountNum.startsWith("1"));
    }
  }

  // ========== REVERSE ORDER TESTS (value = field) ==========

  @Test
  public void test_reverse_string_equals_field() throws IOException {
    // Test 'Amber' = firstname (literal on left)
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where 'Amber'=firstname | fields firstname";
    JSONObject result = executeQuery(query);
    
    // Should match Amber
    JSONArray dataRows = result.getJSONArray("datarows");
    if (dataRows.length() > 0) {
      assertEquals("Should find Amber", "Amber", dataRows.getJSONArray(0).getString(0));
    }
  }

  @Test
  public void test_reverse_wildcard_equals_field() throws IOException {
    // Test 'H*' = firstname (wildcard literal on left)
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where 'H*'=firstname | fields firstname";
    JSONObject result = executeQuery(query);
    
    // Should match names starting with H (like Hattie)
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String name = dataRows.getJSONArray(i).getString(0);
      assertTrue("Name should start with H", name.startsWith("H"));
    }
  }

  @Test
  public void test_reverse_numeric_string_equals_field() throws IOException {
    // Test '36' = age (numeric string on left)
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where '36'=age | fields age, firstname";
    JSONObject result = executeQuery(query);
    
    // Should match age 36
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      int age = dataRows.getJSONArray(i).getInt(0);
      assertEquals("Age should be 36", 36, age);
    }
  }

  // ========== NOT EQUALS TESTS ==========

  @Test
  public void test_not_equals_wildcard_string() throws IOException {
    // Test firstname!='A*'
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname!='A*' | fields firstname | head 5";
    JSONObject result = executeQuery(query);
    
    // Should NOT match names starting with A
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String name = dataRows.getJSONArray(i).getString(0);
      assertFalse("Name should not start with A", name.startsWith("A"));
    }
  }

  @Test
  public void test_not_equals_wildcard_numeric() throws IOException {
    // Test age!='3*'
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where age!='3*' | fields age | head 5";
    JSONObject result = executeQuery(query);
    
    // Should NOT match ages starting with 3
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String ageStr = String.valueOf(dataRows.getJSONArray(i).getInt(0));
      assertFalse("Age should not start with 3", ageStr.startsWith("3"));
    }
  }

  // ========== WILDCARD INDEX SPECIFIC TESTS ==========
  
  @Test
  public void test_wildcard_index_keyword_field() throws IOException {
    // The WILDCARD index has KeywordBody field
    // Add some data with native wildcards for testing
    String query = "search source=" + TEST_INDEX_WILDCARD + " | where KeywordBody='test*' | fields KeywordBody";
    JSONObject result = executeQuery(query);
    
    // Should match entries starting with "test"
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String value = dataRows.getJSONArray(i).getString(0);
      assertTrue("Should start with 'test'", value.startsWith("test"));
    }
  }

  // ========== EDGE CASES ==========

  @Test
  public void test_empty_string_comparison() throws IOException {
    // Test field = '' (empty string)
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname='' | fields firstname";
    JSONObject result = executeQuery(query);
    
    // Should only match empty strings (likely none in account data)
    if (result.getInt("total") > 0) {
      JSONArray dataRows = result.getJSONArray("datarows");
      for (int i = 0; i < dataRows.length(); i++) {
        assertEquals("Should be empty string", "", dataRows.getJSONArray(i).getString(0));
      }
    }
  }

  @Test
  public void test_only_wildcard_pattern() throws IOException {
    // Test firstname='*' (matches everything)
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname='*' | fields firstname | head 5";
    JSONObject result = executeQuery(query);
    
    // '*' should match all non-null values
    assertTrue("* should match all values", result.getInt("total") > 0);
  }

  @Test
  public void test_gender_single_char_field() throws IOException {
    // Gender field has single character values (M/F)
    // Test exact match
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where gender='M' | fields gender | head 5";
    JSONObject result = executeQuery(query);
    
    // Should match only 'M' values
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      assertEquals("Gender should be M", "M", dataRows.getJSONArray(i).getString(0));
    }
  }

  @Test
  public void test_gender_question_wildcard() throws IOException {
    // Test gender='?' (matches single character)
    String query = "search source=" + TEST_INDEX_ACCOUNT + " | where gender='?' | fields gender | head 5";
    JSONObject result = executeQuery(query);
    
    // Should match single character values (M or F)
    JSONArray dataRows = result.getJSONArray("datarows");
    for (int i = 0; i < dataRows.length(); i++) {
      String gender = dataRows.getJSONArray(i).getString(0);
      assertEquals("Gender should be single char", 1, gender.length());
      assertTrue("Gender should be M or F", gender.equals("M") || gender.equals("F"));
    }
  }

  // ========== PERFORMANCE/PUSHDOWN TESTS ==========

  @Test
  public void test_pushdown_string_wildcard() throws IOException {
    enableCalcite();
    enablePushdown();

    try {
      String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname='A*' | explain";
      JSONObject result = executeQuery(query);
      
      String plan = result.toString();
      // With pushdown, should use native wildcard query or ILIKE
      assertTrue("Should use optimized wildcard query",
                 plan.contains("wildcard") || plan.contains("ILIKE") || plan.contains("like"));
    } finally {
      disablePushdown();
      disableCalcite();
    }
  }

  @Test
  public void test_no_pushdown_in_memory_filtering() throws IOException {
    enableCalcite();
    disablePushdown();

    try {
      String query = "search source=" + TEST_INDEX_ACCOUNT + " | where firstname='A*' | fields firstname | head 5";
      JSONObject result = executeQuery(query);
      
      // Without pushdown, fetches all and filters in-memory
      // Should still return correct results
      JSONArray dataRows = result.getJSONArray("datarows");
      for (int i = 0; i < dataRows.length(); i++) {
        String name = dataRows.getJSONArray(i).getString(0);
        assertTrue("Should still match pattern", name.startsWith("A"));
      }
    } finally {
      enablePushdown();
      disableCalcite();
    }
  }

  // ========== HELPER METHODS ==========

  private void enablePushdown() throws IOException {
    updateClusterSettings(
        new PPLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "true"));
  }

  private void disablePushdown() throws IOException {
    updateClusterSettings(
        new PPLIntegTestCase.ClusterSetting(
            "persistent", Settings.Key.CALCITE_PUSHDOWN_ENABLED.getKeyValue(), "false"));
  }
}