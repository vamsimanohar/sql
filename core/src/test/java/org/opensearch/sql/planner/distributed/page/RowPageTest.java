/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class RowPageTest {

  @Test
  void should_create_page_with_rows_and_columns() {
    Object[][] data = {
      {"Alice", 30, 1000.0},
      {"Bob", 25, 2000.0}
    };
    RowPage page = new RowPage(data, 3);

    assertEquals(2, page.getPositionCount());
    assertEquals(3, page.getChannelCount());
  }

  @Test
  void should_access_values_by_position_and_channel() {
    Object[][] data = {
      {"Alice", 30, 1000.0},
      {"Bob", 25, 2000.0}
    };
    RowPage page = new RowPage(data, 3);

    assertEquals("Alice", page.getValue(0, 0));
    assertEquals(30, page.getValue(0, 1));
    assertEquals(1000.0, page.getValue(0, 2));
    assertEquals("Bob", page.getValue(1, 0));
    assertEquals(25, page.getValue(1, 1));
  }

  @Test
  void should_handle_null_values() {
    Object[][] data = {{null, 30, null}};
    RowPage page = new RowPage(data, 3);

    assertNull(page.getValue(0, 0));
    assertEquals(30, page.getValue(0, 1));
    assertNull(page.getValue(0, 2));
  }

  @Test
  void should_create_sub_region() {
    Object[][] data = {
      {"Alice", 30},
      {"Bob", 25},
      {"Charlie", 35},
      {"Diana", 28}
    };
    RowPage page = new RowPage(data, 2);

    Page region = page.getRegion(1, 2);
    assertEquals(2, region.getPositionCount());
    assertEquals("Bob", region.getValue(0, 0));
    assertEquals("Charlie", region.getValue(1, 0));
  }

  @Test
  void should_create_empty_page() {
    Page empty = Page.empty(3);
    assertEquals(0, empty.getPositionCount());
    assertEquals(3, empty.getChannelCount());
  }

  @Test
  void should_throw_on_invalid_position() {
    Object[][] data = {{"Alice", 30}};
    RowPage page = new RowPage(data, 2);

    assertThrows(IndexOutOfBoundsException.class, () -> page.getValue(-1, 0));
    assertThrows(IndexOutOfBoundsException.class, () -> page.getValue(1, 0));
  }

  @Test
  void should_throw_on_invalid_channel() {
    Object[][] data = {{"Alice", 30}};
    RowPage page = new RowPage(data, 2);

    assertThrows(IndexOutOfBoundsException.class, () -> page.getValue(0, -1));
    assertThrows(IndexOutOfBoundsException.class, () -> page.getValue(0, 2));
  }

  @Test
  void should_throw_on_invalid_region() {
    Object[][] data = {{"Alice"}, {"Bob"}};
    RowPage page = new RowPage(data, 1);

    assertThrows(IndexOutOfBoundsException.class, () -> page.getRegion(1, 3));
    assertThrows(IndexOutOfBoundsException.class, () -> page.getRegion(-1, 1));
  }
}
