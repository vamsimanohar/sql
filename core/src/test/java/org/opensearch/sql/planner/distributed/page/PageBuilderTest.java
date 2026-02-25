/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.page;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class PageBuilderTest {

  @Test
  void should_build_page_row_by_row() {
    PageBuilder builder = new PageBuilder(3);

    builder.beginRow();
    builder.setValue(0, "Alice");
    builder.setValue(1, 30);
    builder.setValue(2, 1000.0);
    builder.endRow();

    builder.beginRow();
    builder.setValue(0, "Bob");
    builder.setValue(1, 25);
    builder.setValue(2, 2000.0);
    builder.endRow();

    Page page = builder.build();
    assertEquals(2, page.getPositionCount());
    assertEquals(3, page.getChannelCount());
    assertEquals("Alice", page.getValue(0, 0));
    assertEquals(2000.0, page.getValue(1, 2));
  }

  @Test
  void should_track_row_count() {
    PageBuilder builder = new PageBuilder(2);
    assertTrue(builder.isEmpty());
    assertEquals(0, builder.getRowCount());

    builder.beginRow();
    builder.setValue(0, "A");
    builder.setValue(1, 1);
    builder.endRow();

    assertEquals(1, builder.getRowCount());
  }

  @Test
  void should_reset_after_build() {
    PageBuilder builder = new PageBuilder(1);
    builder.beginRow();
    builder.setValue(0, "test");
    builder.endRow();

    Page page = builder.build();
    assertEquals(1, page.getPositionCount());

    // Builder should be empty after build
    assertTrue(builder.isEmpty());
    assertEquals(0, builder.getRowCount());
  }

  @Test
  void should_throw_on_set_before_begin() {
    PageBuilder builder = new PageBuilder(2);
    assertThrows(IllegalStateException.class, () -> builder.setValue(0, "value"));
  }

  @Test
  void should_throw_on_end_before_begin() {
    PageBuilder builder = new PageBuilder(2);
    assertThrows(IllegalStateException.class, () -> builder.endRow());
  }

  @Test
  void should_throw_on_build_with_uncommitted_row() {
    PageBuilder builder = new PageBuilder(2);
    builder.beginRow();
    builder.setValue(0, "value");
    assertThrows(IllegalStateException.class, () -> builder.build());
  }

  @Test
  void should_throw_on_invalid_channel() {
    PageBuilder builder = new PageBuilder(2);
    builder.beginRow();
    assertThrows(IndexOutOfBoundsException.class, () -> builder.setValue(2, "value"));
    assertThrows(IndexOutOfBoundsException.class, () -> builder.setValue(-1, "value"));
  }
}
