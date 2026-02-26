/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.dataunit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class LocalityAwareDataUnitAssignmentTest {

  private final LocalityAwareDataUnitAssignment assignment = new LocalityAwareDataUnitAssignment();

  @Test
  void should_assign_to_primary_preferred_node() {
    DataUnit du0 = new OpenSearchDataUnit("idx", 0, List.of("node-1", "node-2"), -1, -1);
    DataUnit du1 = new OpenSearchDataUnit("idx", 1, List.of("node-2", "node-3"), -1, -1);

    Map<String, List<DataUnit>> result =
        assignment.assign(List.of(du0, du1), List.of("node-1", "node-2", "node-3"));

    assertEquals(2, result.size());
    assertEquals(1, result.get("node-1").size());
    assertEquals(1, result.get("node-2").size());
    assertEquals("idx/0", result.get("node-1").get(0).getDataUnitId());
    assertEquals("idx/1", result.get("node-2").get(0).getDataUnitId());
  }

  @Test
  void should_assign_multiple_shards_to_same_node() {
    DataUnit du0 = new OpenSearchDataUnit("idx", 0, List.of("node-1"), -1, -1);
    DataUnit du1 = new OpenSearchDataUnit("idx", 1, List.of("node-1"), -1, -1);
    DataUnit du2 = new OpenSearchDataUnit("idx", 2, List.of("node-2"), -1, -1);

    Map<String, List<DataUnit>> result =
        assignment.assign(List.of(du0, du1, du2), List.of("node-1", "node-2"));

    assertEquals(2, result.size());
    assertEquals(2, result.get("node-1").size());
    assertEquals(1, result.get("node-2").size());
  }

  @Test
  void should_fallback_to_replica_when_primary_unavailable() {
    // Primary on node-3 (not available), replica on node-1 (available)
    DataUnit du = new OpenSearchDataUnit("idx", 0, List.of("node-3", "node-1"), -1, -1);

    Map<String, List<DataUnit>> result =
        assignment.assign(List.of(du), List.of("node-1", "node-2"));

    assertEquals(1, result.size());
    assertEquals(1, result.get("node-1").size());
  }

  @Test
  void should_throw_when_no_preferred_node_available() {
    DataUnit du = new OpenSearchDataUnit("idx", 0, List.of("node-3", "node-4"), -1, -1);

    assertThrows(
        IllegalStateException.class,
        () -> assignment.assign(List.of(du), List.of("node-1", "node-2")));
  }

  @Test
  void should_handle_empty_data_units() {
    Map<String, List<DataUnit>> result = assignment.assign(List.of(), List.of("node-1", "node-2"));

    assertEquals(0, result.size());
  }
}
