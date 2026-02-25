/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.dataunit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchDataUnitSourceTest {

  @Mock private ClusterService clusterService;
  @Mock private ClusterState clusterState;
  @Mock private RoutingTable routingTable;

  @Test
  void should_discover_shards_with_primary_and_replicas() {
    // Mock shard 0: primary on node-1, replica on node-2
    ShardRouting primary0 = mockShardRouting("node-1", true);
    ShardRouting replica0 = mockShardRouting("node-2", false);
    IndexShardRoutingTable shardTable0 = mock(IndexShardRoutingTable.class);
    when(shardTable0.primaryShard()).thenReturn(primary0);
    when(shardTable0.replicaShards()).thenReturn(List.of(replica0));

    // Mock shard 1: primary on node-2, replica on node-3
    ShardRouting primary1 = mockShardRouting("node-2", true);
    ShardRouting replica1 = mockShardRouting("node-3", false);
    IndexShardRoutingTable shardTable1 = mock(IndexShardRoutingTable.class);
    when(shardTable1.primaryShard()).thenReturn(primary1);
    when(shardTable1.replicaShards()).thenReturn(List.of(replica1));

    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
    when(indexRoutingTable.getShards()).thenReturn(Map.of(0, shardTable0, 1, shardTable1));

    when(routingTable.index("accounts")).thenReturn(indexRoutingTable);
    when(clusterState.routingTable()).thenReturn(routingTable);
    when(clusterService.state()).thenReturn(clusterState);

    OpenSearchDataUnitSource source = new OpenSearchDataUnitSource(clusterService, "accounts");
    assertFalse(source.isFinished());

    List<DataUnit> dataUnits = source.getNextBatch();
    assertTrue(source.isFinished());
    assertEquals(2, dataUnits.size());

    // Verify shard 0
    OpenSearchDataUnit du0 = findDataUnit(dataUnits, 0);
    assertEquals("accounts", du0.getIndexName());
    assertEquals(0, du0.getShardId());
    assertEquals(List.of("node-1", "node-2"), du0.getPreferredNodes());
    assertFalse(du0.isRemotelyAccessible());

    // Verify shard 1
    OpenSearchDataUnit du1 = findDataUnit(dataUnits, 1);
    assertEquals("accounts", du1.getIndexName());
    assertEquals(1, du1.getShardId());
    assertEquals(List.of("node-2", "node-3"), du1.getPreferredNodes());
  }

  @Test
  void should_return_empty_on_second_batch() {
    ShardRouting primary = mockShardRouting("node-1", true);
    IndexShardRoutingTable shardTable = mock(IndexShardRoutingTable.class);
    when(shardTable.primaryShard()).thenReturn(primary);
    when(shardTable.replicaShards()).thenReturn(List.of());

    IndexRoutingTable indexRoutingTable = mock(IndexRoutingTable.class);
    when(indexRoutingTable.getShards()).thenReturn(Map.of(0, shardTable));

    when(routingTable.index("accounts")).thenReturn(indexRoutingTable);
    when(clusterState.routingTable()).thenReturn(routingTable);
    when(clusterService.state()).thenReturn(clusterState);

    OpenSearchDataUnitSource source = new OpenSearchDataUnitSource(clusterService, "accounts");
    List<DataUnit> first = source.getNextBatch();
    assertEquals(1, first.size());
    assertTrue(source.isFinished());

    List<DataUnit> second = source.getNextBatch();
    assertTrue(second.isEmpty());
  }

  @Test
  void should_throw_for_nonexistent_index() {
    when(routingTable.index("nonexistent")).thenReturn(null);
    when(clusterState.routingTable()).thenReturn(routingTable);
    when(clusterService.state()).thenReturn(clusterState);

    OpenSearchDataUnitSource source = new OpenSearchDataUnitSource(clusterService, "nonexistent");
    assertThrows(IllegalArgumentException.class, () -> source.getNextBatch());
  }

  private ShardRouting mockShardRouting(String nodeId, boolean primary) {
    ShardRouting routing = mock(ShardRouting.class);
    when(routing.currentNodeId()).thenReturn(nodeId);
    when(routing.assignedToNode()).thenReturn(true);
    return routing;
  }

  private OpenSearchDataUnit findDataUnit(List<DataUnit> units, int shardId) {
    return units.stream()
        .map(u -> (OpenSearchDataUnit) u)
        .filter(u -> u.getShardId() == shardId)
        .findFirst()
        .orElseThrow(() -> new AssertionError("DataUnit for shard " + shardId + " not found"));
  }
}
