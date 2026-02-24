/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class TransportExecuteDistributedTaskActionTest {

  @Mock private TransportService transportService;
  @Mock private ClusterService clusterService;
  @Mock private ActionFilters actionFilters;
  @Mock private Client client;
  @Mock private IndicesService indicesService;
  @Mock private Task task;

  private TransportExecuteDistributedTaskAction action;

  @BeforeEach
  void setUp() {
    action =
        new TransportExecuteDistributedTaskAction(
            transportService, actionFilters, clusterService, client, indicesService);

    // Setup cluster service mock
    DiscoveryNode localNode = mock(DiscoveryNode.class);
    when(localNode.getId()).thenReturn("test-node-1");
    when(clusterService.localNode()).thenReturn(localNode);
  }

  @Test
  void action_name_should_be_defined() {
    assertEquals(
        "cluster:admin/opensearch/sql/distributed/execute",
        TransportExecuteDistributedTaskAction.NAME);
  }

  @Test
  void should_validate_operator_pipeline_request() {
    // Given: Valid operator pipeline request
    ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest();
    request.setExecutionMode("OPERATOR_PIPELINE");
    request.setIndexName("test-index");
    request.setShardIds(List.of(0, 1));
    request.setFieldNames(List.of("field1", "field2"));
    request.setQueryLimit(100);
    request.setStageId("operator-pipeline");

    // Then
    assertTrue(request.isValid());
    assertNotNull(request.toString());
  }

  @Test
  void should_reject_invalid_request_missing_index() {
    // Given: Request without index name
    ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest();
    request.setExecutionMode("OPERATOR_PIPELINE");
    request.setShardIds(List.of(0, 1));
    request.setFieldNames(List.of("field1"));
    request.setQueryLimit(100);

    // Then
    assertNotNull(request.validate());
  }

  @Test
  void should_reject_invalid_request_missing_shards() {
    // Given: Request without shard IDs
    ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest();
    request.setExecutionMode("OPERATOR_PIPELINE");
    request.setIndexName("test-index");
    request.setFieldNames(List.of("field1"));
    request.setQueryLimit(100);

    // Then
    assertNotNull(request.validate());
  }

  @Test
  void should_reject_invalid_request_missing_fields() {
    // Given: Request without field names
    ExecuteDistributedTaskRequest request = new ExecuteDistributedTaskRequest();
    request.setExecutionMode("OPERATOR_PIPELINE");
    request.setIndexName("test-index");
    request.setShardIds(List.of(0, 1));
    request.setQueryLimit(100);

    // Then
    assertNotNull(request.validate());
  }
}
