/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import org.opensearch.action.ActionType;

/**
 * Transport action for executing distributed query tasks on remote cluster nodes.
 *
 * <p>This action enables the DistributedTaskScheduler to send WorkUnits to specific nodes for
 * execution. Each node receives a set of WorkUnits and executes them locally, returning results
 * back to the coordinator.
 *
 * <p><strong>Usage:</strong>
 *
 * <pre>
 * TransportService.sendRequest(
 *   targetNode,
 *   ExecuteDistributedTaskAction.NAME,
 *   new ExecuteDistributedTaskRequest(workUnits, stageId, inputData),
 *   responseListener
 * );
 * </pre>
 */
public class ExecuteDistributedTaskAction extends ActionType<ExecuteDistributedTaskResponse> {

  /** Action name used for transport routing */
  public static final String NAME = "cluster:admin/opensearch/sql/distributed/execute";

  /** Singleton instance */
  public static final ExecuteDistributedTaskAction INSTANCE = new ExecuteDistributedTaskAction();

  private ExecuteDistributedTaskAction() {
    super(NAME, ExecuteDistributedTaskResponse::new);
  }
}
