/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response message containing results from distributed query task execution.
 *
 * <p>Contains the execution results, performance metrics, and any error information from executing
 * WorkUnits on a remote cluster node.
 *
 * <p><strong>Result Format:</strong> Results are returned as generic Objects to support different
 * data types:
 *
 * <ul>
 *   <li>SCAN stage: Filtered and projected document data
 *   <li>PROCESS stage: Partial aggregation results
 *   <li>FINALIZE stage: Final query results
 * </ul>
 */
@Data
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@NoArgsConstructor
public class ExecuteDistributedTaskResponse extends ActionResponse {

  /** Results from executing the work units */
  private List<Object> results;

  /** Execution statistics and performance metrics */
  private Map<String, Object> executionStats;

  /** Node ID where the tasks were executed */
  private String nodeId;

  /** Whether execution completed successfully */
  private boolean success;

  /** Error message if execution failed */
  private String errorMessage;

  /** Constructor for deserialization from stream. */
  public ExecuteDistributedTaskResponse(StreamInput in) throws IOException {
    super(in);

    // TODO: Phase 1 - Implement proper serialization for results
    this.nodeId = in.readString();
    this.success = in.readBoolean();
    this.errorMessage = in.readOptionalString();

    // Note: Results serialization will need to be implemented when
    // the actual task operators produce concrete result types
    this.results = List.of(); // Placeholder
    this.executionStats = Map.of(); // Placeholder
  }

  /** Serializes this response to a stream for network transport. */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(nodeId != null ? nodeId : "");
    out.writeBoolean(success);
    out.writeOptionalString(errorMessage);

    // TODO: Phase 1 - Implement proper serialization for results
    // For now, we write minimal placeholder data
  }

  /** Creates a successful response with results. */
  public static ExecuteDistributedTaskResponse success(
      String nodeId, List<Object> results, Map<String, Object> stats) {
    return new ExecuteDistributedTaskResponse(results, stats, nodeId, true, null);
  }

  /** Creates a failure response with error information. */
  public static ExecuteDistributedTaskResponse failure(String nodeId, String errorMessage) {
    return new ExecuteDistributedTaskResponse(List.of(), Map.of(), nodeId, false, errorMessage);
  }

  /** Gets the number of results returned. */
  public int getResultCount() {
    return results != null ? results.size() : 0;
  }

  /** Checks if the execution was successful. */
  public boolean isSuccessful() {
    return success && errorMessage == null;
  }

  @Override
  public String toString() {
    return String.format(
        "ExecuteDistributedTaskResponse{nodeId='%s', success=%s, results=%d, error='%s'}",
        nodeId, success, getResultCount(), errorMessage);
  }
}
