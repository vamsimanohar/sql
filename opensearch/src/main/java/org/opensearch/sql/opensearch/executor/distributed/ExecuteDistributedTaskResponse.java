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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Response message containing results from distributed query task execution.
 *
 * <p>Contains the execution results, performance metrics, and any error information from executing
 * WorkUnits on a remote cluster node.
 *
 * <p><strong>Phase 1B Serialization:</strong> Serializes the SearchResponse (which implements
 * Writeable) for returning per-shard search results from remote nodes. This prepares for Phase 1C
 * transport-based execution.
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

  /** SearchResponse from per-shard execution (Phase 1B). */
  private SearchResponse searchResponse;

  /** Constructor with original fields for backward compatibility. */
  public ExecuteDistributedTaskResponse(
      List<Object> results,
      Map<String, Object> executionStats,
      String nodeId,
      boolean success,
      String errorMessage) {
    this.results = results;
    this.executionStats = executionStats;
    this.nodeId = nodeId;
    this.success = success;
    this.errorMessage = errorMessage;
  }

  /** Constructor for deserialization from stream. */
  public ExecuteDistributedTaskResponse(StreamInput in) throws IOException {
    super(in);
    this.nodeId = in.readString();
    this.success = in.readBoolean();
    this.errorMessage = in.readOptionalString();

    // Deserialize SearchResponse (implements Writeable)
    if (in.readBoolean()) {
      this.searchResponse = new SearchResponse(in);
    }

    // Generic results not serialized over transport
    this.results = List.of();
    this.executionStats = Map.of();
  }

  /** Serializes this response to a stream for network transport. */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(nodeId != null ? nodeId : "");
    out.writeBoolean(success);
    out.writeOptionalString(errorMessage);

    // Serialize SearchResponse (implements Writeable)
    if (searchResponse != null) {
      out.writeBoolean(true);
      searchResponse.writeTo(out);
    } else {
      out.writeBoolean(false);
    }
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

  /** Creates a successful response containing a SearchResponse (Phase 1C). */
  public static ExecuteDistributedTaskResponse successWithSearch(
      String nodeId, SearchResponse searchResponse) {
    ExecuteDistributedTaskResponse resp =
        new ExecuteDistributedTaskResponse(List.of(), Map.of(), nodeId, true, null);
    resp.setSearchResponse(searchResponse);
    return resp;
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
