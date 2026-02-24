/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.io.IOException;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.planner.distributed.WorkUnit;

/**
 * Request message for executing distributed query tasks on a remote node.
 *
 * <p>Contains the work units to be executed, along with context information needed for execution
 * such as stage ID and input data from previous stages.
 *
 * <p><strong>Phase 1B Serialization:</strong> Serializes the SearchSourceBuilder (which implements
 * Writeable) along with index name and shard IDs for per-shard execution on remote nodes. This
 * prepares for Phase 1C transport-based execution.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@NoArgsConstructor
public class ExecuteDistributedTaskRequest extends ActionRequest {

  /** Work units to execute on the target node */
  private List<WorkUnit> workUnits;

  /** ID of the execution stage these work units belong to */
  private String stageId;

  /** Input data from previous stages (null for initial scan stages) */
  private Object inputData;

  /** SearchSourceBuilder for per-shard execution (Phase 1B). */
  private SearchSourceBuilder searchSourceBuilder;

  /** Index name for per-shard execution (Phase 1B). */
  private String indexName;

  /** Shard IDs to execute on the target node (Phase 1B). */
  private List<Integer> shardIds;

  /** Execution mode: "SSB" (default) or "OPERATOR_PIPELINE" (Phase 5B). */
  private String executionMode;

  /** Fields to return when using operator pipeline mode (Phase 5B). */
  private List<String> fieldNames;

  /** Row limit when using operator pipeline mode (Phase 5B). */
  private int queryLimit;

  /** Constructor with original fields for backward compatibility. */
  public ExecuteDistributedTaskRequest(List<WorkUnit> workUnits, String stageId, Object inputData) {
    this.workUnits = workUnits;
    this.stageId = stageId;
    this.inputData = inputData;
  }

  /** Constructor for deserialization from stream. */
  public ExecuteDistributedTaskRequest(StreamInput in) throws IOException {
    super(in);
    this.stageId = in.readString();
    this.indexName = in.readOptionalString();

    // Deserialize SearchSourceBuilder (implements Writeable)
    if (in.readBoolean()) {
      this.searchSourceBuilder = new SearchSourceBuilder(in);
    }

    // Deserialize shard IDs
    if (in.readBoolean()) {
      int shardCount = in.readVInt();
      this.shardIds = new java.util.ArrayList<>(shardCount);
      for (int i = 0; i < shardCount; i++) {
        this.shardIds.add(in.readVInt());
      }
    }

    // Deserialize operator pipeline fields (Phase 5B)
    this.executionMode = in.readOptionalString();
    if (in.readBoolean()) {
      this.fieldNames = in.readStringList();
    }
    this.queryLimit = in.readVInt();

    // WorkUnit and inputData are not serialized over transport (used locally only)
    this.workUnits = List.of();
    this.inputData = null;
  }

  /** Serializes this request to a stream for network transport. */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(stageId != null ? stageId : "");
    out.writeOptionalString(indexName);

    // Serialize SearchSourceBuilder (implements Writeable)
    if (searchSourceBuilder != null) {
      out.writeBoolean(true);
      searchSourceBuilder.writeTo(out);
    } else {
      out.writeBoolean(false);
    }

    // Serialize shard IDs
    if (shardIds != null) {
      out.writeBoolean(true);
      out.writeVInt(shardIds.size());
      for (int shardId : shardIds) {
        out.writeVInt(shardId);
      }
    } else {
      out.writeBoolean(false);
    }

    // Serialize operator pipeline fields (Phase 5B)
    out.writeOptionalString(executionMode);
    if (fieldNames != null) {
      out.writeBoolean(true);
      out.writeStringCollection(fieldNames);
    } else {
      out.writeBoolean(false);
    }
    out.writeVInt(queryLimit);
  }

  /**
   * Validates the request before execution. Supports both Phase 1C (SearchSourceBuilder-based) and
   * legacy (WorkUnit-based) request formats.
   *
   * @return true if request is valid for execution
   */
  public boolean isValid() {
    // Phase 5B: Operator pipeline request
    if ("OPERATOR_PIPELINE".equals(executionMode)) {
      return indexName != null
          && !indexName.isEmpty()
          && shardIds != null
          && !shardIds.isEmpty()
          && fieldNames != null
          && !fieldNames.isEmpty()
          && queryLimit > 0;
    }
    // Phase 1C: SSB-based request
    if (searchSourceBuilder != null) {
      return indexName != null && !indexName.isEmpty() && shardIds != null && !shardIds.isEmpty();
    }
    // Legacy: WorkUnit-based request
    return stageId != null && !stageId.isEmpty() && workUnits != null;
  }

  /** Gets the number of work units in this request. */
  public int getWorkUnitCount() {
    return workUnits != null ? workUnits.size() : 0;
  }

  @Override
  public ActionRequestValidationException validate() {
    // Phase 5B: Operator pipeline request
    if ("OPERATOR_PIPELINE".equals(executionMode)) {
      ActionRequestValidationException validationException = null;
      if (indexName == null || indexName.trim().isEmpty()) {
        validationException = new ActionRequestValidationException();
        validationException.addValidationError("Index name cannot be null or empty");
      }
      if (shardIds == null || shardIds.isEmpty()) {
        if (validationException == null) {
          validationException = new ActionRequestValidationException();
        }
        validationException.addValidationError("Shard IDs cannot be null or empty");
      }
      if (fieldNames == null || fieldNames.isEmpty()) {
        if (validationException == null) {
          validationException = new ActionRequestValidationException();
        }
        validationException.addValidationError("Field names cannot be null or empty");
      }
      return validationException;
    }

    // Phase 1C: SSB-based request requires index + shards
    if (searchSourceBuilder != null) {
      ActionRequestValidationException validationException = null;
      if (indexName == null || indexName.trim().isEmpty()) {
        validationException = new ActionRequestValidationException();
        validationException.addValidationError("Index name cannot be null or empty");
      }
      if (shardIds == null || shardIds.isEmpty()) {
        if (validationException == null) {
          validationException = new ActionRequestValidationException();
        }
        validationException.addValidationError("Shard IDs cannot be null or empty");
      }
      return validationException;
    }

    // Legacy: WorkUnit-based validation
    ActionRequestValidationException validationException = null;

    if (stageId == null || stageId.trim().isEmpty()) {
      validationException = new ActionRequestValidationException();
      validationException.addValidationError("Stage ID cannot be null or empty");
    }

    if (workUnits == null) {
      if (validationException == null) {
        validationException = new ActionRequestValidationException();
      }
      validationException.addValidationError("Work units cannot be null");
    }

    return validationException;
  }

  @Override
  public String toString() {
    return String.format(
        "ExecuteDistributedTaskRequest{stageId='%s', workUnits=%d, index='%s', shards=%s,"
            + " mode='%s'}",
        stageId, getWorkUnitCount(), indexName, shardIds, executionMode);
  }
}
