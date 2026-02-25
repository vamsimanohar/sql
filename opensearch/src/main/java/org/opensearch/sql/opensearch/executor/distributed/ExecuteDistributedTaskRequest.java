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
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request message for executing distributed query tasks on a remote node.
 *
 * <p>Contains the operator pipeline parameters needed for execution: index name, shard IDs, field
 * names, query limit, and optional filter conditions.
 */
@Data
@EqualsAndHashCode(callSuper = false)
@AllArgsConstructor
@NoArgsConstructor
public class ExecuteDistributedTaskRequest extends ActionRequest {

  /** ID of the execution stage these work units belong to */
  private String stageId;

  /** Index name for per-shard execution. */
  private String indexName;

  /** Shard IDs to execute on the target node. */
  private List<Integer> shardIds;

  /** Execution mode: always "OPERATOR_PIPELINE". */
  private String executionMode;

  /** Fields to return when using operator pipeline mode. */
  private List<String> fieldNames;

  /** Row limit when using operator pipeline mode. */
  private int queryLimit;

  /**
   * Filter conditions for operator pipeline. Each entry is a Map with keys: "field" (String), "op"
   * (String: EQ, NEQ, GT, GTE, LT, LTE), "value" (Object). Multiple conditions are ANDed. Compound
   * boolean uses "bool" key with "AND"/"OR" and "children" list. Null means match all.
   */
  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> filterConditions;

  /** Constructor for deserialization from stream. */
  public ExecuteDistributedTaskRequest(StreamInput in) throws IOException {
    super(in);
    this.stageId = in.readString();
    this.indexName = in.readOptionalString();

    // Skip SearchSourceBuilder field (backward compat: always false for new requests)
    if (in.readBoolean()) {
      // Consume the SearchSourceBuilder bytes for backward compatibility
      new org.opensearch.search.builder.SearchSourceBuilder(in);
    }

    // Deserialize shard IDs
    if (in.readBoolean()) {
      int shardCount = in.readVInt();
      this.shardIds = new java.util.ArrayList<>(shardCount);
      for (int i = 0; i < shardCount; i++) {
        this.shardIds.add(in.readVInt());
      }
    }

    // Deserialize operator pipeline fields
    this.executionMode = in.readOptionalString();
    if (in.readBoolean()) {
      this.fieldNames = in.readStringList();
    }
    this.queryLimit = in.readVInt();

    // Deserialize filter conditions
    if (in.readBoolean()) {
      int filterCount = in.readVInt();
      this.filterConditions = new java.util.ArrayList<>(filterCount);
      for (int i = 0; i < filterCount; i++) {
        @SuppressWarnings("unchecked")
        Map<String, Object> condition = (Map<String, Object>) in.readGenericValue();
        this.filterConditions.add(condition);
      }
    }
  }

  /** Serializes this request to a stream for network transport. */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeString(stageId != null ? stageId : "");
    out.writeOptionalString(indexName);

    // SearchSourceBuilder field — always false for new requests
    out.writeBoolean(false);

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

    // Serialize operator pipeline fields
    out.writeOptionalString(executionMode);
    if (fieldNames != null) {
      out.writeBoolean(true);
      out.writeStringCollection(fieldNames);
    } else {
      out.writeBoolean(false);
    }
    out.writeVInt(queryLimit);

    // Serialize filter conditions
    if (filterConditions != null && !filterConditions.isEmpty()) {
      out.writeBoolean(true);
      out.writeVInt(filterConditions.size());
      for (Map<String, Object> condition : filterConditions) {
        out.writeGenericValue(condition);
      }
    } else {
      out.writeBoolean(false);
    }
  }

  /**
   * Validates the request before execution.
   *
   * @return true if request is valid for execution
   */
  public boolean isValid() {
    return indexName != null
        && !indexName.isEmpty()
        && shardIds != null
        && !shardIds.isEmpty()
        && fieldNames != null
        && !fieldNames.isEmpty()
        && queryLimit > 0;
  }

  @Override
  public ActionRequestValidationException validate() {
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

  @Override
  public String toString() {
    return String.format(
        "ExecuteDistributedTaskRequest{stageId='%s', index='%s', shards=%s, mode='%s'}",
        stageId, indexName, shardIds, executionMode);
  }
}
