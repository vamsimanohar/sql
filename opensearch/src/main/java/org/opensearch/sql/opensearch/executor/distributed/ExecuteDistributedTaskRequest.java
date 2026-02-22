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
import org.opensearch.sql.planner.distributed.WorkUnit;

/**
 * Request message for executing distributed query tasks on a remote node.
 *
 * <p>Contains the work units to be executed, along with context information
 * needed for execution such as stage ID and input data from previous stages.
 *
 * <p><strong>Serialization:</strong>
 * This class implements OpenSearch's Streamable interface for efficient
 * network serialization between cluster nodes.
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

  /**
   * Constructor for deserialization from stream.
   */
  public ExecuteDistributedTaskRequest(StreamInput in) throws IOException {
    super(in);
    // TODO: Phase 1 - Implement proper serialization for WorkUnit and inputData
    // For now, we'll use basic Java serialization as placeholder
    this.stageId = in.readString();

    // Note: WorkUnit serialization will need to be implemented when
    // the actual task operators are created in future phases
    this.workUnits = List.of(); // Placeholder
    this.inputData = null; // Placeholder
  }

  /**
   * Serializes this request to a stream for network transport.
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);

    // TODO: Phase 1 - Implement proper serialization for WorkUnit and inputData
    out.writeString(stageId != null ? stageId : "");

    // Note: WorkUnit serialization will need to be implemented when
    // the actual task operators are created in future phases
    // For now, we write minimal placeholder data
  }

  /**
   * Validates the request before execution.
   *
   * @return true if request is valid for execution
   */
  public boolean isValid() {
    return stageId != null && !stageId.isEmpty() && workUnits != null;
  }

  /**
   * Gets the number of work units in this request.
   */
  public int getWorkUnitCount() {
    return workUnits != null ? workUnits.size() : 0;
  }

  @Override
  public ActionRequestValidationException validate() {
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
        "ExecuteDistributedTaskRequest{stageId='%s', workUnits=%d}",
        stageId, getWorkUnitCount());
  }
}