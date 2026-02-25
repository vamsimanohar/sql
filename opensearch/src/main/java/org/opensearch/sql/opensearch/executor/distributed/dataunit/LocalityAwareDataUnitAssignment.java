/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed.dataunit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.opensearch.sql.planner.distributed.dataunit.DataUnit;
import org.opensearch.sql.planner.distributed.dataunit.DataUnitAssignment;

/**
 * Assigns data units to nodes based on data locality. For OpenSearch shards, each shard must run on
 * a node that holds it (primary or replica). The first preferred node that is in the available
 * nodes list is chosen.
 *
 * <p>This is essentially a {@code groupBy(preferredNode)} operation since shards are pinned to
 * specific nodes.
 */
public class LocalityAwareDataUnitAssignment implements DataUnitAssignment {

  @Override
  public Map<String, List<DataUnit>> assign(List<DataUnit> dataUnits, List<String> availableNodes) {
    Set<String> available = new HashSet<>(availableNodes);
    Map<String, List<DataUnit>> assignment = new HashMap<>();

    for (DataUnit dataUnit : dataUnits) {
      String targetNode = findTargetNode(dataUnit, available);
      assignment.computeIfAbsent(targetNode, k -> new ArrayList<>()).add(dataUnit);
    }

    return assignment;
  }

  private String findTargetNode(DataUnit dataUnit, Set<String> availableNodes) {
    for (String preferred : dataUnit.getPreferredNodes()) {
      if (availableNodes.contains(preferred)) {
        return preferred;
      }
    }

    if (!dataUnit.isRemotelyAccessible()) {
      throw new IllegalStateException(
          "DataUnit "
              + dataUnit.getDataUnitId()
              + " requires local access but none of its preferred nodes "
              + dataUnit.getPreferredNodes()
              + " are available");
    }

    // Remotely accessible — should not happen for OpenSearch shards, but handle gracefully
    throw new IllegalStateException(
        "DataUnit "
            + dataUnit.getDataUnitId()
            + " has no preferred node in available nodes. Preferred: "
            + dataUnit.getPreferredNodes()
            + ", Available: "
            + availableNodes);
  }
}
