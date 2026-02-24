/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.split;

import java.util.List;
import java.util.Map;

/**
 * Assigns splits to nodes, respecting data locality and load balance. Implementations decide which
 * node should process each split based on preferred nodes, current load, and cluster topology.
 */
public interface SplitAssignment {

  /**
   * Assigns splits to nodes.
   *
   * @param splits the splits to assign
   * @param availableNodes the nodes available for execution
   * @return a mapping from node ID to the list of splits assigned to that node
   */
  Map<String, List<Split>> assign(List<Split> splits, List<String> availableNodes);
}
