/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.dataunit;

import java.util.List;
import java.util.Map;

/**
 * Assigns data units to nodes, respecting data locality and load balance. Implementations decide
 * which node should process each data unit based on preferred nodes, current load, and cluster
 * topology.
 */
public interface DataUnitAssignment {

  /**
   * Assigns data units to nodes.
   *
   * @param dataUnits the data units to assign
   * @param availableNodes the nodes available for execution
   * @return a mapping from node ID to the list of data units assigned to that node
   */
  Map<String, List<DataUnit>> assign(List<DataUnit> dataUnits, List<String> availableNodes);
}
