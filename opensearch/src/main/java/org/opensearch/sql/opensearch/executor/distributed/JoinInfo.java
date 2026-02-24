/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;

/**
 * Holds extracted information about a join: both sides' table names, field names, equi-join key
 * indices, join type, pre-join filters, and field counts.
 */
public record JoinInfo(
    RelNode leftInput,
    RelNode rightInput,
    String leftTableName,
    String rightTableName,
    List<String> leftFieldNames,
    List<String> rightFieldNames,
    List<Integer> leftKeyIndices,
    List<Integer> rightKeyIndices,
    JoinRelType joinType,
    int leftFieldCount,
    int rightFieldCount,
    List<Map<String, Object>> leftFilters,
    List<Map<String, Object>> rightFilters) {}
