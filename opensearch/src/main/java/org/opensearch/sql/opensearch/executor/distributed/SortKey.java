/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

/** Represents a sort key with field name, position, direction, and null ordering. */
public record SortKey(String fieldName, int fieldIndex, boolean descending, boolean nullsLast) {}
