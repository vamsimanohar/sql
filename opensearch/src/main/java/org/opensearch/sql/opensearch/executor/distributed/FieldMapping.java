/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor.distributed;

/** Maps an output field name to its physical scan-level field name. */
public record FieldMapping(String outputName, String physicalName) {}
