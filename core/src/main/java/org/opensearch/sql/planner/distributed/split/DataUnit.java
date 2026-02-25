/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.split;

import java.util.List;
import java.util.Map;

/**
 * A unit of data assigned to a SourceOperator. Each DataUnit represents a portion of data to read —
 * typically one OpenSearch shard. Includes preferred nodes for data locality and estimated size for
 * load balancing.
 *
 * <p>Subclasses provide storage-specific details (e.g., {@code OpenSearchDataUnit} adds index name
 * and shard ID).
 */
public abstract class DataUnit {

  /** Returns a unique identifier for this data unit. */
  public abstract String getDataUnitId();

  /** Returns the nodes where this data unit can be read locally (primary + replicas). */
  public abstract List<String> getPreferredNodes();

  /** Returns the estimated number of rows in this data unit. */
  public abstract long getEstimatedRows();

  /** Returns the estimated size in bytes of this data unit. */
  public abstract long getEstimatedSizeBytes();

  /** Returns storage-specific properties for this data unit. */
  public abstract Map<String, String> getProperties();

  /**
   * Returns whether this data unit can be read from any node (true) or requires execution on a
   * preferred node (false). Default is true; OpenSearch shard data units override to false because
   * Lucene requires local access.
   */
  public boolean isRemotelyAccessible() {
    return true;
  }
}
