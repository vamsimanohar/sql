/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.distributed.exchange;

import org.opensearch.sql.planner.distributed.page.Page;
import org.opensearch.sql.planner.distributed.stage.PartitioningScheme;

/**
 * Buffers output pages from a stage before sending them to downstream consumers via the exchange
 * layer. Provides back-pressure to prevent producers from overwhelming consumers.
 *
 * <p>Serialization format is an implementation detail. The default implementation uses OpenSearch
 * transport ({@code StreamOutput}). A future implementation can use Arrow IPC ({@code
 * ArrowRecordBatch}) for zero-copy columnar exchange.
 */
public interface OutputBuffer extends AutoCloseable {

  /**
   * Enqueues a page for delivery to downstream consumers.
   *
   * @param page the page to send
   */
  void enqueue(Page page);

  /** Signals that no more pages will be enqueued. */
  void setNoMorePages();

  /** Returns true if the buffer is full and the producer should wait (back-pressure). */
  boolean isFull();

  /** Returns the total size of buffered data in bytes. */
  long getBufferedBytes();

  /** Aborts the buffer, discarding any buffered pages. */
  void abort();

  /** Returns true if all pages have been consumed and no more will be produced. */
  boolean isFinished();

  /** Returns the partitioning scheme for this buffer's output. */
  PartitioningScheme getPartitioningScheme();
}
