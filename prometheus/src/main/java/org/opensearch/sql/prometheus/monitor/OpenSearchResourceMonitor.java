/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.monitor;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.function.Supplier;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.sql.monitor.ResourceMonitor;

/**
 * {@link ResourceMonitor} implementation on Elasticsearch. When the heap memory usage exceeds
 * certain threshold, the monitor is not healthy.
 * Todo, add metrics.
 */
@Log4j2
public class OpenSearchResourceMonitor extends ResourceMonitor {
  private final Retry retry;
  private final OpenSearchMemoryHealthy memoryMonitor;

  /**
   * Constructor of ElasticsearchCircuitBreaker.
   */
  public OpenSearchResourceMonitor(
      OpenSearchMemoryHealthy memoryMonitor) {
    RetryConfig config =
        RetryConfig.custom()
            .maxAttempts(3)
            .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(1000))
            .retryExceptions(OpenSearchMemoryHealthy.MemoryUsageExceedException.class)
            .ignoreExceptions(
                OpenSearchMemoryHealthy.MemoryUsageExceedFastFailureException.class)
            .build();
    retry = Retry.of("mem", config);
    this.memoryMonitor = memoryMonitor;
  }

  /**
   * Is Healthy.
   *
   * @return true if healthy, otherwise return false.
   */
  @Override
  public boolean isHealthy() {
    try {
      ByteSizeValue limit = new ByteSizeValue(10000L);
      Supplier<Boolean> booleanSupplier =
          Retry.decorateSupplier(retry,
              () -> memoryMonitor
                  .isMemoryHealthy(limit.getBytes()));
      return booleanSupplier.get();
    } catch (Exception e) {
      return false;
    }
  }
}
