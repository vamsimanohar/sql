/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexScan;

@Getter
public class MergeLimitAndIndexScan implements Rule<LogicalLimit> {

  private final Capture<PrometheusLogicalIndexScan> indexScanCapture;

  @Accessors(fluent = true)
  private final Pattern<LogicalLimit> pattern;

  /**
   * Constructor of MergeLimitAndIndexScan.
   */
  public MergeLimitAndIndexScan() {
    this.indexScanCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalLimit.class)
        .with(source()
            .matching(typeOf(PrometheusLogicalIndexScan.class).capturedAs(indexScanCapture)));
  }

  @Override
  public LogicalPlan apply(LogicalLimit plan, Captures captures) {
    PrometheusLogicalIndexScan indexScan = captures.get(indexScanCapture);
    PrometheusLogicalIndexScan.PrometheusLogicalIndexScanBuilder builder =
        PrometheusLogicalIndexScan.builder();
    builder.relationName(indexScan.getRelationName())
        .filter(indexScan.getFilter())
        .offset(plan.getOffset())
        .limit(plan.getLimit());
    if (indexScan.getSortList() != null) {
      builder.sortList(indexScan.getSortList());
    }
    return builder.build();
  }
}
