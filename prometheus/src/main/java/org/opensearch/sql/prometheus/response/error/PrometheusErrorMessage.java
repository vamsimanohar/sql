/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.response.error;

import java.util.Locale;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;

/**
 * OpenSearch Error Message.
 */
public class PrometheusErrorMessage extends ErrorMessage {

  PrometheusErrorMessage(OpenSearchException exception, int status) {
    super(exception, status);
  }

  @Override
  protected String fetchReason() {
    return "Error occurred in OpenSearch engine: " + exception.getMessage();
  }

  /**
   * Currently Sql-Jdbc plugin only supports string type as reason and details in the error
   * messages.
   */
  @Override
  protected String fetchDetails() {
    StringBuilder details = new StringBuilder();
    if (exception instanceof SearchPhaseExecutionException) {
      details.append(
          fetchSearchPhaseExecutionExceptionDetails((SearchPhaseExecutionException) exception));
    } else {
      details.append(((OpenSearchException) exception).getDetailedMessage());
    }
    details.append(
        "\nFor more details, please send request for Json format to see the raw response from "
            + "OpenSearch engine.");
    return details.toString();
  }

  /**
   * Could not deliver the exactly same error messages due to the limit of JDBC types.
   * Currently our cases occurred only SearchPhaseExecutionException instances
   * among all types of OpenSearch exceptions
   * according to the survey, see all types: OpenSearchException.OpenSearchExceptionHandle.
   * Either add methods of fetching details for different types, or re-make a consistent
   * message by not giving
   * detailed messages/root causes but only a suggestion message.
   */
  private String fetchSearchPhaseExecutionExceptionDetails(
      SearchPhaseExecutionException exception) {
    StringBuilder details = new StringBuilder();
    ShardSearchFailure[] shardFailures = exception.shardFailures();
    for (ShardSearchFailure failure : shardFailures) {
      details.append(String.format(Locale.ROOT, "Shard[%d]: %s\n", failure.shardId(),
          failure.getCause().toString()));
    }
    return details.toString();
  }
}
