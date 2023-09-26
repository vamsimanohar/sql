/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.client;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;

public interface SparkJobClient {

  String startJobRun(StartJobRequest startJobRequest);

  GetJobRunResult getJobRunResult(String applicationId, String jobId);

  CancelJobRunResult cancelJobRun(String applicationId, String jobId);
}
