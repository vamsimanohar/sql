/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.opensearch.sql.spark.data.constants.SparkConstants.DRIVER_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_DELEGATE_CATALOG;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AUTH_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_AWSREGION_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_HOST_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_PORT_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.FLINT_INDEX_STORE_SCHEME_KEY;
import static org.opensearch.sql.spark.data.constants.SparkConstants.HIVE_METASTORE_GLUE_ARN_KEY;

import com.amazonaws.services.emrserverless.model.CancelJobRunResult;
import com.amazonaws.services.emrserverless.model.GetJobRunResult;
import com.amazonaws.services.emrserverless.model.JobRunState;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.json.JSONObject;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasources.auth.DataSourceUserAuthorizationHelperImpl;
import org.opensearch.sql.spark.asyncquery.model.S3GlueSparkSubmitParameters;
import org.opensearch.sql.spark.client.SparkJobClient;
import org.opensearch.sql.spark.client.StartJobRequest;
import org.opensearch.sql.spark.dispatcher.model.DispatchQueryRequest;
import org.opensearch.sql.spark.dispatcher.model.FullyQualifiedTableName;
import org.opensearch.sql.spark.dispatcher.model.IndexDetails;
import org.opensearch.sql.spark.response.JobExecutionResponseReader;
import org.opensearch.sql.spark.rest.model.LangType;
import org.opensearch.sql.spark.utils.SQLQueryUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** This class takes care of understanding query and dispatching job query to emr serverless. */
@AllArgsConstructor
public class SparkQueryDispatcher {

  private static final Logger logger = LogManager.getLogger(SparkQueryDispatcher.class);

  private SparkJobClient sparkJobClient;

  private DataSourceService dataSourceService;

  private DataSourceUserAuthorizationHelperImpl dataSourceUserAuthorizationHelper;

  private JobExecutionResponseReader jobExecutionResponseReader;

  public String dispatch(DispatchQueryRequest dispatchQueryRequest) {
    return sparkJobClient.startJobRun(getStartJobRequest(dispatchQueryRequest));
  }

  // TODO : Fetch from Result Index and then make call to EMR Serverless.
  public JSONObject getQueryResponse(String applicationId, String queryId) {
    GetJobRunResult getJobRunResult = sparkJobClient.getJobRunResult(applicationId, queryId);
    JSONObject result = new JSONObject();
    if (getJobRunResult.getJobRun().getState().equals(JobRunState.SUCCESS.toString())) {
      result = jobExecutionResponseReader.getResultFromOpensearchIndex(queryId);
    }
    result.put("status", getJobRunResult.getJobRun().getState());
    return result;
  }

  public String cancelJob(String applicationId, String jobId) {
    CancelJobRunResult cancelJobRunResult = sparkJobClient.cancelJobRun(applicationId, jobId);
    return cancelJobRunResult.getJobRunId();
  }

  private StartJobRequest getStartJobRequest(DispatchQueryRequest dispatchQueryRequest) {
    if (LangType.SQL.equals(dispatchQueryRequest.getLangType())) {
      if (SQLQueryUtils.isIndexQuery(dispatchQueryRequest.getQuery()))
        return getStartJobRequestForIndexRequest(dispatchQueryRequest);
      else {
        return getStartJobRequestForNonIndexQueries(dispatchQueryRequest);
      }
    }
    throw new UnsupportedOperationException(
        String.format("UnSupported Lang type:: %s", dispatchQueryRequest.getLangType()));
  }

  private String getDataSourceRoleARN(DataSourceMetadata dataSourceMetadata) {
    logger.info("Datasource :" + dataSourceMetadata.getProperties());
    if (DataSourceType.S3GLUE.equals(dataSourceMetadata.getConnector())) {
      return dataSourceMetadata.getProperties().get("glue.auth.role_arn");
    }
    throw new UnsupportedOperationException(
        String.format(
            "UnSupported datasource type for async queries:: %s",
            dataSourceMetadata.getConnector()));
  }

  private String constructSparkParameters(String datasourceName) {
    DataSourceMetadata dataSourceMetadata =
        dataSourceService.getRawDataSourceMetadata(datasourceName);
    S3GlueSparkSubmitParameters s3GlueSparkSubmitParameters = new S3GlueSparkSubmitParameters();
    s3GlueSparkSubmitParameters.addParameter(
        DRIVER_ENV_ASSUME_ROLE_ARN_KEY, getDataSourceRoleARN(dataSourceMetadata));
    s3GlueSparkSubmitParameters.addParameter(
        EXECUTOR_ENV_ASSUME_ROLE_ARN_KEY, getDataSourceRoleARN(dataSourceMetadata));
    s3GlueSparkSubmitParameters.addParameter(
        HIVE_METASTORE_GLUE_ARN_KEY, getDataSourceRoleARN(dataSourceMetadata));
    String opensearchuri = dataSourceMetadata.getProperties().get("glue.indexstore.opensearch.uri");
    URI uri;
    try {
      uri = new URI(opensearchuri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format(
              "Bad URI in indexstore configuration of the : %s datasoure.", datasourceName));
    }
    String auth = dataSourceMetadata.getProperties().get("glue.indexstore.opensearch.auth");
    String region = dataSourceMetadata.getProperties().get("glue.indexstore.opensearch.region");
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_HOST_KEY, uri.getHost());
    s3GlueSparkSubmitParameters.addParameter(
        FLINT_INDEX_STORE_PORT_KEY, String.valueOf(uri.getPort()));
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_SCHEME_KEY, uri.getScheme());
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AUTH_KEY, auth);
    s3GlueSparkSubmitParameters.addParameter(FLINT_INDEX_STORE_AWSREGION_KEY, region);
    s3GlueSparkSubmitParameters.addParameter(
        "spark.sql.catalog." + datasourceName, FLINT_DELEGATE_CATALOG);
    return s3GlueSparkSubmitParameters.toString();
  }

  private StartJobRequest getStartJobRequestForNonIndexQueries(
      DispatchQueryRequest dispatchQueryRequest) {
    StartJobRequest startJobRequest;
    FullyQualifiedTableName fullyQualifiedTableName =
        SQLQueryUtils.extractFullyQualifiedTableName(dispatchQueryRequest.getQuery());
    if (fullyQualifiedTableName == null || fullyQualifiedTableName.getDatasourceName() == null) {
      throw new UnsupportedOperationException("Queries without a datasource are not supported");
    }
    Map<String, String> tags = new HashMap<>();
    dataSourceUserAuthorizationHelper.authorizeDataSource(
        this.dataSourceService.getRawDataSourceMetadata(
            fullyQualifiedTableName.getDatasourceName()));
    String jobName =
        fullyQualifiedTableName.getDatasourceName()
            + "-"
            + fullyQualifiedTableName.getSchemaName()
            + "-"
            + fullyQualifiedTableName.getTableName();
    tags.put("datasource", fullyQualifiedTableName.getDatasourceName());
    tags.put("table", fullyQualifiedTableName.getTableName());
    startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            constructSparkParameters(fullyQualifiedTableName.getDatasourceName()),
            tags);
    return startJobRequest;
  }

  private StartJobRequest getStartJobRequestForIndexRequest(
      DispatchQueryRequest dispatchQueryRequest) {
    StartJobRequest startJobRequest;
    IndexDetails indexDetails = SQLQueryUtils.extractIndexDetails(dispatchQueryRequest.getQuery());
    FullyQualifiedTableName fullyQualifiedTableName = indexDetails.getFullyQualifiedTableName();
    if (fullyQualifiedTableName.getDatasourceName() == null) {
      throw new UnsupportedOperationException("Queries without a datasource are not supported");
    }
    dataSourceUserAuthorizationHelper.authorizeDataSource(
        this.dataSourceService.getRawDataSourceMetadata(
            fullyQualifiedTableName.getDatasourceName()));
    String jobName =
        fullyQualifiedTableName.getDatasourceName()
            + "-"
            + fullyQualifiedTableName.getSchemaName()
            + "-"
            + fullyQualifiedTableName.getTableName()
            + "-"
            + indexDetails.getIndexName();
    Map<String, String> tags = new HashMap<>();
    tags.put("index", indexDetails.getIndexName());
    tags.put("datasource", fullyQualifiedTableName.getDatasourceName());
    tags.put("table", fullyQualifiedTableName.getTableName());
    startJobRequest =
        new StartJobRequest(
            dispatchQueryRequest.getQuery(),
            jobName,
            dispatchQueryRequest.getApplicationId(),
            dispatchQueryRequest.getExecutionRoleARN(),
            constructSparkParameters(fullyQualifiedTableName.getDatasourceName()),
            tags);
    return startJobRequest;
  }
}
