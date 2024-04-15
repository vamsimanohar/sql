package org.opensearch.sql.asyncquery.config.model;

/** Interface for extracting and providing SparkExecutionEngineConfig */
public interface SparkExecutionEngineConfigSupplier {

  /**
   * Get SparkExecutionEngineConfig
   *
   * @return {@link SparkExecutionEngineConfig}.
   */
  SparkExecutionEngineConfig getSparkExecutionEngineConfig();
}
