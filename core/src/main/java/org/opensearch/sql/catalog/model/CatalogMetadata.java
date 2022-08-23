/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.catalog.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class CatalogMetadata {

  private static final Logger LOG = LogManager.getLogger();

  @JsonProperty(required = true)
  private String name;

  @JsonProperty(required = true)
  private String uri;

  @JsonProperty(required = true)
  @JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
  private ConnectorType connector;

  private AbstractAuthenticationData authentication;

  /**
   * Converts inputstream of bytes into list of catalog metadata.
   *
   * @param inputStream inputstream.
   * @return List of catalog metadata.
   */
  public static List<CatalogMetadata> fromInputStream(InputStream inputStream) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    try {
      return objectMapper.readValue(inputStream, new TypeReference<>() {
      });
    } catch (IOException e) {
      LOG.error("Catalog Configuration File uploaded is malformed. Verify and re-upload.");
      throw new IllegalArgumentException(
          "Malformed Catalog Configuration Json" + e.getMessage());
    }
  }

}
