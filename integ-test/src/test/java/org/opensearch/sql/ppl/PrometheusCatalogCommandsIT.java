/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PrometheusCatalogCommandsIT extends PPLIntegTestCase {

  @Test
  @SneakyThrows
  public void testSourceMetricCommand() {
    JSONObject response =
        executeQuery("source=my_prometheus.prometheus_http_requests_total");
    verifySchema(response,
        schema(VALUE, "double"),
        schema(TIMESTAMP,  "timestamp"),
        schema("handler",  "string"),
        schema("code",  "string"),
        schema("instance",  "string"),
        schema("job",  "string"));
    Assertions.assertEquals("", response.toString());
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(6, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

  @Test
  @SneakyThrows
  public void testMetricAggregationCommand() {
    JSONObject response =
        executeQuery("source=my_prometheus.prometheus_http_requests_total | stats avg(@value) by span(@timestamp, 15s), handler, job");
    verifySchema(response,
        schema("avg(@value)",  "double"),
        schema("span(@timestamp,15s)", "timestamp"),
        schema("handler", "string"),
        schema("job", "string"));
    Assertions.assertEquals("", response.toString());
    Assertions.assertTrue(response.getInt("size") > 0);
    Assertions.assertEquals(4, response.getJSONArray("datarows").getJSONArray(0).length());
    JSONArray firstRow = response.getJSONArray("datarows").getJSONArray(0);
    for (int i = 0; i < firstRow.length(); i++) {
      Assertions.assertNotNull(firstRow.get(i));
      Assertions.assertTrue(StringUtils.isNotEmpty(firstRow.get(i).toString()));
    }
  }

}
