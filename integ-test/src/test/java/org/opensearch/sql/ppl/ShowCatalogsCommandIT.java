/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class ShowCatalogsCommandIT extends PPLIntegTestCase{

  @Test
  public void testDescribeAllFields() throws IOException {
    JSONObject result = executeQuery("show catalogs");
    verifyColumn(
        result,
        columnName("CATALOG_NAME"),
        columnName("CONNECTOR_TYPE")
    );
  }

}
