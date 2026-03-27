/*-
 * #%L
 * athena-databricks
 * %%
 * Copyright (C) 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.databricks;

import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Shared constants for the Databricks Athena connector.
 */
public final class DatabricksConstants
{
    private DatabricksConstants() {}

    public static final String DATABRICKS_NAME = "databricks";
    public static final String DATABRICKS_DRIVER_CLASS = "com.databricks.client.jdbc.Driver";
    public static final int DATABRICKS_DEFAULT_PORT = 443;

    /** Databricks SQL quote character for identifiers. */
    public static final String QUOTE_CHARACTER = "`";

    /** Environment variable key for the Databricks SQL warehouse HTTP path. */
    public static final String HTTP_PATH_CONFIG_KEY = "databricks_http_path";
    /** Environment variable key for the Databricks Unity Catalog name. */
    public static final String CONN_CATALOG_CONFIG_KEY = "databricks_conn_catalog";
    /** Environment variable key for the JDBC fetch size. */
    public static final String FETCH_SIZE_CONFIG_KEY = "databricks_fetch_size";

    public static final int DEFAULT_FETCH_SIZE = 10000;

    /** Default JDBC connection properties for Databricks. */
    public static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of(
            "databaseTerm", "SCHEMA",
            "ssl", "1",
            "AuthMech", "3",
            "user", "token");

    /**
     * Builds JDBC properties by combining defaults with httpPath and ConnCatalog from environment config.
     */
    public static Map<String, String> buildJdbcProperties(Map<String, String> configOptions)
    {
        Map<String, String> props = new HashMap<>(JDBC_PROPERTIES);
        props.put("httpPath", configOptions.getOrDefault(HTTP_PATH_CONFIG_KEY, ""));
        props.put("ConnCatalog", configOptions.getOrDefault(CONN_CATALOG_CONFIG_KEY, ""));
        // Disable Arrow/Cloud Fetch to prevent OOM in memory-constrained Lambda.
        // Results stream row-by-row via Thrift, controlled by PreparedStatement.setFetchSize().
        props.put("EnableArrow", "0");
        return props;
    }
}
