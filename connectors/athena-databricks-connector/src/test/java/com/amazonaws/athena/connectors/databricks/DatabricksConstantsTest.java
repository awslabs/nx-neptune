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

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.*;
import static org.junit.Assert.*;

public class DatabricksConstantsTest
{
    @Test
    public void testBuildJdbcPropertiesWithConfig()
    {
        Map<String, String> config = new HashMap<>();
        config.put(HTTP_PATH_CONFIG_KEY, "/sql/1.0/warehouses/abc123");
        config.put(CONN_CATALOG_CONFIG_KEY, "my_catalog");

        Map<String, String> props = buildJdbcProperties(config);

        assertEquals("/sql/1.0/warehouses/abc123", props.get("httpPath"));
        assertEquals("my_catalog", props.get("ConnCatalog"));
        assertEquals("SCHEMA", props.get("databaseTerm"));
        assertEquals("1", props.get("ssl"));
        assertEquals("3", props.get("AuthMech"));
        assertEquals("token", props.get("user"));
    }

    @Test
    public void testBuildJdbcPropertiesWithEmptyConfig()
    {
        Map<String, String> props = buildJdbcProperties(Collections.emptyMap());

        assertEquals("", props.get("httpPath"));
        assertEquals("", props.get("ConnCatalog"));
        assertEquals("SCHEMA", props.get("databaseTerm"));
    }

    @Test
    public void testConstants()
    {
        assertEquals("databricks", DATABRICKS_NAME);
        assertEquals("com.databricks.client.jdbc.Driver", DATABRICKS_DRIVER_CLASS);
        assertEquals(443, DATABRICKS_DEFAULT_PORT);
        assertEquals("`", QUOTE_CHARACTER);
    }
}
