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
package com.amazonaws.athena.connectors.databricks.resolver;

import org.junit.Test;

import java.util.List;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_NAME;
import static org.junit.Assert.*;

public class DatabricksJdbcCaseResolverTest
{
    private final DatabricksJdbcCaseResolver resolver = new DatabricksJdbcCaseResolver(DATABRICKS_NAME);

    @Test
    public void testSchemaNameQueryTemplate()
    {
        String query = resolver.getCaseInsensitivelySchemaNameQueryTemplate();
        assertTrue(query.contains("information_schema.schemata"));
        assertTrue(query.contains("lower(schema_name)"));
    }

    @Test
    public void testTableNameQueryTemplate()
    {
        List<String> queries = resolver.getCaseInsensitivelyTableNameQueryTemplate();
        assertEquals(1, queries.size());
        assertTrue(queries.get(0).contains("information_schema.tables"));
        assertTrue(queries.get(0).contains("lower(table_name)"));
    }

    @Test
    public void testSchemaNameColumnKey()
    {
        assertEquals("schema_name", resolver.getCaseInsensitivelySchemaNameColumnKey());
    }

    @Test
    public void testTableNameColumnKey()
    {
        assertEquals("table_name", resolver.getCaseInsensitivelyTableNameColumnKey());
    }
}
