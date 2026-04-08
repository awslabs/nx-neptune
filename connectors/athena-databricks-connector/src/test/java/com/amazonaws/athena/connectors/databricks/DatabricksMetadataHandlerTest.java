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

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetDataSourceCapabilitiesResponse;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DatabricksMetadataHandlerTest
{
    private static final String CATALOG_NAME = "testCatalog";
    private BlockAllocator allocator;

    @Before
    public void setup()
    {
        allocator = new BlockAllocatorImpl();
    }

    @After
    public void tearDown()
    {
        allocator.close();
    }

    @Test
    public void testGetPartitionSchema()
    {
        DatabricksMetadataHandler handler = new DatabricksMetadataHandler(
                new com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig(
                        CATALOG_NAME, DatabricksConstants.DATABRICKS_NAME,
                        "databricks://jdbc:databricks://host/default"),
                java.util.Collections.emptyMap());

        Schema schema = handler.getPartitionSchema(CATALOG_NAME);

        assertEquals(1, schema.getFields().size());
        assertEquals("partition", schema.getFields().get(0).getName());
        assertEquals(Types.MinorType.VARCHAR.getType(), schema.getFields().get(0).getType());
    }

    @Test
    public void testDoGetDataSourceCapabilities()
    {
        DatabricksMetadataHandler handler = new DatabricksMetadataHandler(
                new com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig(
                        CATALOG_NAME, DatabricksConstants.DATABRICKS_NAME,
                        "databricks://jdbc:databricks://host/default"),
                java.util.Collections.emptyMap());

        FederatedIdentity identity = new FederatedIdentity("arn", "account", java.util.Collections.emptyMap(), java.util.Collections.emptyList(), java.util.Collections.emptyMap());
        GetDataSourceCapabilitiesRequest request = new GetDataSourceCapabilitiesRequest(identity, "queryId", CATALOG_NAME);
        GetDataSourceCapabilitiesResponse response = handler.doGetDataSourceCapabilities(allocator, request);

        Map<String, List<OptimizationSubType>> capabilities = response.getCapabilities();
        assertEquals(CATALOG_NAME, response.getCatalogName());
        assertEquals(3, capabilities.size());

        // Filter pushdown
        List<OptimizationSubType> filterPushdown = capabilities.get("supports_filter_pushdown");
        assertNotNull("Filter pushdown should be present", filterPushdown);
        assertEquals(2, filterPushdown.size());
        assertTrue(filterPushdown.stream().anyMatch(s -> s.getSubType().equals("sorted_range_set")));
        assertTrue(filterPushdown.stream().anyMatch(s -> s.getSubType().equals("nullable_comparison")));

        // Limit pushdown
        List<OptimizationSubType> limitPushdown = capabilities.get("supports_limit_pushdown");
        assertNotNull("Limit pushdown should be present", limitPushdown);
        assertEquals(1, limitPushdown.size());
        assertTrue(limitPushdown.stream().anyMatch(s -> s.getSubType().equals("integer_constant")));

        // TopN pushdown
        List<OptimizationSubType> topNPushdown = capabilities.get("supports_top_n_pushdown");
        assertNotNull("TopN pushdown should be present", topNPushdown);
        assertEquals(1, topNPushdown.size());
        assertTrue(topNPushdown.stream().anyMatch(s -> s.getSubType().equals("SUPPORTS_ORDER_BY")));
    }
}
