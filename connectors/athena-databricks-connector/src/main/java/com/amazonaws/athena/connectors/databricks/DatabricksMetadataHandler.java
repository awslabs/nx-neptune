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

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.DataSourceOptimizations;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.OptimizationSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.FilterPushdownSubType;
import com.amazonaws.athena.connectors.databricks.resolver.DataBricksJDBCCaseResolver;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles metadata operations for Databricks Unity Catalog via JDBC.
 * Retrieves schema, table, column, and partition information from Databricks.
 */
public class DatabricksMetadataHandler
        extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksMetadataHandler.class);

    public static final String DATABRICKS_NAME = "databricks";
    public static final String DATABRICKS_DRIVER_CLASS = "com.databricks.client.jdbc.Driver";
    public static final int DATABRICKS_DEFAULT_PORT = 443;

    private static final String BLOCK_PARTITION_COLUMN_NAME = "partition";
    private static final String ALL_PARTITIONS = "*";

    /** Environment variable key for the Databricks SQL warehouse HTTP path. */
    static final String HTTP_PATH_CONFIG_KEY = "databricks_http_path";
    /** Environment variable key for the Databricks Unity Catalog name. */
    static final String CONN_CATALOG_CONFIG_KEY = "databricks_conn_catalog";

    /** Default JDBC connection properties for Databricks. */
    private static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of(
            "databaseTerm", "SCHEMA",
            "ssl", "1",
            "AuthMech", "3",
            "user", "token");

    /**
     * Instantiates handler to be used by Lambda function directly.
     * Reads connection config from environment variables.
     */
    public DatabricksMetadataHandler(Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(DATABRICKS_NAME, configOptions), configOptions);
    }

    /**
     * Instantiates handler with explicit connection config.
     */
    public DatabricksMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> configOptions)
    {
        super(databaseConnectionConfig,
                new GenericJdbcConnectionFactory(databaseConnectionConfig, buildJdbcProperties(configOptions), new DatabaseConnectionInfo(DATABRICKS_DRIVER_CLASS, DATABRICKS_DEFAULT_PORT)),
                configOptions,
                new DataBricksJDBCCaseResolver(DATABRICKS_NAME));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema getPartitionSchema(String catalogName) {
        return SchemaBuilder.newBuilder()
                .addField(BLOCK_PARTITION_COLUMN_NAME, Types.MinorType.VARCHAR.getType())
                .build();
    }

    /**
     * {@inheritDoc}
     * Returns a single partition since Databricks partition pruning is not yet supported.
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {
        blockWriter.writeRows((Block block, int rowNum) -> {
            block.setValue(BLOCK_PARTITION_COLUMN_NAME, rowNum, ALL_PARTITIONS);
            return 1;
        });
    }

    /**
     * {@inheritDoc}
     * Returns a single split covering the entire table.
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest) {
        return new GetSplitsResponse(getSplitsRequest.getCatalogName(),
                Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey()).build());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetDataSourceCapabilitiesResponse doGetDataSourceCapabilities(BlockAllocator allocator, GetDataSourceCapabilitiesRequest request) {
        ImmutableMap.Builder<String, List<OptimizationSubType>> capabilities = ImmutableMap.builder();
        capabilities.put(DataSourceOptimizations.SUPPORTS_FILTER_PUSHDOWN.withSupportedSubTypes(
                FilterPushdownSubType.SORTED_RANGE_SET, FilterPushdownSubType.NULLABLE_COMPARISON
        ));
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }

    /**
     * Builds JDBC properties by combining defaults with httpPath and ConnCatalog from environment config.
     */
    private static Map<String, String> buildJdbcProperties(Map<String, String> configOptions)
    {
        Map<String, String> props = new HashMap<>(JDBC_PROPERTIES);
        props.put("httpPath", configOptions.getOrDefault(HTTP_PATH_CONFIG_KEY, ""));
        props.put("ConnCatalog", configOptions.getOrDefault(CONN_CATALOG_CONFIG_KEY, ""));
        return props;
    }
}
