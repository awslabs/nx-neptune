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
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.LimitPushdownSubType;
import com.amazonaws.athena.connector.lambda.metadata.optimizations.pushdown.TopNPushdownSubType;
import com.amazonaws.athena.connectors.databricks.resolver.DatabricksJdbcCaseResolver;
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

import java.util.List;
import java.util.Map;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.*;

/**
 * Handles metadata operations for Databricks Unity Catalog via JDBC.
 * Retrieves schema, table, column, and partition information from Databricks.
 */
public class DatabricksMetadataHandler
        extends JdbcMetadataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksMetadataHandler.class);

    private static final String BLOCK_PARTITION_COLUMN_NAME = "partition";
    private static final String ALL_PARTITIONS = "*";

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
                new DatabricksJdbcCaseResolver(DATABRICKS_NAME));
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
                Split.newBuilder(makeSpillLocation(getSplitsRequest), makeEncryptionKey())
                        .add(BLOCK_PARTITION_COLUMN_NAME, ALL_PARTITIONS)
                        .build());
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
        capabilities.put(DataSourceOptimizations.SUPPORTS_LIMIT_PUSHDOWN.withSupportedSubTypes(
                LimitPushdownSubType.INTEGER_CONSTANT
        ));
        capabilities.put(DataSourceOptimizations.SUPPORTS_TOP_N_PUSHDOWN.withSupportedSubTypes(
                TopNPushdownSubType.SUPPORTS_ORDER_BY
        ));
        return new GetDataSourceCapabilitiesResponse(request.getCatalogName(), capabilities.build());
    }
}
