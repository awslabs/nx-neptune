/*-
 * #%L
 * athena-jdbc
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
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.metadata.*;
import com.amazonaws.athena.connectors.databricks.resolver.DataBricksJDBCCaseResolver;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcMetadataHandler;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

/**
 * Handles metadata operations for Databricks Unity Catalog via JDBC.
 * Retrieves schema, table, column, and partition information from Databricks.
 */
public class DatabricksMetadataHandler
        extends JdbcMetadataHandler
{

    public static final String DATABRICKS_NAME = "postgres";
    public static final String POSTGRESQL_DRIVER_CLASS = "org.postgresql.Driver";
    public static final int POSTGRESQL_DEFAULT_PORT = 5432;
    public static final String POSTGRES_QUOTE_CHARACTER = "\"";
    public static final Map<String, String> JDBC_PROPERTIES = ImmutableMap.of("databaseTerm", "SCHEMA");

    protected DatabricksMetadataHandler(String sourceType, Map<String, String> configOptions) {
        super(sourceType, configOptions);
    }

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     */
    public DatabricksMetadataHandler(java.util.Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(DATABRICKS_NAME, configOptions), configOptions);
    }

    public DatabricksMetadataHandler(DatabaseConnectionConfig databaseConnectionConfig, java.util.Map<String, String> configOptions)
    {
        super(databaseConnectionConfig,
                new GenericJdbcConnectionFactory(databaseConnectionConfig, JDBC_PROPERTIES, new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, POSTGRESQL_DEFAULT_PORT)),
                configOptions,
                new DataBricksJDBCCaseResolver(DATABRICKS_NAME));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema getPartitionSchema(String catalogName) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) throws Exception {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator blockAllocator, GetSplitsRequest getSplitsRequest) {
        return null;
    }
}
