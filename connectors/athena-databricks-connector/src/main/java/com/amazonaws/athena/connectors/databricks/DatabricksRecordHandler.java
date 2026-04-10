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

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.connection.GenericJdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.DefaultJdbcFederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.*;

/**
 * Handles record reading from Databricks Unity Catalog via JDBC.
 * Builds SQL statements for split-based data retrieval.
 */
public class DatabricksRecordHandler
        extends JdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksRecordHandler.class);
    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private final int fetchSize;

    /**
     * Instantiates handler to be used by Lambda function directly.
     *
     * Recommend using {@link DatabricksCompositeHandler} instead.
     */
    public DatabricksRecordHandler(Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(DATABRICKS_NAME, configOptions), configOptions);
    }

    /**
     * Instantiates handler with explicit connection config.
     */
    public DatabricksRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, Map<String, String> configOptions)
    {
        this(databaseConnectionConfig,
                S3Client.create(),
                SecretsManagerClient.create(),
                AthenaClient.create(),
                new GenericJdbcConnectionFactory(databaseConnectionConfig, buildJdbcProperties(configOptions), new DatabaseConnectionInfo(DATABRICKS_DRIVER_CLASS, DATABRICKS_DEFAULT_PORT)),
                new DatabricksQueryStringBuilder(QUOTE_CHARACTER, new DefaultJdbcFederationExpressionParser()),
                configOptions);
    }

    @VisibleForTesting
    DatabricksRecordHandler(DatabaseConnectionConfig databaseConnectionConfig, S3Client s3Client, SecretsManagerClient secretsManager,
            AthenaClient athena, JdbcConnectionFactory jdbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder, Map<String, String> configOptions)
    {
        super(s3Client, secretsManager, athena, databaseConnectionConfig, jdbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
        int parsedFetchSize;
        try {
            parsedFetchSize = Integer.parseInt(configOptions.getOrDefault(FETCH_SIZE_CONFIG_KEY, String.valueOf(DEFAULT_FETCH_SIZE)));
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid fetch size config, using default: {}", DEFAULT_FETCH_SIZE, e);
            parsedFetchSize = DEFAULT_FETCH_SIZE;
        }
        this.fetchSize = parsedFetchSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split) throws SQLException {
        PreparedStatement preparedStatement = jdbcSplitQueryBuilder.buildSql(jdbcConnection, null, tableName.getSchemaName(), tableName.getTableName(), schema, constraints, split);
        preparedStatement.setFetchSize(fetchSize);
        return preparedStatement;
    }
}
