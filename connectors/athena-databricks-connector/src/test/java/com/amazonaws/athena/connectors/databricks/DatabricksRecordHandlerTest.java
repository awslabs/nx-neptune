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

import com.amazonaws.athena.connector.lambda.data.FieldBuilder;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.JdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.manager.DefaultJdbcFederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import com.google.common.collect.ImmutableMap;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.nullable;

public class DatabricksRecordHandlerTest
{
    private static final String TEST_CATALOG = "testCatalog";
    private static final String TEST_SCHEMA = "testSchema";
    private static final String TEST_TABLE = "testTable";
    private static final String PARTITION_COL = "partition";

    private DatabricksRecordHandler recordHandler;
    private Connection connection;
    private JdbcConnectionFactory jdbcConnectionFactory;

    @Before
    public void setup() throws Exception
    {
        S3Client s3 = Mockito.mock(S3Client.class);
        SecretsManagerClient secrets = Mockito.mock(SecretsManagerClient.class);
        AthenaClient athena = Mockito.mock(AthenaClient.class);
        connection = Mockito.mock(Connection.class);
        jdbcConnectionFactory = Mockito.mock(JdbcConnectionFactory.class);
        Mockito.when(jdbcConnectionFactory.getConnection(nullable(CredentialsProvider.class))).thenReturn(connection);

        JdbcSplitQueryBuilder queryBuilder = new DatabricksQueryStringBuilder(QUOTE_CHARACTER, new DefaultJdbcFederationExpressionParser());
        DatabaseConnectionConfig dbConfig = new DatabaseConnectionConfig(TEST_CATALOG, DATABRICKS_NAME,
                "databricks://jdbc:databricks://hostname/default");

        recordHandler = new DatabricksRecordHandler(dbConfig, s3, secrets, athena, jdbcConnectionFactory, queryBuilder, ImmutableMap.of());
    }

    @Test
    public void testBuildSplitSqlBasic() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("col1", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("col2", Types.MinorType.INT.getType()).build())
                .addField(FieldBuilder.newBuilder(PARTITION_COL, Types.MinorType.VARCHAR.getType()).build())
                .build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(PARTITION_COL, "*"));
        Mockito.when(split.getProperty(PARTITION_COL)).thenReturn("*");

        String expectedSql = "SELECT `col1`, `col2` FROM `testSchema`.`testTable`";
        PreparedStatement expectedStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedStmt);

        Constraints constraints = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        PreparedStatement result = recordHandler.buildSplitSql(connection, TEST_CATALOG, tableName, schema, constraints, split);

        assertEquals(expectedStmt, result);
        Mockito.verify(result).setFetchSize(10000);
    }

    @Test
    public void testBuildSplitSqlExcludesPartitionColumn() throws SQLException
    {
        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("city_name", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder("temperature", Types.MinorType.FLOAT8.getType()).build())
                .addField(FieldBuilder.newBuilder(PARTITION_COL, Types.MinorType.VARCHAR.getType()).build())
                .build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(PARTITION_COL, "*"));
        Mockito.when(split.getProperty(PARTITION_COL)).thenReturn("*");

        // partition column should NOT appear in SELECT
        String expectedSql = "SELECT `city_name`, `temperature` FROM `testSchema`.`testTable`";
        PreparedStatement expectedStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedStmt);

        Constraints constraints = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        PreparedStatement result = recordHandler.buildSplitSql(connection, TEST_CATALOG, tableName, schema, constraints, split);

        assertEquals(expectedStmt, result);
    }

    @Test
    public void testBuildSplitSqlCustomFetchSize() throws Exception
    {
        // Create handler with custom fetch size
        JdbcSplitQueryBuilder queryBuilder = new DatabricksQueryStringBuilder(QUOTE_CHARACTER, new DefaultJdbcFederationExpressionParser());
        DatabaseConnectionConfig dbConfig = new DatabaseConnectionConfig(TEST_CATALOG, DATABRICKS_NAME,
                "databricks://jdbc:databricks://hostname/default");
        DatabricksRecordHandler customHandler = new DatabricksRecordHandler(dbConfig,
                Mockito.mock(S3Client.class), Mockito.mock(SecretsManagerClient.class), Mockito.mock(AthenaClient.class),
                jdbcConnectionFactory, queryBuilder, ImmutableMap.of(FETCH_SIZE_CONFIG_KEY, "5000"));

        TableName tableName = new TableName(TEST_SCHEMA, TEST_TABLE);
        Schema schema = SchemaBuilder.newBuilder()
                .addField(FieldBuilder.newBuilder("col1", Types.MinorType.VARCHAR.getType()).build())
                .addField(FieldBuilder.newBuilder(PARTITION_COL, Types.MinorType.VARCHAR.getType()).build())
                .build();

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(ImmutableMap.of(PARTITION_COL, "*"));
        Mockito.when(split.getProperty(PARTITION_COL)).thenReturn("*");

        String expectedSql = "SELECT `col1` FROM `testSchema`.`testTable`";
        PreparedStatement expectedStmt = Mockito.mock(PreparedStatement.class);
        Mockito.when(connection.prepareStatement(Mockito.eq(expectedSql))).thenReturn(expectedStmt);

        Constraints constraints = new Constraints(
                Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(),
                Constraints.DEFAULT_NO_LIMIT, Collections.emptyMap(), null);

        PreparedStatement result = customHandler.buildSplitSql(connection, TEST_CATALOG, tableName, schema, constraints, split);

        assertEquals(expectedStmt, result);
        Mockito.verify(result).setFetchSize(5000);
    }
}
