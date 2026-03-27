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
import com.amazonaws.athena.connectors.jdbc.manager.JdbcRecordHandler;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Handles record reading from Databricks Unity Catalog via JDBC.
 * Builds SQL statements for split-based data retrieval.
 */
public class DatabricksRecordHandler
        extends JdbcRecordHandler
{
    protected DatabricksRecordHandler(String sourceType, Map<String, String> configOptions) {
        super(sourceType, configOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement buildSplitSql(Connection jdbcConnection, String catalogName, TableName tableName, Schema schema, Constraints constraints, Split split) throws SQLException {
        return null;
    }
}
