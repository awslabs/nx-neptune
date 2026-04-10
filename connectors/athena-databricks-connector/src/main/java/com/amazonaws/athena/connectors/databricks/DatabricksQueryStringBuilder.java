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
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;

import java.util.Collections;
import java.util.List;

/**
 * Builds SQL queries for Databricks split-based data retrieval.
 */
public class DatabricksQueryStringBuilder
        extends JdbcSplitQueryBuilder
{
    public DatabricksQueryStringBuilder(String quoteCharacters, FederationExpressionParser federationExpressionParser)
    {
        super(quoteCharacters, federationExpressionParser);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
    {
        StringBuilder tableName = new StringBuilder();
        tableName.append(" FROM ");
        if (schema != null && !schema.isEmpty()) {
            tableName.append(quote(schema)).append('.');
        }
        tableName.append(quote(table));
        return tableName.toString();
    }

    /**
     * {@inheritDoc}
     * No partition-based WHERE clauses since partitioning is not yet supported.
     */
    @Override
    protected List<String> getPartitionWhereClauses(Split split)
    {
        // Partitioning is not implemented — all data is read in a single split.
        // For large tables, use LIMIT or WHERE clauses to avoid Lambda timeout/OOM.
        return Collections.emptyList();
    }
}
