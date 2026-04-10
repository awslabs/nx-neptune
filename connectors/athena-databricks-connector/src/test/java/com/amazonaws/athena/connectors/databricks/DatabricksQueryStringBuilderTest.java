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
import com.amazonaws.athena.connectors.jdbc.manager.DefaultJdbcFederationExpressionParser;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class DatabricksQueryStringBuilderTest
{
    private DatabricksQueryStringBuilder queryBuilder;

    @Before
    public void setup()
    {
        queryBuilder = new DatabricksQueryStringBuilder("`", new DefaultJdbcFederationExpressionParser());
    }

    @Test
    public void testGetFromClauseWithSchemaAndTable()
    {
        Split split = Mockito.mock(Split.class);
        String result = queryBuilder.getFromClauseWithSplit(null, "my_schema", "my_table", split);
        assertEquals(" FROM `my_schema`.`my_table`", result);
    }

    @Test
    public void testGetFromClauseWithTableOnly()
    {
        Split split = Mockito.mock(Split.class);
        String result = queryBuilder.getFromClauseWithSplit(null, null, "my_table", split);
        assertEquals(" FROM `my_table`", result);
    }

    @Test
    public void testGetFromClauseWithEmptySchema()
    {
        Split split = Mockito.mock(Split.class);
        String result = queryBuilder.getFromClauseWithSplit(null, "", "my_table", split);
        assertEquals(" FROM `my_table`", result);
    }

    @Test
    public void testGetPartitionWhereClausesReturnsEmpty()
    {
        Split split = Mockito.mock(Split.class);
        List<String> clauses = queryBuilder.getPartitionWhereClauses(split);
        assertTrue(clauses.isEmpty());
    }
}
