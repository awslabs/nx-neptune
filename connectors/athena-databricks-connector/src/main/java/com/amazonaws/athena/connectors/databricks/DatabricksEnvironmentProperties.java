/*-
 * #%L
 * athena-databricks
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
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

import com.amazonaws.athena.connectors.jdbc.JdbcEnvironmentProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DATABRICKS_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.DEFAULT_DATABASE_CONFIG_KEY;
import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.HOST_CONFIG_KEY;
import static com.amazonaws.athena.connectors.databricks.DatabricksConstants.SECRET_NAME_CONFIG_KEY;

/**
 * Provides Databricks-specific JDBC environment properties.
 * Builds the connection string from individual environment variables (host, database, secret)
 * so users don't need to provide the full JDBC URL.
 */
public class DatabricksEnvironmentProperties extends JdbcEnvironmentProperties
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksEnvironmentProperties.class);
    /**
     * Builds the {@code default} connection string required by the Athena Federation SDK
     * from individual Lambda environment variables.
     *
     * <p>If a {@code default} env var is already present (e.g. via a Glue connection),
     * it is left unchanged. Otherwise, the connection string is assembled from:
     * <ul>
     *   <li>{@code databricks_host} — workspace hostname</li>
     *   <li>{@code databricks_default_database} — target database (defaults to {@code "default"})</li>
     *   <li>{@code secret_manager_databricks_token_name} — Secrets Manager secret for PAT injection</li>
     * </ul>
     *
     * @return environment map containing the {@code default} connection string and all Lambda env vars
     */
    @Override
    public Map<String, String> createEnvironment()
    {
        Map<String, String> env = super.createEnvironment();
        if (!env.containsKey("default") && env.containsKey(HOST_CONFIG_KEY)) {
            String host = env.get(HOST_CONFIG_KEY);
            String database = env.getOrDefault(DEFAULT_DATABASE_CONFIG_KEY, "default");
            String secret = env.getOrDefault(SECRET_NAME_CONFIG_KEY, "");
            String connectionString = String.format("databricks://jdbc:databricks://%s:%d/%s${%s}",
                    host, DATABRICKS_DEFAULT_PORT, database, secret);
            env.put("default", connectionString);
            LOGGER.trace("Built JDBC connection string: {}", connectionString);
        }
        return env;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String getConnectionStringPrefix(Map<String, String> connectionProperties)
    {
        return "jdbc:databricks://";
    }
}
