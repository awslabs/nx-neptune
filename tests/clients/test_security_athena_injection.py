# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""
Security test proving that _execute_athena_query separates sql_parameters
from the query string via Athena's ExecutionParameters, preventing SQL injection.

User-supplied values are never interpolated into the QueryString — they are passed
separately via ExecutionParameters, ensuring malicious input is treated as data.

Usage:
    pytest integ_test/test_security_athena_injection.py -v
"""

import pytest
from unittest.mock import MagicMock

from nx_neptune.instance_management import _execute_athena_query

SQL_INJECTION_PAYLOADS = [
    "'; DROP TABLE users; --",
    "' OR '1'='1",
    "Robert'); DROP TABLE students;--",
    "' UNION SELECT * FROM information_schema.tables --",
    "1; DELETE FROM orders WHERE '1'='1",
]

OUTPUT_LOCATION = "s3://test-bucket/results/"


@pytest.fixture
def mock_athena_client():
    client = MagicMock()
    client.start_query_execution.return_value = {
        "QueryExecutionId": "test-execution-id"
    }
    return client


class TestAthenaParameterSeparation:
    """Prove sql_parameters are passed via ExecutionParameters, never embedded in QueryString."""

    @pytest.mark.parametrize("payload", SQL_INJECTION_PAYLOADS)
    def test_sql_parameters_passed_separately(self, mock_athena_client, payload):
        query = "SELECT * FROM my_table WHERE id = ? AND name = ?"
        params = ["safe_id", payload]

        _execute_athena_query(
            mock_athena_client, query, OUTPUT_LOCATION, sql_parameters=params
        )

        call_kwargs = mock_athena_client.start_query_execution.call_args[1]
        assert call_kwargs["QueryString"] == query
        assert payload not in call_kwargs["QueryString"]
        assert call_kwargs["ExecutionParameters"] == params

    @pytest.mark.parametrize("payload", SQL_INJECTION_PAYLOADS)
    def test_query_string_never_modified(self, mock_athena_client, payload):
        query = "INSERT INTO my_table VALUES (?, ?, ?)"
        params = [payload, payload, payload]

        _execute_athena_query(
            mock_athena_client, query, OUTPUT_LOCATION, sql_parameters=params
        )

        call_kwargs = mock_athena_client.start_query_execution.call_args[1]
        assert call_kwargs["QueryString"] == query
        assert call_kwargs["ExecutionParameters"] == params


class TestAthenaNoParameters:
    """Prove ExecutionParameters is omitted when no sql_parameters are provided."""

    def test_no_parameters_omits_execution_parameters(self, mock_athena_client):
        _execute_athena_query(
            mock_athena_client, "SELECT * FROM my_table", OUTPUT_LOCATION
        )

        call_kwargs = mock_athena_client.start_query_execution.call_args[1]
        assert "ExecutionParameters" not in call_kwargs


class TestAthenaCatalogDatabase:
    """Prove catalog/database are passed via QueryExecutionContext, not in QueryString."""

    def test_catalog_and_database_in_context(self, mock_athena_client):
        query = "SELECT * FROM my_table WHERE col = ?"
        payload = "'; DROP TABLE my_table; --"

        _execute_athena_query(
            mock_athena_client,
            query,
            OUTPUT_LOCATION,
            sql_parameters=[payload],
            catalog="my_catalog",
            database="my_database",
        )

        call_kwargs = mock_athena_client.start_query_execution.call_args[1]
        assert call_kwargs["QueryString"] == query
        assert payload not in call_kwargs["QueryString"]
        assert call_kwargs["ExecutionParameters"] == [payload]
        assert call_kwargs["QueryExecutionContext"]["Catalog"] == "my_catalog"
        assert call_kwargs["QueryExecutionContext"]["Database"] == "my_database"
