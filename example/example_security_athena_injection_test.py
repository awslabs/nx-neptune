# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
Security test demonstrating that _execute_athena_query separates sql_parameters
from the query string via Athena's ExecutionParameters, preventing SQL injection.

User-supplied values are never interpolated into the QueryString — they are passed
separately via ExecutionParameters, ensuring malicious input is treated as data.
"""
from unittest.mock import MagicMock

from nx_neptune.instance_management import _execute_athena_query


# --- SQL injection payloads ---
SQL_INJECTION_PAYLOADS = [
    "'; DROP TABLE users; --",
    "' OR '1'='1",
    "Robert'); DROP TABLE students;--",
    "' UNION SELECT * FROM information_schema.tables --",
    "1; DELETE FROM orders WHERE '1'='1",
]

OUTPUT_LOCATION = "s3://test-bucket/results/"


def _create_mock_athena_client():
    """Create a mock Athena client that returns a fake QueryExecutionId."""
    mock_client = MagicMock()
    mock_client.start_query_execution.return_value = {
        "QueryExecutionId": "test-execution-id"
    }
    return mock_client


def test_sql_parameters_passed_separately():
    """Verify sql_parameters are passed via ExecutionParameters, not embedded in QueryString."""
    print("=== Test: sql_parameters passed via ExecutionParameters ===")

    query = "SELECT * FROM my_table WHERE id = ? AND name = ?"

    for payload in SQL_INJECTION_PAYLOADS:
        mock_client = _create_mock_athena_client()
        params = ["safe_id", payload]

        _execute_athena_query(mock_client, query, OUTPUT_LOCATION, sql_parameters=params)

        call_kwargs = mock_client.start_query_execution.call_args[1]

        # QueryString must remain the parameterized template
        assert call_kwargs["QueryString"] == query, "QueryString must not be modified"
        assert payload not in call_kwargs["QueryString"], "Payload must not be in QueryString"

        # Payload must be in ExecutionParameters
        assert call_kwargs["ExecutionParameters"] == params, "Parameters must be passed via ExecutionParameters"
        assert payload in call_kwargs["ExecutionParameters"], "Payload must be in ExecutionParameters"

        print(f"  PASS: payload '{payload[:40]}...' in ExecutionParameters, not in QueryString")

    print()


def test_no_parameters_omits_execution_parameters():
    """Verify ExecutionParameters is omitted when no sql_parameters are provided."""
    print("=== Test: no sql_parameters omits ExecutionParameters ===")

    mock_client = _create_mock_athena_client()
    query = "SELECT * FROM my_table"

    _execute_athena_query(mock_client, query, OUTPUT_LOCATION)

    call_kwargs = mock_client.start_query_execution.call_args[1]
    assert "ExecutionParameters" not in call_kwargs, "ExecutionParameters must not be present when no params"
    print("  PASS: ExecutionParameters omitted when sql_parameters is None")

    print()


def test_query_string_never_modified_by_parameters():
    """Verify the QueryString is always passed as-is regardless of parameter content."""
    print("=== Test: QueryString never modified by parameter content ===")

    query = "INSERT INTO my_table VALUES (?, ?, ?)"

    for payload in SQL_INJECTION_PAYLOADS:
        mock_client = _create_mock_athena_client()
        params = [payload, payload, payload]

        _execute_athena_query(mock_client, query, OUTPUT_LOCATION, sql_parameters=params)

        call_kwargs = mock_client.start_query_execution.call_args[1]
        assert call_kwargs["QueryString"] == query, "QueryString must remain unchanged"
        assert call_kwargs["ExecutionParameters"] == params

        print(f"  PASS: QueryString unchanged with payload '{payload[:40]}...'")

    print()


def test_catalog_and_database_do_not_leak_into_query():
    """Verify catalog and database are passed in QueryExecutionContext, not in QueryString."""
    print("=== Test: catalog/database passed via QueryExecutionContext ===")

    mock_client = _create_mock_athena_client()
    query = "SELECT * FROM my_table WHERE col = ?"
    payload = "'; DROP TABLE my_table; --"

    _execute_athena_query(
        mock_client, query, OUTPUT_LOCATION,
        sql_parameters=[payload],
        catalog="my_catalog",
        database="my_database",
    )

    call_kwargs = mock_client.start_query_execution.call_args[1]

    assert call_kwargs["QueryString"] == query
    assert payload not in call_kwargs["QueryString"]
    assert call_kwargs["ExecutionParameters"] == [payload]
    assert call_kwargs["QueryExecutionContext"]["Catalog"] == "my_catalog"
    assert call_kwargs["QueryExecutionContext"]["Database"] == "my_database"

    print("  PASS: catalog/database in QueryExecutionContext, payload in ExecutionParameters")

    print()


def main():
    test_sql_parameters_passed_separately()
    test_no_parameters_omits_execution_parameters()
    test_query_string_never_modified_by_parameters()
    test_catalog_and_database_do_not_leak_into_query()
    print("=" * 50)
    print("All Athena SQL injection security tests passed.")


if __name__ == "__main__":
    main()
