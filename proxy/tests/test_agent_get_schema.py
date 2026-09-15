# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the agent get_schema tool.

Verify it returns metadata only (names/types, no rows), is capped, and reports
truncation — the schema is the agent's grounding, so its shape matters.
"""

from unittest.mock import MagicMock, patch

from nx_neptune_proxy.agent import tools
from nx_neptune_proxy.agent.tools import MAX_TABLES, get_schema, list_databases


def _fake_athena_with_tables(n_tables: int, cols_per_table: int = 3):
    """Build a MagicMock Athena client whose list_table_metadata returns
    n_tables tables (single page), each with cols_per_table columns."""
    tables = [
        {
            "Name": f"table_{i}",
            "Columns": [
                {"Name": f"col_{j}", "Type": "string"} for j in range(cols_per_table)
            ],
        }
        for i in range(n_tables)
    ]
    client = MagicMock()
    client.list_table_metadata.return_value = {"TableMetadataList": tables}
    return client


def _call(client):
    with patch.object(tools.ClientFactory, "athena", return_value=client):
        # get_schema is a strands @tool; call the underlying function directly.
        fn = getattr(get_schema, "__wrapped__", get_schema)
        return fn(database="db", catalog="cat")


def test_returns_tables_and_columns_shape():
    result = _call(_fake_athena_with_tables(2))
    assert result["catalog"] == "cat"
    assert result["database"] == "db"
    assert result["truncated"] is False
    assert [t["name"] for t in result["tables"]] == ["table_0", "table_1"]
    assert result["tables"][0]["columns"] == [
        {"name": "col_0", "type": "string"},
        {"name": "col_1", "type": "string"},
        {"name": "col_2", "type": "string"},
    ]


def test_caps_tables_and_flags_truncation():
    result = _call(_fake_athena_with_tables(MAX_TABLES + 10))
    assert len(result["tables"]) == MAX_TABLES
    assert result["truncated"] is True


def test_metadata_only_no_row_data():
    """The result must contain only names/types — never sampled row values."""
    client = _fake_athena_with_tables(1)
    result = _call(client)
    # No API that returns rows should have been called.
    client.get_query_execution.assert_not_called()
    client.start_query_execution.assert_not_called()
    # Every column entry has exactly name+type keys, nothing resembling a value.
    for t in result["tables"]:
        for c in t["columns"]:
            assert set(c.keys()) == {"name", "type"}


def _fake_athena_with_databases(names):
    client = MagicMock()
    client.list_databases.return_value = {"DatabaseList": [{"Name": n} for n in names]}
    return client


def _call_list_dbs(client, catalog="AwsDataCatalog"):
    with patch.object(tools.ClientFactory, "athena", return_value=client):
        fn = getattr(list_databases, "__wrapped__", list_databases)
        return fn(catalog=catalog)


def test_list_databases_returns_names():
    result = _call_list_dbs(_fake_athena_with_databases(["fraud_db", "sales_db"]))
    assert result["catalog"] == "AwsDataCatalog"
    assert result["truncated"] is False
    assert result["databases"] == ["fraud_db", "sales_db"]


def test_list_databases_metadata_only():
    """Discovery returns names only — never tables or rows."""
    client = _fake_athena_with_databases(["db1"])
    result = _call_list_dbs(client)
    client.list_table_metadata.assert_not_called()
    client.start_query_execution.assert_not_called()
    assert result["databases"] == ["db1"]
