# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assistant Athena tool layer (spec §9, Group C): metadata reads, guarded
sampling, and the shared query-execution helper."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nx_neptune_proxy.assistant.athena_tools import (
    MAX_SAMPLE_ROWS,
    AthenaToolError,
    get_columns,
    list_tables,
    sample_table,
)
from nx_neptune_proxy.services.athena_query import AthenaQueryError, execute_query_rows

TOOLS = "nx_neptune_proxy.assistant.athena_tools"
QUERY = "nx_neptune_proxy.services.athena_query"


# --- metadata tools (no query cost) --------------------------------------


@patch(f"{TOOLS}.agent_athena_client")
def test_list_tables_paginates(mock_client):
    athena = MagicMock()
    athena.list_table_metadata.side_effect = [
        {"TableMetadataList": [{"Name": "a"}], "NextToken": "t"},
        {"TableMetadataList": [{"Name": "b"}]},
    ]
    mock_client.return_value = athena

    assert list_tables("cat", "db") == ["a", "b"]
    athena.list_table_metadata.assert_called_with(
        CatalogName="cat", DatabaseName="db", NextToken="t"
    )


@patch(f"{TOOLS}.agent_athena_client")
def test_get_columns_maps_name_and_type(mock_client):
    athena = MagicMock()
    athena.get_table_metadata.return_value = {
        "TableMetadata": {"Columns": [{"Name": "id", "Type": "string"}]}
    }
    mock_client.return_value = athena

    assert get_columns("cat", "db", "t") == [{"name": "id", "type": "string"}]
    athena.get_table_metadata.assert_called_once_with(
        CatalogName="cat", DatabaseName="db", TableName="t"
    )


# --- sample_table guardrails ---------------------------------------------


@patch(f"{TOOLS}.execute_query_rows", new_callable=AsyncMock)
@patch(f"{TOOLS}.get_settings")
@patch(f"{TOOLS}.list_tables")
@patch(f"{TOOLS}.agent_athena_client")
def test_sample_table_clamps_limit_and_validates_and_quotes(
    mock_client, mock_list, mock_settings, mock_exec
):
    mock_list.return_value = ["malware"]
    mock_settings.return_value = SimpleNamespace(config_bucket="s3://b/p")
    mock_exec.return_value = {"columns": ["c"], "rows": [["v"]]}

    result = sample_table("cat", "db", "malware", limit=9999)

    assert result == {"columns": ["c"], "rows": [["v"]]}
    _, kwargs = mock_exec.call_args
    args = mock_exec.call_args.args
    assert kwargs["limit"] == MAX_SAMPLE_ROWS  # clamped down from 9999
    assert kwargs["catalog"] == "cat"
    assert kwargs["database"] == "db"
    assert args[1] == 'SELECT * FROM "malware"'  # validated + quoted identifier
    assert args[2] == "s3://b/p/assistant-samples"  # staging derived from settings


@patch(f"{TOOLS}.execute_query_rows", new_callable=AsyncMock)
@patch(f"{TOOLS}.get_settings")
@patch(f"{TOOLS}.list_tables")
@patch(f"{TOOLS}.agent_athena_client")
def test_sample_table_clamps_limit_up_to_minimum(
    mock_client, mock_list, mock_settings, mock_exec
):
    mock_list.return_value = ["t"]
    mock_settings.return_value = SimpleNamespace(config_bucket="s3://b")
    mock_exec.return_value = {"columns": [], "rows": []}

    sample_table("cat", "db", "t", limit=0)
    assert mock_exec.call_args.kwargs["limit"] == 1  # floored to 1


@patch(f"{TOOLS}.list_tables")
def test_sample_table_rejects_unknown_table(mock_list):
    mock_list.return_value = ["known"]
    with pytest.raises(AthenaToolError, match="not found"):
        sample_table("cat", "db", "unknown")


@patch(f"{TOOLS}.get_settings")
@patch(f"{TOOLS}.list_tables")
def test_sample_table_requires_staging_location(mock_list, mock_settings):
    mock_list.return_value = ["t"]
    mock_settings.return_value = SimpleNamespace(config_bucket="")
    with pytest.raises(AthenaToolError, match="staging location"):
        sample_table("cat", "db", "t")


@patch(f"{TOOLS}.execute_query_rows", new_callable=AsyncMock)
@patch(f"{TOOLS}.get_settings")
@patch(f"{TOOLS}.list_tables")
@patch(f"{TOOLS}.agent_athena_client")
def test_sample_table_normalizes_bare_bucket(
    mock_client, mock_list, mock_settings, mock_exec
):
    mock_list.return_value = ["t"]
    mock_settings.return_value = SimpleNamespace(config_bucket="my-bucket/pre/")
    mock_exec.return_value = {"columns": [], "rows": []}

    sample_table("cat", "db", "t")
    assert mock_exec.call_args.args[2] == "s3://my-bucket/pre/assistant-samples"


# --- shared execute_query_rows helper ------------------------------------


@pytest.mark.asyncio
@patch(f"{QUERY}.get_athena_query_results", return_value=[["c1"], ["v1"]])
@patch(f"{QUERY}.get_query_state", return_value="SUCCEEDED")
@patch(f"{QUERY}.wait_until_all_complete", new_callable=AsyncMock)
@patch(f"{QUERY}._execute_athena_query", return_value="exec-1")
async def test_execute_query_rows_success_wraps_limit(
    mock_exec, mock_wait, mock_state, mock_results
):
    client = MagicMock()
    out = await execute_query_rows(
        client, "SELECT * FROM t", "s3://b", catalog="c", database="d", limit=10
    )
    assert out == {"columns": ["c1"], "rows": [["v1"]]}
    # LIMIT applied to the statement handed to Athena
    assert mock_exec.call_args.args[1] == "SELECT * FROM t LIMIT 10"


@pytest.mark.asyncio
@patch(f"{QUERY}.get_query_failure_reason", return_value="boom")
@patch(f"{QUERY}.get_query_state", return_value="FAILED")
@patch(f"{QUERY}.wait_until_all_complete", new_callable=AsyncMock)
@patch(f"{QUERY}._execute_athena_query", return_value="exec-1")
async def test_execute_query_rows_raises_on_failed_state(
    mock_exec, mock_wait, mock_state, mock_reason
):
    with pytest.raises(AthenaQueryError, match="boom"):
        await execute_query_rows(
            MagicMock(), "SELECT 1", "s3://b", catalog="c", database="d"
        )


@pytest.mark.asyncio
@patch(f"{QUERY}.get_athena_query_results", return_value=[["c1"]])
@patch(f"{QUERY}.get_query_state", return_value="SUCCEEDED")
@patch(f"{QUERY}.wait_until_all_complete", new_callable=AsyncMock)
@patch(f"{QUERY}._execute_athena_query", return_value="exec-1")
async def test_execute_query_rows_no_limit_runs_verbatim(
    mock_exec, mock_wait, mock_state, mock_results
):
    await execute_query_rows(
        MagicMock(), "SELECT 1", "s3://b", catalog="c", database="d"
    )
    assert mock_exec.call_args.args[1] == "SELECT 1"  # unmodified, no LIMIT
