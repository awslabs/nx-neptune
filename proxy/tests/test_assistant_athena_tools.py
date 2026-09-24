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
    list_buckets,
    list_catalogs,
    list_tables,
    list_tables_with_columns,
    sample_table,
    validate_bucket,
    validate_sql_queries,
)
from nx_neptune_proxy.services.athena_query import AthenaQueryError, execute_query_rows

TOOLS = "nx_neptune_proxy.assistant.athena_tools"
QUERY = "nx_neptune_proxy.services.athena_query"


# --- metadata tools (no query cost) --------------------------------------


@patch(f"{TOOLS}.agent_s3_client")
@patch(f"{TOOLS}.get_settings")
def test_list_buckets_region_filtered(mock_settings, mock_s3):
    mock_settings.return_value = SimpleNamespace(region="us-west-1")
    client = MagicMock()
    client.list_buckets.return_value = {"Buckets": [{"Name": "b1"}, {"Name": "b2"}]}
    mock_s3.return_value = client

    assert list_buckets() == ["b1", "b2"]
    client.list_buckets.assert_called_once_with(BucketRegion="us-west-1")


@patch(f"{TOOLS}.agent_s3_client")
@patch(f"{TOOLS}.get_settings")
def test_list_buckets_empty_without_region(mock_settings, mock_s3):
    mock_settings.return_value = SimpleNamespace(region="")
    assert list_buckets() == []
    mock_s3.assert_not_called()  # no client built, no S3 call when no region


@patch("nx_neptune.validators.check_bucket_versioning")
@patch("nx_neptune.validators.check_bucket_region")
@patch("nx_neptune.validators.check_bucket_exists")
@patch(f"{TOOLS}.get_settings")
def test_validate_bucket_runs_both_checks_with_region(
    mock_settings, mock_exists, mock_region, mock_versioning
):
    mock_settings.return_value = SimpleNamespace(region="us-west-1")
    mock_exists.return_value = SimpleNamespace(
        to_dict=lambda: {"check": "exists", "passed": True, "message": "ok"}
    )
    mock_region.return_value = SimpleNamespace(
        to_dict=lambda: {"check": "region", "passed": True, "message": "in region"}
    )
    mock_versioning.return_value = SimpleNamespace(
        to_dict=lambda: {"check": "versioning", "passed": True, "message": "enabled"}
    )

    result = validate_bucket("my-bucket")

    # Mirrors the projection /validate button: exists, region, versioning.
    assert [c["check"] for c in result] == ["exists", "region", "versioning"]
    assert all(c["passed"] for c in result)
    mock_exists.assert_called_once_with("my-bucket")
    mock_region.assert_called_once_with("my-bucket", "us-west-1")
    mock_versioning.assert_called_once_with("my-bucket")


@patch("nx_neptune.validators.check_bucket_versioning")
@patch("nx_neptune.validators.check_bucket_region")
@patch("nx_neptune.validators.check_bucket_exists")
@patch(f"{TOOLS}.get_settings")
def test_validate_bucket_skips_region_check_when_no_region(
    mock_settings, mock_exists, mock_region, mock_versioning
):
    mock_settings.return_value = SimpleNamespace(region="")
    mock_exists.return_value = SimpleNamespace(
        to_dict=lambda: {"check": "exists", "passed": True, "message": "ok"}
    )
    mock_versioning.return_value = SimpleNamespace(
        to_dict=lambda: {"check": "versioning", "passed": True, "message": "enabled"}
    )

    result = validate_bucket("my-bucket")

    # Region check is skipped without a region, but versioning still runs
    # (matching the button, which always checks staging-bucket versioning).
    assert [c["check"] for c in result] == ["exists", "versioning"]
    mock_region.assert_not_called()
    mock_versioning.assert_called_once_with("my-bucket")


@patch("nx_neptune.validators.check_athena_query")
def test_validate_sql_queries_checks_each_query_with_its_type(mock_check):
    mock_check.side_effect = [
        SimpleNamespace(passed=True, message="node ok"),
        SimpleNamespace(passed=False, message="missing ~from/~to"),
    ]
    labeled = [
        ("node query 1", "SELECT 1", "node"),
        ("edge query 1", "SELECT 2", "edge"),
    ]

    result = validate_sql_queries(
        labeled, "AwsDataCatalog", "tpch", "s3://staging"
    )

    assert result == [
        {"check": "node query 1", "passed": True, "message": "node ok"},
        {"check": "edge query 1", "passed": False, "message": "missing ~from/~to"},
    ]
    # Each query is checked against the given bucket/catalog/db with its type.
    assert mock_check.call_count == 2
    mock_check.assert_any_call(
        sql_query="SELECT 1",
        database="tpch",
        output_location="s3://staging",
        catalog="AwsDataCatalog",
        query_type="node",
    )
    mock_check.assert_any_call(
        sql_query="SELECT 2",
        database="tpch",
        output_location="s3://staging",
        catalog="AwsDataCatalog",
        query_type="edge",
    )


def test_validate_sql_queries_empty_list_runs_no_checks():
    with patch("nx_neptune.validators.check_athena_query") as mock_check:
        assert validate_sql_queries([], "AwsDataCatalog", "tpch", "s3://staging") == []
        mock_check.assert_not_called()


@patch(f"{TOOLS}.agent_athena_client")
def test_list_catalogs_paginates_and_maps(mock_client):
    athena = MagicMock()
    athena.list_data_catalogs.side_effect = [
        {
            "DataCatalogsSummary": [{"CatalogName": "AwsDataCatalog", "Type": "GLUE"}],
            "NextToken": "t",
        },
        {"DataCatalogsSummary": [{"CatalogName": "fed", "Type": "FEDERATED"}]},
    ]
    mock_client.return_value = athena

    assert list_catalogs() == [
        {"name": "AwsDataCatalog", "type": "GLUE"},
        {"name": "fed", "type": "FEDERATED"},
    ]
    athena.list_data_catalogs.assert_called_with(NextToken="t")


@patch(f"{TOOLS}.agent_athena_client")
def test_list_catalogs_type_optional(mock_client):
    athena = MagicMock()
    athena.list_data_catalogs.return_value = {
        "DataCatalogsSummary": [{"CatalogName": "only-name"}]
    }
    mock_client.return_value = athena

    assert list_catalogs() == [{"name": "only-name", "type": None}]


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


@patch(f"{TOOLS}.list_tables")
@patch(f"{TOOLS}.agent_athena_client")
def test_list_tables_with_columns_batches_requested_tables(mock_client, mock_list):
    mock_list.return_value = ["orders", "customers", "unused"]
    athena = MagicMock()
    athena.get_table_metadata.side_effect = [
        {"TableMetadata": {"Columns": [{"Name": "oid", "Type": "int"}]}},
        {"TableMetadata": {"Columns": [{"Name": "cid", "Type": "string"}]}},
    ]
    mock_client.return_value = athena

    result = list_tables_with_columns("cat", "db", ["orders", "customers"])

    assert result == [
        {"name": "orders", "columns": [{"name": "oid", "type": "int"}]},
        {"name": "customers", "columns": [{"name": "cid", "type": "string"}]},
    ]
    # one metadata call per requested table, not for the whole database
    assert athena.get_table_metadata.call_count == 2


@patch(f"{TOOLS}.list_tables")
@patch(f"{TOOLS}.agent_athena_client")
def test_list_tables_with_columns_skips_unknown_tables(mock_client, mock_list):
    mock_list.return_value = ["orders"]
    athena = MagicMock()
    athena.get_table_metadata.return_value = {
        "TableMetadata": {"Columns": [{"Name": "oid", "Type": "int"}]}
    }
    mock_client.return_value = athena

    # "ghost" is not in the database -> skipped, no raise
    result = list_tables_with_columns("cat", "db", ["orders", "ghost"])

    assert result == [{"name": "orders", "columns": [{"name": "oid", "type": "int"}]}]
    athena.get_table_metadata.assert_called_once_with(
        CatalogName="cat", DatabaseName="db", TableName="orders"
    )


@patch(f"{TOOLS}.list_tables")
@patch(f"{TOOLS}.agent_athena_client")
def test_list_tables_with_columns_empty_input_makes_no_calls(mock_client, mock_list):
    result = list_tables_with_columns("cat", "db", [])
    assert result == []
    mock_list.assert_not_called()
    mock_client.assert_not_called()


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
