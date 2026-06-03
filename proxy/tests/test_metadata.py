# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


# --- Athena databases ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_databases(mock_cf, client):
    mock_athena = MagicMock()
    mock_athena.list_databases.return_value = {
        "DatabaseList": [{"Name": "db1"}, {"Name": "db2"}]
    }
    mock_cf.return_value.athena.return_value = mock_athena

    resp = await client.get("/api/v0/metadata/athena/databases")
    assert resp.status_code == 200
    assert resp.json() == {"databases": ["db1", "db2"]}


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_databases_pagination(mock_cf, client):
    mock_athena = MagicMock()
    mock_athena.list_databases.side_effect = [
        {"DatabaseList": [{"Name": "db1"}], "NextToken": "tok1"},
        {"DatabaseList": [{"Name": "db2"}]},
    ]
    mock_cf.return_value.athena.return_value = mock_athena

    resp = await client.get("/api/v0/metadata/athena/databases")
    assert resp.status_code == 200
    assert resp.json() == {"databases": ["db1", "db2"]}


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_databases_custom_catalog(mock_cf, client):
    mock_athena = MagicMock()
    mock_athena.list_databases.return_value = {"DatabaseList": [{"Name": "x"}]}
    mock_cf.return_value.athena.return_value = mock_athena

    resp = await client.get("/api/v0/metadata/athena/databases?catalog=MyCatalog")
    assert resp.status_code == 200
    mock_athena.list_databases.assert_called_with(CatalogName="MyCatalog")


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_databases_access_denied(mock_cf, client):
    mock_athena = MagicMock()
    mock_athena.list_databases.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "No access"}}, "ListDatabases"
    )
    mock_cf.return_value.athena.return_value = mock_athena

    resp = await client.get("/api/v0/metadata/athena/databases")
    assert resp.status_code == 403
    assert resp.json()["error"] == "AccessDeniedException"


# --- Athena tables ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_tables(mock_cf, client):
    mock_athena = MagicMock()
    mock_athena.list_table_metadata.return_value = {
        "TableMetadataList": [{"Name": "t1"}, {"Name": "t2"}]
    }
    mock_cf.return_value.athena.return_value = mock_athena

    resp = await client.get("/api/v0/metadata/athena/tables?database=mydb")
    assert resp.status_code == 200
    assert resp.json() == {"tables": ["t1", "t2"]}


@pytest.mark.asyncio
async def test_list_tables_missing_database(client):
    resp = await client.get("/api/v0/metadata/athena/tables")
    assert resp.status_code == 422


# --- Athena columns ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_columns(mock_cf, client):
    mock_athena = MagicMock()
    mock_athena.get_table_metadata.return_value = {
        "TableMetadata": {"Columns": [{"Name": "id", "Type": "int"}, {"Name": "name", "Type": "string"}]}
    }
    mock_cf.return_value.athena.return_value = mock_athena

    resp = await client.get("/api/v0/metadata/athena/columns?database=mydb&table=mytable")
    assert resp.status_code == 200
    assert resp.json() == {"columns": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]}


@pytest.mark.asyncio
async def test_list_columns_missing_params(client):
    resp = await client.get("/api/v0/metadata/athena/columns?database=mydb")
    assert resp.status_code == 422


# --- S3 buckets ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.Settings")
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_s3_buckets(mock_cf, mock_settings, client):
    mock_s3 = MagicMock()
    mock_s3.list_buckets.return_value = {"Buckets": [{"Name": "bucket1"}, {"Name": "bucket2"}]}
    mock_cf.return_value.s3.return_value = mock_s3
    mock_settings.from_env.return_value.region = "us-west-2"

    resp = await client.get("/api/v0/metadata/s3/buckets")
    assert resp.status_code == 200
    assert resp.json() == {"buckets": ["bucket1", "bucket2"]}
    mock_s3.list_buckets.assert_called_with(BucketRegion="us-west-2")


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.Settings")
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_s3_buckets_no_region(mock_cf, mock_settings, client):
    mock_s3 = MagicMock()
    mock_s3.list_buckets.return_value = {"Buckets": [{"Name": "b1"}]}
    mock_cf.return_value.s3.return_value = mock_s3
    mock_settings.from_env.return_value.region = ""

    resp = await client.get("/api/v0/metadata/s3/buckets")
    assert resp.status_code == 200
    mock_s3.list_buckets.assert_called_with()


# --- Neptune graphs ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_neptune_graphs(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.list_graphs.return_value = {
        "graphs": [
            {"id": "g-123", "name": "my-graph", "status": "AVAILABLE"},
            {"id": "g-456", "name": "other", "status": "CREATING"},
        ]
    }
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/metadata/neptune/graph-analytics")
    assert resp.status_code == 200
    assert resp.json() == {
        "graphs": [
            {"id": "g-123", "name": "my-graph", "status": "AVAILABLE"},
            {"id": "g-456", "name": "other", "status": "CREATING"},
        ]
    }


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_list_neptune_graphs_empty(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.list_graphs.return_value = {"graphs": []}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/metadata/neptune/graph-analytics")
    assert resp.status_code == 200
    assert resp.json() == {"graphs": []}


# --- Error handler tests ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.metadata.ClientFactory")
async def test_unknown_aws_error_returns_502(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.list_graphs.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "Something broke"}}, "ListGraphs"
    )
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/metadata/neptune/graph-analytics")
    assert resp.status_code == 502
    assert resp.json()["error"] == "InternalServerError"
