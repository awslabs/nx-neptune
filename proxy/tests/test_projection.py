# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.services.projection_store import store


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture(autouse=True)
def clear_store():
    store._projections.clear()
    yield
    store._projections.clear()


SAMPLE_BODY = {
    "database": "mydb",
    "sql_query": "SELECT * FROM t",
    "graph_name": "test-graph",
    "s3_staging_bucket": "s3://my-bucket/staging/",
}


# --- CRUD ---


@pytest.mark.asyncio
async def test_create_projection(client):
    resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "draft"
    assert data["database"] == "mydb"
    assert data["catalog"] == "AwsDataCatalog"
    assert data["graph_memory_gb"] == 16
    assert "id" in data


@pytest.mark.asyncio
async def test_get_projection(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.get(f"/api/v0/projection/{pid}")
    assert resp.status_code == 200
    assert resp.json()["id"] == pid


@pytest.mark.asyncio
async def test_get_projection_not_found(client):
    resp = await client.get("/api/v0/projection/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_update_projection(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.put(f"/api/v0/projection/{pid}", json={"sql_query": "SELECT id FROM t"})
    assert resp.status_code == 200
    assert resp.json()["sql_query"] == "SELECT id FROM t"
    # Other fields unchanged
    assert resp.json()["database"] == "mydb"


@pytest.mark.asyncio
async def test_update_projection_not_found(client):
    resp = await client.put("/api/v0/projection/nonexistent", json={"sql_query": "x"})
    assert resp.status_code == 404


# --- Status ---


@pytest.mark.asyncio
async def test_get_status(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.get(f"/api/v0/projection/{pid}/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == pid
    assert data["status"] == "draft"
    assert data["progress"] == 0


# --- Validate ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.projection.validate_resources")
async def test_validate_projection(mock_validate, client):
    mock_validate.return_value = [
        {"check": "bucket_region", "passed": True, "error": None},
        {"check": "query_valid", "passed": True, "error": None},
    ]

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.post(f"/api/v0/projection/{pid}/validate")
    assert resp.status_code == 200
    data = resp.json()
    assert data["valid"] is True
    assert len(data["checks"]) == 2


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.projection.validate_resources")
async def test_validate_projection_fails(mock_validate, client):
    mock_validate.return_value = [
        {"check": "bucket_region", "passed": False, "error": "Wrong region"},
    ]

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.post(f"/api/v0/projection/{pid}/validate")
    assert resp.status_code == 200
    assert resp.json()["valid"] is False


# --- Validate query ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.projection.check_athena_query")
async def test_validate_query(mock_check, client):
    mock_result = MagicMock()
    mock_result.passed = True
    mock_result.to_dict.return_value = {"check": "athena_query", "passed": True, "error": None}
    mock_check.return_value = mock_result

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.post(f"/api/v0/projection/{pid}/validate-query")
    assert resp.status_code == 200
    assert resp.json()["valid"] is True


# --- Preview ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.projection.get_athena_query_results")
@patch("nx_neptune_proxy.routers.projection.wait_until_all_complete")
@patch("nx_neptune_proxy.routers.projection.ClientFactory")
async def test_preview(mock_cf, mock_wait, mock_results, client):
    mock_athena = MagicMock()
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-1"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_cf.return_value.athena.return_value = mock_athena
    mock_results.return_value = [["id", "name"], ["1", "Alice"]]

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.post(f"/api/v0/projection/{pid}/preview")
    assert resp.status_code == 200
    data = resp.json()
    assert data["error"] is None
    assert data["results"][0]["columns"] == ["id", "name"]
    assert data["results"][0]["rows"] == [["1", "Alice"]]


# --- Execute ---


@pytest.mark.asyncio
async def test_execute_returns_202(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    with patch("nx_neptune_proxy.routers.projection.run_pipeline"):
        resp = await client.post(f"/api/v0/projection/{pid}/execute")
    assert resp.status_code == 202
    assert resp.json()["status"] == "accepted"


@pytest.mark.asyncio
async def test_execute_conflict_if_already_running(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]
    store.get(pid).status = "executing"

    resp = await client.post(f"/api/v0/projection/{pid}/execute")
    assert resp.status_code == 409
