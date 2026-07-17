# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.services.projection_store import store
from nx_neptune_proxy.services.db import get_connection


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture(autouse=True)
def clear_store():
    conn = get_connection()
    conn.execute("DELETE FROM projections")
    conn.execute("DELETE FROM projects")
    conn.commit()
    conn.close()
    yield
    conn = get_connection()
    conn.execute("DELETE FROM projections")
    conn.execute("DELETE FROM projects")
    conn.commit()
    conn.close()


SAMPLE_BODY = {
    "database": "mydb",
    "sql_query": "SELECT * FROM t",
    "node_query": "SELECT * FROM t",
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
    mock_result.message = ""
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
    store.update(pid, status="executing")

    resp = await client.post(f"/api/v0/projection/{pid}/execute")
    assert resp.status_code == 409


# --- List projections ---


@pytest.mark.asyncio
async def test_list_projections(client):
    await client.post("/api/v0/projection", json=SAMPLE_BODY)
    await client.post("/api/v0/projection", json=SAMPLE_BODY)

    resp = await client.get("/api/v0/projection")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


# --- Malformed body (422) ---


@pytest.mark.asyncio
async def test_create_projection_invalid_body(client):
    resp = await client.post("/api/v0/projection", json={"graph_memory_gb": "not_a_number"})
    assert resp.status_code == 422


# --- Execute → poll lifecycle ---


@pytest.mark.asyncio
async def test_execute_poll_lifecycle(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    with patch("nx_neptune_proxy.routers.projection.run_pipeline"):
        resp = await client.post(f"/api/v0/projection/{pid}/execute")
    assert resp.status_code == 202

    # Status should be executing (set by background task, but we simulate)
    store.update(pid, status="executing", progress=50)
    resp = await client.get(f"/api/v0/projection/{pid}/status")
    assert resp.json()["status"] == "executing"
    assert resp.json()["progress"] == 50

    # Simulate completion
    store.update(pid, status="complete", progress=100, graph_endpoint="https://g-123.neptune-graph.amazonaws.com")
    resp = await client.get(f"/api/v0/projection/{pid}/status")
    assert resp.json()["status"] == "complete"
    assert resp.json()["progress"] == 100
    assert resp.json()["graph_endpoint"] == "https://g-123.neptune-graph.amazonaws.com"


# --- Post-import query ---


@pytest.mark.asyncio
async def test_create_projection_with_post_import_query(client):
    body = {**SAMPLE_BODY, "post_import_query": "MATCH (n) RETURN count(n)"}
    resp = await client.post("/api/v0/projection", json=body)
    assert resp.status_code == 201
    data = resp.json()
    assert data["post_import_query"] == "MATCH (n) RETURN count(n)"


@pytest.mark.asyncio
async def test_update_post_import_query(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    resp = await client.put(f"/api/v0/projection/{pid}", json={"post_import_query": "MATCH (n) RETURN n LIMIT 5"})
    assert resp.status_code == 200
    assert resp.json()["post_import_query"] == "MATCH (n) RETURN n LIMIT 5"


@pytest.mark.asyncio
async def test_run_query_no_graph(client):
    create_resp = await client.post("/api/v0/projection", json={**SAMPLE_BODY, "post_import_query": "MATCH (n) RETURN n"})
    pid = create_resp.json()["id"]

    resp = await client.post(f"/api/v0/projection/{pid}/run-query")
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_run_query_no_query_configured(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-123")

    resp = await client.post(f"/api/v0/projection/{pid}/run-query")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_run_query_success(client):
    body = {**SAMPLE_BODY, "post_import_query": "MATCH (n) RETURN count(n)"}
    create_resp = await client.post("/api/v0/projection", json=body)
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-123")

    mock_results = [{"count(n)": 42}]
    with patch("nx_neptune_proxy.routers.projection.execute_opencypher_query", return_value=mock_results):
        resp = await client.post(f"/api/v0/projection/{pid}/run-query")
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is True
    assert data["row_count"] == 1
    assert data["results"] == mock_results


@pytest.mark.asyncio
async def test_run_query_failure(client):
    body = {**SAMPLE_BODY, "post_import_query": "INVALID CYPHER"}
    create_resp = await client.post("/api/v0/projection", json=body)
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-123")

    with patch("nx_neptune_proxy.routers.projection.execute_opencypher_query", side_effect=Exception("Syntax error")):
        resp = await client.post(f"/api/v0/projection/{pid}/run-query")
    assert resp.status_code == 200
    data = resp.json()
    assert data["success"] is False
    assert "Syntax error" in data["error"]

    # Verify post_import_error was stored
    proj = store.get(pid)
    assert "Syntax error" in proj.post_import_error


# --- Timings ---


@pytest.mark.asyncio
async def test_append_timing_accumulates_sequentially(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]

    # Fresh projection starts with no timings
    assert store.get(pid).timings == []

    store.append_timing(pid, "graph_creation", 1.234)
    store.append_timing(pid, "athena_export", 5.5)
    store.append_timing(pid, "graph_import", 10.0)

    proj = store.get(pid)
    assert [t["phase"] for t in proj.timings] == ["graph_creation", "athena_export", "graph_import"]
    assert proj.timings[0]["seconds"] == 1.234
    assert all("at" in t for t in proj.timings)


@pytest.mark.asyncio
async def test_run_query_records_timing(client):
    body = {**SAMPLE_BODY, "post_import_query": "MATCH (n) RETURN count(n)"}
    create_resp = await client.post("/api/v0/projection", json=body)
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-123")

    with patch("nx_neptune_proxy.routers.projection.execute_opencypher_query", return_value=[{"count(n)": 1}]):
        await client.post(f"/api/v0/projection/{pid}/run-query")
        await client.post(f"/api/v0/projection/{pid}/run-query")

    # Each re-run appends a new sequential post_import_query record
    proj = store.get(pid)
    phases = [t["phase"] for t in proj.timings]
    assert phases == ["post_import_query", "post_import_query"]


@pytest.mark.asyncio
async def test_timings_exposed_in_response(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY)
    pid = create_resp.json()["id"]
    store.append_timing(pid, "graph_creation", 2.0)

    resp = await client.get(f"/api/v0/projection/{pid}")
    assert resp.status_code == 200
    timings = resp.json()["timings"]
    assert len(timings) == 1
    assert timings[0]["phase"] == "graph_creation"
    assert timings[0]["seconds"] == 2.0
