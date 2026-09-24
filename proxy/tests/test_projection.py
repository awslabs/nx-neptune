# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.auth import get_token
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport,
        base_url="http://localhost",
        headers={
            "X-Requested-With": "nx-neptune",
            "Authorization": f"Bearer {get_token()}",
        },
    )


@pytest.fixture(autouse=True)
def clear_store():
    with connection() as conn:
        conn.execute("DELETE FROM projections")
        conn.execute("DELETE FROM projects")
    # Create a default project for tests
    p = project_store.create(name="Test Project")
    global _TEST_PROJECT_ID
    _TEST_PROJECT_ID = p.id
    yield
    with connection() as conn:
        conn.execute("DELETE FROM projections")
        conn.execute("DELETE FROM projects")


_TEST_PROJECT_ID = ""


def SAMPLE_BODY():
    return {
        "database": "mydb",
        "node_query": "SELECT id AS `~id`, type AS `~label` FROM nodes",
        "edge_query": "SELECT src AS `~from`, dst AS `~to`, rel AS `~label` FROM edges",
        "graph_name": "test-graph",
        "s3_staging_bucket": "s3://my-bucket/staging/",
        "project_id": _TEST_PROJECT_ID,
    }


# --- CRUD ---


@pytest.mark.asyncio
async def test_create_projection(client):
    resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "draft"
    assert data["database"] == "mydb"
    assert data["catalog"] == "AwsDataCatalog"
    assert data["graph_memory_gb"] == 16
    assert "id" in data


@pytest.mark.asyncio
async def test_get_projection(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
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
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    resp = await client.put(
        f"/api/v0/projection/{pid}",
        json={
            "database": "updated_db",
            "graph_name": "updated-graph",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["database"] == "updated_db"
    assert resp.json()["graph_name"] == "updated-graph"


@pytest.mark.asyncio
async def test_update_projection_not_found(client):
    resp = await client.put(
        "/api/v0/projection/nonexistent", json={"node_query": "x", "edge_query": "y"}
    )
    assert resp.status_code == 404


# --- Status ---


@pytest.mark.asyncio
async def test_get_status(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
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

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
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

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
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

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    # Queries live in the multi-query store, not on the projection record.
    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "SELECT id FROM nodes"}],
            "edge_queries": [{"sql": "SELECT src, dst FROM edges"}],
        },
    )

    resp = await client.post(f"/api/v0/projection/{pid}/validate-query")
    assert resp.status_code == 200
    assert resp.json()["valid"] is True


# --- Preview ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.services.athena_query.get_athena_query_results")
@patch("nx_neptune_proxy.services.athena_query.wait_until_all_complete")
@patch("nx_neptune_proxy.routers.projection.ClientFactory")
async def test_preview(mock_cf, mock_wait, mock_results, client):
    mock_athena = MagicMock()
    mock_athena.start_query_execution.return_value = {"QueryExecutionId": "exec-1"}
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_cf.return_value.athena.return_value = mock_athena
    mock_results.return_value = [["id", "name"], ["1", "Alice"]]

    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    # Queries live in the multi-query store, not on the projection record.
    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "SELECT id, name FROM nodes"}],
            "edge_queries": [],
        },
    )

    resp = await client.post(f"/api/v0/projection/{pid}/preview")
    assert resp.status_code == 200
    data = resp.json()
    assert data["error"] is None
    assert data["results"][0]["columns"] == ["id", "name"]
    assert data["results"][0]["rows"] == [["1", "Alice"]]


# --- Execute ---


@pytest.mark.asyncio
async def test_execute_returns_202(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    with patch("nx_neptune_proxy.routers.projection.run_pipeline"):
        resp = await client.post(f"/api/v0/projection/{pid}/execute")
    assert resp.status_code == 202
    assert resp.json()["status"] == "accepted"


@pytest.mark.asyncio
async def test_execute_conflict_if_already_running(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]
    store.update(pid, status="executing")

    resp = await client.post(f"/api/v0/projection/{pid}/execute")
    assert resp.status_code == 409


# --- Run graph queries ---


@pytest.mark.asyncio
async def test_run_query_returns_results(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-abc123")

    mock_na = MagicMock()
    mock_na.execute_query.return_value = [{"n": {"~id": "1"}}]
    with patch(
        "nx_neptune_proxy.routers.projection.NeptuneAnalyticsClient",
        return_value=mock_na,
    ) as mock_cls:
        resp = await client.post(
            f"/api/v0/projection/{pid}/run-query",
            json={"queries": ["MATCH (n) RETURN n LIMIT 1", "  "]},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] is None
    # Blank query is skipped, so exactly one result set is returned.
    assert body["results"] == [[{"n": {"~id": "1"}}]]
    mock_cls.assert_called_once_with(graph_id="g-abc123")
    mock_na.execute_query.assert_called_once_with("MATCH (n) RETURN n LIMIT 1")


@pytest.mark.asyncio
async def test_run_query_no_graph_returns_409(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    resp = await client.post(
        f"/api/v0/projection/{pid}/run-query",
        json={"queries": ["MATCH (n) RETURN n"]},
    )
    assert resp.status_code == 409
    assert "No graph" in resp.json()["message"]


@pytest.mark.asyncio
async def test_run_query_client_error_returns_partial(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-abc123")

    mock_na = MagicMock()
    mock_na.execute_query.side_effect = [
        [{"ok": True}],
        ClientError(
            {"Error": {"Code": "InvalidParameterException", "Message": "bad cypher"}},
            "ExecuteQuery",
        ),
    ]
    with patch(
        "nx_neptune_proxy.routers.projection.NeptuneAnalyticsClient",
        return_value=mock_na,
    ):
        resp = await client.post(
            f"/api/v0/projection/{pid}/run-query",
            json={"queries": ["MATCH (n) RETURN n", "BROKEN"]},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["error"] is not None
    # First query's result is preserved before the failure.
    assert body["results"] == [[{"ok": True}]]


# --- Explain (syntax validation) ---


@pytest.mark.asyncio
async def test_explain_query_reports_per_query_validity(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-abc123")

    mock_na = MagicMock()
    # First query plans cleanly; second is rejected by the engine.
    mock_na.client.execute_query.side_effect = [
        {"payload": None},
        ClientError(
            {"Error": {"Code": "MalformedQueryException", "Message": "bad cypher"}},
            "ExecuteQuery",
        ),
    ]
    with patch(
        "nx_neptune_proxy.routers.projection.NeptuneAnalyticsClient",
        return_value=mock_na,
    ):
        resp = await client.post(
            f"/api/v0/projection/{pid}/explain-query",
            json={"queries": ["MATCH (n) RETURN n", "  ", "BROKEN"]},
        )

    assert resp.status_code == 200
    results = resp.json()["results"]
    # Blank query is skipped; one verdict per non-blank query.
    assert len(results) == 2
    assert results[0] == {"valid": True, "error": None}
    assert results[1]["valid"] is False
    assert results[1]["error"]

    # Queries are validated via explainMode on the raw boto client (sent
    # verbatim, NOT EXPLAIN-prefixed); validation does not stop at the failure.
    calls = mock_na.client.execute_query.call_args_list
    assert [c.kwargs["queryString"] for c in calls] == ["MATCH (n) RETURN n", "BROKEN"]
    assert all(c.kwargs["explainMode"] == "STATIC" for c in calls)


@pytest.mark.asyncio
async def test_explain_query_sends_query_verbatim_with_explain_mode(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]
    store.update(pid, graph_id="g-abc123")

    mock_na = MagicMock()
    mock_na.client.execute_query.return_value = {"payload": None}
    with patch(
        "nx_neptune_proxy.routers.projection.NeptuneAnalyticsClient",
        return_value=mock_na,
    ):
        resp = await client.post(
            f"/api/v0/projection/{pid}/explain-query",
            json={"queries": ["MATCH (n) RETURN n"]},
        )

    assert resp.status_code == 200
    assert resp.json()["results"] == [{"valid": True, "error": None}]
    # Sent verbatim with explainMode on the raw boto client — never the literal
    # "EXPLAIN" prefix that the engine rejects at column 1 ("Invalid input 'E'").
    kwargs = mock_na.client.execute_query.call_args.kwargs
    assert kwargs["queryString"] == "MATCH (n) RETURN n"
    assert "EXPLAIN" not in kwargs["queryString"].upper()
    assert kwargs["explainMode"] == "STATIC"


@pytest.mark.asyncio
async def test_explain_query_no_graph_returns_409(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    resp = await client.post(
        f"/api/v0/projection/{pid}/explain-query",
        json={"queries": ["MATCH (n) RETURN n"]},
    )
    assert resp.status_code == 409


# --- List projections ---


@pytest.mark.asyncio
async def test_list_projections(client):
    await client.post("/api/v0/projection", json=SAMPLE_BODY())
    await client.post("/api/v0/projection", json=SAMPLE_BODY())

    resp = await client.get("/api/v0/projection")
    assert resp.status_code == 200
    assert len(resp.json()) == 2


# --- Malformed body (422) ---


@pytest.mark.asyncio
async def test_create_projection_invalid_body(client):
    resp = await client.post(
        "/api/v0/projection", json={"graph_memory_gb": "not_a_number"}
    )
    assert resp.status_code == 422


# --- Execute → poll lifecycle ---


@pytest.mark.asyncio
async def test_execute_poll_lifecycle(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
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
    store.update(
        pid,
        status="complete",
        progress=100,
        graph_endpoint="https://g-123.neptune-graph.amazonaws.com",
    )
    resp = await client.get(f"/api/v0/projection/{pid}/status")
    assert resp.json()["status"] == "complete"
    assert resp.json()["progress"] == 100
    assert resp.json()["graph_endpoint"] == "https://g-123.neptune-graph.amazonaws.com"


# --- Queries endpoints ---


@pytest.mark.asyncio
async def test_get_queries_not_found(client):
    resp = await client.get("/api/v0/projection/nonexistent/queries")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_save_queries_not_found(client):
    resp = await client.put(
        "/api/v0/projection/nonexistent/queries",
        json={
            "node_queries": [{"sql": "SELECT 1"}],
            "edge_queries": [],
        },
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_save_and_get_queries(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    save_resp = await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [
                {"sql": "SELECT id FROM nodes"},
                {"sql": "SELECT id FROM users"},
            ],
            "edge_queries": [{"sql": "SELECT src, dst FROM edges"}],
        },
    )
    assert save_resp.status_code == 200
    data = save_resp.json()
    assert len(data["node_queries"]) == 2
    assert len(data["edge_queries"]) == 1
    assert data["node_queries"][0]["sql"] == "SELECT id FROM nodes"
    assert data["node_queries"][1]["position"] == 1

    get_resp = await client.get(f"/api/v0/projection/{pid}/queries")
    assert get_resp.status_code == 200
    assert get_resp.json() == data


@pytest.mark.asyncio
async def test_save_queries_replaces_previous(client):
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "old query 1"}, {"sql": "old query 2"}],
            "edge_queries": [],
        },
    )

    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "new query only"}],
            "edge_queries": [],
        },
    )

    get_resp = await client.get(f"/api/v0/projection/{pid}/queries")
    data = get_resp.json()
    assert len(data["node_queries"]) == 1
    assert data["node_queries"][0]["sql"] == "new query only"


@pytest.mark.asyncio
async def test_graph_queries_persist_via_queries_endpoint(client):
    """The /queries endpoint round-trips openCypher graph queries (text only)."""
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    save_resp = await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "SELECT id FROM nodes"}],
            "edge_queries": [],
            "graph_queries": [
                {"cypher": "MATCH (n) RETURN n LIMIT 10"},
                {"cypher": "MATCH (a)-[r]->(b) RETURN r"},
            ],
        },
    )
    assert save_resp.status_code == 200
    data = save_resp.json()
    assert [q["cypher"] for q in data["graph_queries"]] == [
        "MATCH (n) RETURN n LIMIT 10",
        "MATCH (a)-[r]->(b) RETURN r",
    ]
    assert data["graph_queries"][1]["position"] == 1

    get_resp = await client.get(f"/api/v0/projection/{pid}/queries")
    assert get_resp.json()["graph_queries"] == data["graph_queries"]


@pytest.mark.asyncio
async def test_queries_omitting_graph_leaves_them_unchanged(client):
    """A node/edge-only save must not wipe previously stored graph queries."""
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "n"}],
            "edge_queries": [],
            "graph_queries": [{"cypher": "MATCH (n) RETURN n"}],
        },
    )
    # Save again WITHOUT graph_queries (the common node/edge-only save).
    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={"node_queries": [{"sql": "n2"}], "edge_queries": []},
    )
    get_resp = await client.get(f"/api/v0/projection/{pid}/queries")
    data = get_resp.json()
    assert [q["cypher"] for q in data["graph_queries"]] == ["MATCH (n) RETURN n"]
    # Sending [] explicitly clears them.
    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={"node_queries": [{"sql": "n2"}], "edge_queries": [], "graph_queries": []},
    )
    get_resp = await client.get(f"/api/v0/projection/{pid}/queries")
    assert get_resp.json()["graph_queries"] == []


@pytest.mark.asyncio
async def test_graph_only_endpoint_preserves_node_edge(client):
    """PUT /graph-queries replaces graph queries without touching node/edge."""
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "SELECT id FROM nodes"}],
            "edge_queries": [{"sql": "SELECT src, dst FROM edges"}],
        },
    )
    graph_resp = await client.put(
        f"/api/v0/projection/{pid}/graph-queries",
        json={"graph_queries": [{"cypher": "MATCH (n) RETURN count(n)"}]},
    )
    assert graph_resp.status_code == 200
    assert graph_resp.json()["graph_queries"][0]["cypher"] == "MATCH (n) RETURN count(n)"

    get_resp = await client.get(f"/api/v0/projection/{pid}/queries")
    data = get_resp.json()
    assert len(data["node_queries"]) == 1
    assert len(data["edge_queries"]) == 1
    assert [q["cypher"] for q in data["graph_queries"]] == ["MATCH (n) RETURN count(n)"]


# --- Delete clears queries (regression) ---


@pytest.mark.asyncio
async def test_delete_projection_clears_queries(client):
    """Deleting a projection must remove its node/edge queries (no orphans)."""
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "SELECT id FROM nodes"}],
            "edge_queries": [{"sql": "SELECT src, dst FROM edges"}],
        },
    )

    # Sanity: rows exist before delete.
    with connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM node_queries WHERE projection_id = ?",
                (pid,),
            ).fetchone()["c"]
            == 1
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM edge_queries WHERE projection_id = ?",
                (pid,),
            ).fetchone()["c"]
            == 1
        )

    del_resp = await client.delete(f"/api/v0/projection/{pid}")
    assert del_resp.status_code == 200
    assert del_resp.json()["status"] == "deleted"

    # No orphaned query rows remain after the projection is gone.
    with connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM node_queries WHERE projection_id = ?",
                (pid,),
            ).fetchone()["c"]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM edge_queries WHERE projection_id = ?",
                (pid,),
            ).fetchone()["c"]
            == 0
        )


@pytest.mark.asyncio
async def test_fk_cascade_backstop_clears_queries(client):
    """Deleting the projection row directly must cascade to query rows.

    Guards the ``PRAGMA foreign_keys = ON`` + ``ON DELETE CASCADE`` backstop so
    a future direct deletion path can't silently re-orphan query rows.
    """
    create_resp = await client.post("/api/v0/projection", json=SAMPLE_BODY())
    pid = create_resp.json()["id"]

    await client.put(
        f"/api/v0/projection/{pid}/queries",
        json={
            "node_queries": [{"sql": "SELECT id FROM nodes"}],
            "edge_queries": [{"sql": "SELECT src, dst FROM edges"}],
        },
    )

    # Delete only the projection row — rely on the FK cascade, not the service.
    with connection() as conn:
        conn.execute("DELETE FROM projections WHERE id = ?", (pid,))

    with connection() as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM node_queries WHERE projection_id = ?",
                (pid,),
            ).fetchone()["c"]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) AS c FROM edge_queries WHERE projection_id = ?",
                (pid,),
            ).fetchone()["c"]
            == 0
        )
