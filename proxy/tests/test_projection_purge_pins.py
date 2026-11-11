# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pins that tell the purge safety checks apart.

The router `graph_id` pre-check and the `graph_id` condition in `purge_on`
both yield HTTP 409, so HTTP-level tests cannot say which one is load-bearing.
Each check is pinned separately here, plus the single-statement atomicity of
`purge_on` and the router's 404 fallback.
"""

import threading
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.auth import get_token
from nx_neptune_proxy.services import projection_service as service_module
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.projection_store import STATUS_IMPORTING, store

BASE = "/api/v0/projection"
GRAPH_MESSAGE = "Delete the graph before purging this projection"


@pytest.fixture
def client():
    return AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://localhost",
        headers={
            "X-Requested-With": "nx-neptune",
            "Authorization": f"Bearer {get_token()}",
        },
    )


@pytest.fixture(autouse=True)
def clean_tables():
    with connection() as conn:
        conn.execute("DELETE FROM projections")
        conn.execute("DELETE FROM projects")
    yield
    with connection() as conn:
        conn.execute("DELETE FROM projections")
        conn.execute("DELETE FROM projects")


def _make_projection(**overrides):
    project = project_store.create(name="Pin Project")
    p = store.create(
        database="mydb",
        node_query="SELECT 1",
        edge_query="SELECT 1",
        graph_name="test-graph",
        s3_staging_bucket="s3://bucket/staging/",
        project_id=project.id,
    )
    return store.update(p.id, **overrides) if overrides else p


# --- purge_on's own graph_id condition (router bypassed) ---


@pytest.mark.parametrize("force", [False, True])
@pytest.mark.parametrize("status", ["draft", "complete", "failed", "archived"])
def test_service_purge_refuses_a_row_with_a_graph_id(status, force):
    p = _make_projection(status=status, graph_id="g-abc123")

    assert projection_service.purge(p.id, force=force) is False
    assert store.get(p.id).graph_id == "g-abc123"


@pytest.mark.parametrize("force", [False, True])
def test_service_purge_refuses_in_flight_row_with_graph_id(force):
    p = _make_projection(status="importing", graph_id="g-abc123")

    assert projection_service.purge(p.id, force=force) is False
    assert store.get(p.id) is not None


def test_store_purge_on_refuses_a_row_with_a_graph_id_even_when_forced():
    p = _make_projection(graph_id="g-abc123")

    with connection() as conn:
        assert store.purge_on(conn, p.id, force=True) is False
    assert store.get(p.id) is not None


def test_service_purge_removes_idle_row_without_graph():
    p = _make_projection()

    assert projection_service.purge(p.id) is True
    assert store.get(p.id) is None


def test_service_purge_force_removes_in_flight_row_without_graph():
    p = _make_projection(status="importing")

    assert projection_service.purge(p.id) is False
    assert projection_service.purge(p.id, force=True) is True
    assert store.get(p.id) is None


# --- The router's graph_id pre-check, told apart by its message ---


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["", "?force=true"])
@pytest.mark.parametrize("status", ["draft", "importing", "failed"])
async def test_router_names_the_graph_when_one_is_attached(client, status, query):
    p = _make_projection(status=status, graph_id="g-abc123")

    resp = await client.delete(f"{BASE}/{p.id}{query}")

    assert resp.status_code == 409
    assert GRAPH_MESSAGE in resp.json()["detail"]


@pytest.mark.asyncio
async def test_router_uses_the_busy_message_for_in_flight_without_graph(client):
    p = _make_projection(status="importing")

    resp = await client.delete(f"{BASE}/{p.id}")

    assert resp.status_code == 409
    assert GRAPH_MESSAGE not in resp.json()["detail"]
    assert "busy" in resp.json()["detail"]


# --- Purge 404 fallback ---


@pytest.mark.asyncio
async def test_purge_returns_404_when_row_vanishes_between_read_and_purge(client):
    p = _make_projection()
    real_purge = projection_service.purge

    def vanish_then_purge(projection_id, force=False):
        with connection() as conn:
            conn.execute("DELETE FROM projections WHERE id = ?", (projection_id,))
        return real_purge(projection_id, force=force)

    with patch.object(projection_service, "purge", side_effect=vanish_then_purge):
        resp = await client.delete(f"{BASE}/{p.id}")

    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_purge_of_unknown_projection_is_404(client):
    resp = await client.delete(f"{BASE}/does-not-exist")

    assert resp.status_code == 404


# --- Single-statement atomicity of purge_on ---


def _projection_statements(statements):
    # SQLite echoes the parent statement once per ON DELETE CASCADE trigger
    # program, so collapse consecutive repeats into one issued statement.
    issued = [s for s in statements if "projections" in s.lower()]
    return [s for i, s in enumerate(issued) if i == 0 or s != issued[i - 1]]


def test_purge_on_issues_exactly_one_delete_and_never_reads_the_row():
    p = _make_projection()
    statements: list[str] = []

    with connection() as conn:
        conn.set_trace_callback(statements.append)
        assert store.purge_on(conn, p.id) is True
        conn.set_trace_callback(None)

    touching = _projection_statements(statements)
    assert len(touching) == 1, touching
    assert touching[0].lstrip().upper().startswith("DELETE")


def test_purge_on_refusal_is_decided_by_the_same_single_statement():
    p = _make_projection(graph_id="g-abc123")
    statements: list[str] = []

    with connection() as conn:
        conn.set_trace_callback(statements.append)
        assert store.purge_on(conn, p.id) is False
        conn.set_trace_callback(None)

    touching = _projection_statements(statements)
    assert len(touching) == 1, touching
    assert touching[0].lstrip().upper().startswith("DELETE")


def test_service_purge_reads_no_projection_row_before_deleting():
    p = _make_projection()
    statements: list[str] = []
    real = service_module.connection

    @contextmanager
    def traced():
        with real() as conn:
            conn.set_trace_callback(statements.append)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)

    with patch.object(service_module, "connection", traced):
        assert projection_service.purge(p.id) is True

    selects = [
        s
        for s in _projection_statements(statements)
        if s.lstrip().upper().startswith("SELECT")
    ]
    assert selects == []


@pytest.mark.parametrize("round_", range(30))
def test_purge_versus_claim_race_never_runs_a_pipeline_on_a_deleted_row(round_):
    p = _make_projection()
    barrier = threading.Barrier(2)
    result: dict = {}

    def purge():
        barrier.wait()
        result["purge"] = projection_service.purge(p.id)

    def claim():
        barrier.wait()
        result["claim"] = projection_service.try_transition(p.id, STATUS_IMPORTING)

    threads = [threading.Thread(target=purge), threading.Thread(target=claim)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Exactly one side wins, and the row's fate agrees with the winner.
    assert result["purge"] != result["claim"], result
    row = store.get(p.id)
    assert (row is None) == result["purge"]
    if result["claim"]:
        assert row.status == STATUS_IMPORTING
