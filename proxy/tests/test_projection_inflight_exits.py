# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Orphan-purge, force exits and vanished-graph reconcile.

Spec: openspec/changes/fix-projection-inflight-guards/specs/projection-api-guards
Covers design D6 (F1), D7 (F2) and D8 (F3). Additive to test_projection_guards.py.
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.auth import get_token
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store

ROUTER = "nx_neptune_proxy.routers.projection"
BASE = "/api/v0/projection"


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
    project = project_store.create(name="Exit Project")
    p = store.create(
        database="mydb",
        node_query="SELECT 1",
        edge_query="SELECT 1",
        graph_name="test-graph",
        s3_staging_bucket="s3://bucket/staging/",
        project_id=project.id,
    )
    if overrides:
        p = store.update(p.id, **overrides)
    return p


def _not_found():
    return ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
        "GetGraph",
    )


async def _instant_sleep(_seconds):
    return None


def _patch_graph_deps(neptune, managed=True):
    """Patch ClientFactory/sleep/prefix guard. get_graph_or_exception stays real."""
    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune
    assert_kwargs = (
        {}
        if managed
        else {"side_effect": HTTPException(status_code=403, detail="not managed")}
    )
    return (
        patch(f"{ROUTER}.ClientFactory", factory),
        patch(f"{ROUTER}.assert_managed_graph", **assert_kwargs),
        patch(f"{ROUTER}.asyncio.sleep", new=_instant_sleep),
    )


def _live_neptune():
    """Neptune whose get_graph returns a graph, then reports it gone after delete."""
    neptune = MagicMock()
    neptune.get_graph.side_effect = [{"name": "x"}, _not_found(), _not_found()]
    return neptune


def _vanished_neptune():
    neptune = MagicMock()
    neptune.get_graph.side_effect = _not_found()
    return neptune


# --- F1: purge of an in-flight record that has no graph yet ---


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["importing", "deleting"])
async def test_purge_refused_while_in_flight_even_without_graph_id(client, status):
    p = _make_projection(status=status)

    resp = await client.delete(f"{BASE}/{p.id}")

    assert resp.status_code == 409
    assert store.get(p.id) is not None


@pytest.mark.asyncio
async def test_purge_immediately_after_execute_is_refused(client):
    p = _make_projection()
    with patch(f"{ROUTER}.run_pipeline"):
        first = await client.post(f"{BASE}/{p.id}/execute")
        purge = await client.delete(f"{BASE}/{p.id}")

    assert (first.status_code, purge.status_code) == (202, 409)
    assert store.get(p.id).status == "importing"


# --- F2: force exits ---


@pytest.mark.asyncio
async def test_force_delete_graph_schedules_deletion_of_stuck_import(client):
    p = _make_projection(status="importing", graph_id="g-abc123")
    neptune = _live_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph?force=true")

    assert resp.status_code == 202
    neptune.delete_graph.assert_called_once()
    assert store.get(p.id).graph_id is None
    assert store.get(p.id).status == "archived"


@pytest.mark.asyncio
async def test_delete_graph_force_defaults_to_false(client):
    p = _make_projection(status="importing", graph_id="g-abc123")
    neptune = _live_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph")

    assert resp.status_code == 409
    neptune.delete_graph.assert_not_called()


@pytest.mark.asyncio
async def test_force_delete_graph_still_refuses_archived(client):
    p = _make_projection(status="archived", graph_id="g-abc123")
    neptune = _live_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph?force=true")

    assert resp.status_code == 409
    neptune.delete_graph.assert_not_called()


@pytest.mark.asyncio
async def test_force_delete_graph_still_enforces_managed_prefix(client):
    p = _make_projection(status="importing", graph_id="g-abc123")
    neptune = _live_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune, managed=False)
    with p1, p2, p3:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph?force=true")

    assert resp.status_code == 403
    neptune.delete_graph.assert_not_called()
    assert store.get(p.id).status == "importing"


@pytest.mark.asyncio
async def test_force_delete_graph_without_graph_id_is_refused(client):
    p = _make_projection(status="importing")
    neptune = _live_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph?force=true")

    assert resp.status_code == 409
    neptune.delete_graph.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["importing", "deleting"])
async def test_force_purge_removes_in_flight_record_without_graph(client, status):
    p = _make_projection(status=status)

    resp = await client.delete(f"{BASE}/{p.id}?force=true")

    assert resp.status_code == 200
    assert store.get(p.id) is None


@pytest.mark.asyncio
async def test_purge_force_defaults_to_false(client):
    p = _make_projection(status="importing")

    resp = await client.delete(f"{BASE}/{p.id}")

    assert resp.status_code == 409
    assert store.get(p.id) is not None


@pytest.mark.asyncio
async def test_force_purge_never_discards_recorded_graph(client):
    p = _make_projection(status="importing", graph_id="g-abc123")

    resp = await client.delete(f"{BASE}/{p.id}?force=true")

    assert resp.status_code == 409
    assert store.get(p.id) is not None


@pytest.mark.asyncio
async def test_unforced_requests_all_refused_for_deleting_with_graph(client):
    p = _make_projection(status="deleting", graph_id="g-abc123")
    neptune = _live_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3, patch(f"{ROUTER}.run_pipeline") as pipeline:
        execute = await client.post(f"{BASE}/{p.id}/execute")
        delete_graph = await client.post(f"{BASE}/{p.id}/delete-graph")
        purge = await client.delete(f"{BASE}/{p.id}")

    assert (execute.status_code, delete_graph.status_code, purge.status_code) == (
        409,
        409,
        409,
    )
    pipeline.assert_not_called()


# --- F3: vanished-graph reconcile ---


@pytest.mark.asyncio
async def test_delete_graph_for_vanished_graph_reconciles_and_returns_404(client):
    p = _make_projection(
        status="failed", graph_id="g-gone", graph_endpoint="g-gone.example.com"
    )
    neptune = _vanished_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph")

    assert resp.status_code == 404
    after = store.get(p.id)
    assert (after.graph_id, after.graph_endpoint, after.status) == (
        None,
        None,
        "archived",
    )
    neptune.delete_graph.assert_not_called()


@pytest.mark.asyncio
async def test_purge_succeeds_after_vanished_graph_reconcile(client):
    p = _make_projection(status="failed", graph_id="g-gone")
    neptune = _vanished_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3:
        first = await client.post(f"{BASE}/{p.id}/delete-graph")
    purge = await client.delete(f"{BASE}/{p.id}")

    assert (first.status_code, purge.status_code) == (404, 200)
    assert store.get(p.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["importing", "deleting"])
async def test_vanished_graph_in_flight_without_force_is_left_unchanged(client, status):
    p = _make_projection(
        status=status, graph_id="g-gone", graph_endpoint="g-gone.example.com"
    )
    neptune = _vanished_neptune()

    p1, p2, p3 = _patch_graph_deps(neptune)
    with p1, p2, p3:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph")

    assert resp.status_code == 404
    assert store.get(p.id) == p
