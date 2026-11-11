# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Error paths of execute and delete-graph.

Covers the 404 when a row vanishes mid-request, the clean 502 for an
unverifiable graph, and the exit for an archived row that still names a graph.
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.auth import get_token
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.projection_store import store

ROUTER = "nx_neptune_proxy.routers.projection"
BASE = "/api/v0/projection"


@pytest.fixture
def client():
    # raise_app_exceptions=False so an unhandled error shows up as the 500 it
    # is, rather than as an exception thrown out of the test client.
    return AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
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
    project = project_store.create(name="Error Project")
    p = store.create(
        database="mydb",
        node_query="SELECT 1",
        edge_query="SELECT 1",
        graph_name="test-graph",
        s3_staging_bucket="s3://bucket/staging/",
        project_id=project.id,
    )
    return store.update(p.id, **overrides) if overrides else p


def _client_error(code, message="boom"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "GetGraph")


def _patch_neptune(neptune):
    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune
    return (
        patch(f"{ROUTER}.ClientFactory", factory),
        patch(f"{ROUTER}.assert_managed_graph"),
    )


def _live_neptune():
    neptune = MagicMock()
    neptune.get_graph.return_value = {"name": "nx-neptune-test-graph"}
    return neptune


def _vanished_neptune():
    neptune = MagicMock()
    neptune.get_graph.side_effect = _client_error("ResourceNotFoundException", "gone")
    return neptune


def _vanish_after_read():
    """Patch get_or_raise so the row is deleted right after the router reads it."""
    real = projection_service.get_or_raise

    def read_then_vanish(projection_id):
        p = real(projection_id)
        with connection() as conn:
            conn.execute("DELETE FROM projections WHERE id = ?", (projection_id,))
        return p

    return patch.object(
        projection_service, "get_or_raise", side_effect=read_then_vanish
    )


# --- Row vanishes before the claim: 404, not 409 ---


@pytest.mark.asyncio
async def test_execute_returns_404_when_row_vanishes_before_the_claim(client):
    p = _make_projection()

    with _vanish_after_read(), patch(f"{ROUTER}.run_pipeline") as pipeline:
        resp = await client.post(f"{BASE}/{p.id}/execute")

    assert resp.status_code == 404
    pipeline.assert_not_called()


@pytest.mark.asyncio
async def test_delete_graph_returns_404_when_row_vanishes_before_the_claim(client):
    p = _make_projection(status="failed", graph_id="g-abc123")
    neptune = _live_neptune()

    p1, p2 = _patch_neptune(neptune)
    with p1, p2, _vanish_after_read():
        resp = await client.post(f"{BASE}/{p.id}/delete-graph")

    assert resp.status_code == 404
    neptune.delete_graph.assert_not_called()


# --- Unverifiable graph: 502 and the record is untouched ---


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["", "?force=true"])
async def test_delete_graph_returns_502_when_graph_cannot_be_verified(client, query):
    p = _make_projection(status="failed", graph_id="g-abc123")
    neptune = MagicMock()
    neptune.get_graph.side_effect = _client_error(
        "AccessDeniedException",
        "not authorized: arn:aws:iam::123456789012:role/proxy cannot GetGraph",
    )

    p1, p2 = _patch_neptune(neptune)
    with p1, p2:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph{query}")

    assert resp.status_code == 502
    assert "123456789012" not in resp.text
    kept = store.get(p.id)
    assert (kept.graph_id, kept.status) == ("g-abc123", "failed")
    neptune.delete_graph.assert_not_called()


@pytest.mark.asyncio
async def test_purge_keeps_refusing_after_an_unverifiable_graph(client):
    p = _make_projection(status="failed", graph_id="g-abc123")
    neptune = MagicMock()
    neptune.get_graph.side_effect = _client_error("ThrottlingException")

    p1, p2 = _patch_neptune(neptune)
    with p1, p2:
        deleted = await client.post(f"{BASE}/{p.id}/delete-graph")
    purged = await client.delete(f"{BASE}/{p.id}")

    assert (deleted.status_code, purged.status_code) == (502, 409)
    assert store.get(p.id).graph_id == "g-abc123"


# --- Archived with a recorded graph ---


@pytest.mark.asyncio
async def test_archived_with_vanished_graph_reconciles_then_purges(client):
    p = _make_projection(status="archived", graph_id="g-abc123")

    p1, p2 = _patch_neptune(_vanished_neptune())
    with p1, p2:
        deleted = await client.post(f"{BASE}/{p.id}/delete-graph")
    kept = store.get(p.id)
    purged = await client.delete(f"{BASE}/{p.id}")

    assert deleted.status_code == 404
    assert (kept.graph_id, kept.graph_endpoint, kept.status) == (
        None,
        None,
        "archived",
    )
    assert purged.status_code == 200
    assert store.get(p.id) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["", "?force=true"])
async def test_archived_with_live_graph_is_refused_naming_the_archived_state(
    client, query
):
    p = _make_projection(status="archived", graph_id="g-abc123")
    neptune = _live_neptune()

    p1, p2 = _patch_neptune(neptune)
    with p1, p2:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph{query}")

    assert resp.status_code == 409
    detail = resp.json()["detail"].lower()
    assert "archived" in detail
    assert "importing" not in detail
    neptune.delete_graph.assert_not_called()
    assert store.get(p.id).graph_id == "g-abc123"
