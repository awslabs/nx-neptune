# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""In-flight guards for projection execute / delete-graph / purge.

Spec: openspec/changes/fix-projection-inflight-guards/specs/projection-api-guards
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
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
    """An idle projection with every field execute requires."""
    project = project_store.create(name="Guard Project")
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


def _fake_neptune():
    """Neptune client whose graph is already gone, so delete polling ends at once."""
    neptune = MagicMock()
    neptune.get_graph.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
        "GetGraph",
    )
    return neptune


def _patch_graph_deps(neptune):
    """Patch the AWS touchpoints of delete-graph; returns a context-manager stack."""
    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune
    return (
        patch(f"{ROUTER}.ClientFactory", factory),
        patch(f"{ROUTER}.get_graph_or_exception", return_value={"name": "x"}),
        patch(f"{ROUTER}.assert_managed_graph"),
        patch(f"{ROUTER}.asyncio.sleep", new=_instant_sleep),
    )


async def _instant_sleep(_seconds):
    return None


# --- Execute: in-flight refusals ---


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["importing", "deleting"])
async def test_execute_refused_with_409_for_in_flight_status(client, status):
    p = _make_projection(status=status)

    with patch(f"{ROUTER}.run_pipeline") as pipeline:
        resp = await client.post(f"{BASE}/{p.id}/execute")

    assert resp.status_code == 409
    pipeline.assert_not_called()


@pytest.mark.asyncio
async def test_execute_during_deleting_leaves_projection_unchanged(client):
    p = _make_projection(
        status="deleting", graph_id="g-abc123", step="graph_delete", progress=0.5
    )

    with patch(f"{ROUTER}.run_pipeline"):
        await client.post(f"{BASE}/{p.id}/execute")

    after = store.get(p.id)
    assert after == p


@pytest.mark.asyncio
async def test_execute_claims_importing_before_pipeline_runs(client):
    """Claim happens in the API, so the guard is closed before the task starts."""
    p = _make_projection()

    with patch(f"{ROUTER}.run_pipeline") as pipeline:
        resp = await client.post(f"{BASE}/{p.id}/execute")

    assert resp.status_code == 202
    # run_pipeline is mocked, so only the router could have written this status.
    assert store.get(p.id).status == "importing"
    pipeline.assert_called_once()


@pytest.mark.asyncio
async def test_second_execute_after_first_accepted_is_refused(client):
    p = _make_projection()

    with patch(f"{ROUTER}.run_pipeline") as pipeline:
        first = await client.post(f"{BASE}/{p.id}/execute")
        second = await client.post(f"{BASE}/{p.id}/execute")

    assert (first.status_code, second.status_code) == (202, 409)
    assert pipeline.call_count == 1


@pytest.mark.asyncio
async def test_concurrent_executes_exactly_one_wins(client):
    p = _make_projection()

    with patch(f"{ROUTER}.run_pipeline") as pipeline:
        resps = await asyncio.gather(
            *[client.post(f"{BASE}/{p.id}/execute") for _ in range(8)]
        )

    codes = sorted(r.status_code for r in resps)
    assert codes == [202] + [409] * 7
    assert pipeline.call_count == 1


@pytest.mark.asyncio
async def test_execute_missing_field_does_not_claim_projection(client):
    """D5: validation runs before the claim, so a 400 leaves status untouched."""
    p = _make_projection()
    store.update(p.id, s3_staging_bucket=None)

    resp = await client.post(f"{BASE}/{p.id}/execute")

    assert resp.status_code == 400
    assert store.get(p.id).status == "draft"


# --- Delete-graph ---


@pytest.mark.asyncio
async def test_delete_graph_refused_with_409_while_importing(client):
    p = _make_projection(status="importing", graph_id="g-abc123")
    neptune = _fake_neptune()

    p1, p2, p3, p4 = _patch_graph_deps(neptune)
    with p1, p2, p3, p4:
        resp = await client.post(f"{BASE}/{p.id}/delete-graph")

    assert resp.status_code == 409
    neptune.delete_graph.assert_not_called()
    after = store.get(p.id)
    assert (after.status, after.graph_id) == ("importing", "g-abc123")


@pytest.mark.asyncio
async def test_concurrent_delete_graph_exactly_one_wins(client):
    p = _make_projection(status="complete", graph_id="g-abc123")
    neptune = _fake_neptune()

    p1, p2, p3, p4 = _patch_graph_deps(neptune)
    with p1, p2, p3, p4:
        resps = await asyncio.gather(
            *[client.post(f"{BASE}/{p.id}/delete-graph") for _ in range(8)]
        )

    codes = sorted(r.status_code for r in resps)
    assert codes == [202] + [409] * 7
    assert neptune.delete_graph.call_count == 1


# --- Purge ---


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["complete", "failed", "archived"])
async def test_purge_refused_while_graph_id_is_set(client, status):
    p = _make_projection(status=status, graph_id="g-abc123")

    resp = await client.delete(f"{BASE}/{p.id}")

    assert resp.status_code == 409
    assert store.get(p.id) is not None


@pytest.mark.asyncio
async def test_purge_refused_while_deleting(client):
    p = _make_projection(status="deleting")

    resp = await client.delete(f"{BASE}/{p.id}")

    assert resp.status_code == 409
    assert store.get(p.id) is not None


@pytest.mark.asyncio
async def test_purge_succeeds_for_archived_projection_without_graph(client):
    p = _make_projection(status="archived")

    resp = await client.delete(f"{BASE}/{p.id}")

    assert resp.status_code == 200
    assert store.get(p.id) is None
