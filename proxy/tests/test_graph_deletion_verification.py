# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Graph deletion must be verified, not assumed.

H1: the async project-delete job must not read a deletion timeout as success.
H2: the delete-graph poll loop must not read any AWS error as "graph gone".
In both cases only ResourceNotFoundException proves the graph is gone; anything
else leaves graph_id in place so the guarded purge keeps refusing the row.
"""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.auth import get_token
from nx_neptune_proxy.services import project_deletion
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.project_deletion import delete_project
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store

ROUTER = "nx_neptune_proxy.routers.projection"
PROJECT_BASE = "/api/v0/project"
PROJECTION_BASE = "/api/v0/projection"
GRAPH_ID = "g-abc123"


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


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetGraph")


def _project_with_graph():
    project = project_store.create(name="Verify Project")
    p = store.create(
        database="mydb",
        node_query="SELECT 1",
        edge_query="SELECT 1",
        graph_name="test-graph",
        s3_staging_bucket="s3://bucket/staging/",
        project_id=project.id,
    )
    p = store.update(p.id, graph_id=GRAPH_ID, status="completed")
    return project, p


async def _instant_sleep(_seconds):
    return None


# --- H1: project deletion job, graph-deletion timeout ---


def _job_patches(neptune, timeout=0):
    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune
    return (
        patch(f"{project_deletion.__name__}.ClientFactory", factory),
        patch.object(project_deletion, "POLL_INTERVAL", 0),
        patch.object(project_deletion, "TIMEOUT", timeout),
    )


def _still_live_neptune():
    neptune = MagicMock()
    neptune.get_graph.return_value = {"name": "x", "status": "DELETING"}
    return neptune


def _gone_neptune():
    neptune = MagicMock()
    neptune.get_graph.side_effect = _client_error("ResourceNotFoundException")
    return neptune


@pytest.mark.asyncio
async def test_project_delete_timeout_keeps_graph_id(client):
    project, p = _project_with_graph()

    p1, p2, p3 = _job_patches(_still_live_neptune())
    with p1, p2, p3:
        resp = await client.delete(f"{PROJECT_BASE}/{project.id}")

    assert resp.status_code == 202
    row = store.get(p.id)
    assert row is not None
    assert row.graph_id == GRAPH_ID


@pytest.mark.asyncio
async def test_project_delete_timeout_does_not_delete_project(client):
    project, p = _project_with_graph()

    p1, p2, p3 = _job_patches(_still_live_neptune())
    with p1, p2, p3:
        await client.delete(f"{PROJECT_BASE}/{project.id}")

    assert project_store.get(project.id) is not None


@pytest.mark.asyncio
async def test_project_delete_timeout_does_not_report_child_archived(client):
    project, p = _project_with_graph()

    p1, p2, p3 = _job_patches(_still_live_neptune())
    with p1, p2, p3:
        await client.delete(f"{PROJECT_BASE}/{project.id}")

    assert store.get(p.id).status in {"failed", "deleting"}


@pytest.mark.asyncio
async def test_delete_project_unit_timeout_keeps_child_and_project():
    project, p = _project_with_graph()

    p1, p2, p3 = _job_patches(_still_live_neptune())
    with p1, p2, p3:
        await delete_project(project.id)

    assert store.get(p.id).graph_id == GRAPH_ID
    assert project_store.get(project.id) is not None


@pytest.mark.asyncio
async def test_project_delete_graph_really_gone_removes_child_and_project(client):
    project, p = _project_with_graph()

    p1, p2, p3 = _job_patches(_gone_neptune(), timeout=600)
    with p1, p2, p3:
        resp = await client.delete(f"{PROJECT_BASE}/{project.id}")

    assert resp.status_code == 202
    assert store.get(p.id) is None
    assert project_store.get(project.id) is None


# --- H2: delete-graph poll loop, non-ResourceNotFound errors ---


def _poll_neptune(poll_error):
    """First get_graph (prefix guard) sees a live graph; polls raise poll_error."""
    neptune = MagicMock()
    neptune.get_graph.side_effect = [{"name": "x"}] + [poll_error] * 100
    return neptune


def _graph_patches(neptune):
    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune
    return (
        patch(f"{ROUTER}.ClientFactory", factory),
        patch(f"{ROUTER}.assert_managed_graph"),
        patch(f"{ROUTER}.asyncio.sleep", new=_instant_sleep),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["ThrottlingException", "AccessDeniedException"])
async def test_delete_graph_poll_error_does_not_archive_or_clear_graph(client, code):
    _, p = _project_with_graph()

    p1, p2, p3 = _graph_patches(_poll_neptune(_client_error(code)))
    with p1, p2, p3:
        resp = await client.post(f"{PROJECTION_BASE}/{p.id}/delete-graph")

    assert resp.status_code == 202
    row = store.get(p.id)
    assert row.status != "archived"
    assert row.graph_id == GRAPH_ID


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["ThrottlingException", "AccessDeniedException"])
async def test_purge_after_poll_error_still_refuses_live_graph(client, code):
    _, p = _project_with_graph()

    p1, p2, p3 = _graph_patches(_poll_neptune(_client_error(code)))
    with p1, p2, p3:
        await client.post(f"{PROJECTION_BASE}/{p.id}/delete-graph")
    purge = await client.delete(f"{PROJECTION_BASE}/{p.id}")

    assert purge.status_code == 409
    assert store.get(p.id) is not None
    assert store.get(p.id).graph_id == GRAPH_ID


@pytest.mark.asyncio
async def test_delete_graph_poll_not_found_archives_and_clears_graph(client):
    _, p = _project_with_graph()

    p1, p2, p3 = _graph_patches(
        _poll_neptune(_client_error("ResourceNotFoundException"))
    )
    with p1, p2, p3:
        resp = await client.post(f"{PROJECTION_BASE}/{p.id}/delete-graph")

    assert resp.status_code == 202
    row = store.get(p.id)
    assert row.status == "archived"
    assert row.graph_id is None


@pytest.mark.asyncio
async def test_purge_after_confirmed_deletion_succeeds(client):
    _, p = _project_with_graph()

    p1, p2, p3 = _graph_patches(
        _poll_neptune(_client_error("ResourceNotFoundException"))
    )
    with p1, p2, p3:
        await client.post(f"{PROJECTION_BASE}/{p.id}/delete-graph")
    purge = await client.delete(f"{PROJECTION_BASE}/{p.id}")

    assert purge.status_code == 200
    assert store.get(p.id) is None
