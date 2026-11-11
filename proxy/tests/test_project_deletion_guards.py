# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project deletion must honour the projection in-flight guards.

Spec: openspec/changes/fix-projection-inflight-guards/specs/projection-api-guards
Covers design D11-D14: the endpoint, the async job, the startup resume and the
structural guarantee that only the guarded purge can remove a projection row.
"""

import asyncio
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app, resume_pending_deletions
from nx_neptune_proxy.auth import get_token
from nx_neptune_proxy.services import project_deletion
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.project_deletion import delete_project
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_service import (
    ProjectionService,
    projection_service,
)
from nx_neptune_proxy.services.projection_store import ProjectionStore, store

ENDPOINT = "nx_neptune_proxy.routers.project"
BASE = "/api/v0/project"
SRC = Path(__file__).resolve().parents[1] / "src" / "nx_neptune_proxy"


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


def _child(project_id, **overrides):
    p = store.create(
        database="mydb",
        node_query="SELECT 1",
        edge_query="SELECT 1",
        graph_name="test-graph",
        s3_staging_bucket="s3://bucket/staging/",
        project_id=project_id,
    )
    return store.update(p.id, **overrides) if overrides else p


def _not_found():
    return ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
        "GetGraph",
    )


def _neptune(delete_error=None):
    """Neptune client whose graphs vanish right after delete_graph."""
    neptune = MagicMock()
    neptune.get_graph.side_effect = _not_found()
    if delete_error is not None:
        neptune.delete_graph.side_effect = delete_error
    return neptune


def _patch_job_deps(neptune):
    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune
    return (
        patch(f"{project_deletion.__name__}.ClientFactory", factory),
        patch.object(project_deletion, "POLL_INTERVAL", 0),
    )


# --- Endpoint: DELETE /project/{id} ---


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["importing", "executing"])
async def test_delete_project_refused_while_child_in_flight(client, status):
    project = project_store.create(name="P")
    child = _child(project.id, status=status)

    with patch(f"{ENDPOINT}.delete_project") as job:
        resp = await client.delete(f"{BASE}/{project.id}")

    assert resp.status_code == 409
    assert store.get(child.id) is not None
    assert project_store.get(project.id) is not None
    job.assert_not_called()


@pytest.mark.asyncio
async def test_delete_project_right_after_execute_keeps_child_row(client):
    project = project_store.create(name="P")
    child = _child(project.id)
    with patch("nx_neptune_proxy.routers.projection.run_pipeline"):
        executed = await client.post(f"/api/v0/projection/{child.id}/execute")
        resp = await client.delete(f"{BASE}/{project.id}")

    assert (executed.status_code, resp.status_code) == (202, 409)
    assert store.get(child.id).status == "importing"
    assert project_store.get(project.id) is not None


@pytest.mark.asyncio
async def test_force_delete_project_with_stuck_import_is_accepted(client):
    project = project_store.create(name="P")
    _child(project.id, status="importing")

    with patch(f"{ENDPOINT}.delete_project") as job:
        resp = await client.delete(f"{BASE}/{project.id}?force=true")

    assert resp.status_code == 202
    job.assert_called_once()
    assert job.call_args.args[0] == project.id


@pytest.mark.asyncio
async def test_force_delete_project_claims_child_before_removing_it(client):
    project = project_store.create(name="P")
    child = _child(project.id, status="importing")
    calls = []
    real = projection_service.try_transition

    def spy(pid, to_status, *args, **kwargs):
        calls.append((pid, to_status))
        return real(pid, to_status, *args, **kwargs)

    p1, p2 = _patch_job_deps(_neptune())
    with p1, p2, patch.object(projection_service, "try_transition", side_effect=spy):
        resp = await client.delete(f"{BASE}/{project.id}?force=true")

    assert resp.status_code == 202
    assert (child.id, "deleting") in calls
    assert store.get(child.id) is None
    assert project_store.get(project.id) is None


@pytest.mark.asyncio
async def test_delete_project_tolerates_deleting_child(client):
    project = project_store.create(name="P")
    _child(project.id, status="deleting", graph_id="g-abc123")

    with patch(f"{ENDPOINT}.delete_project"):
        resp = await client.delete(f"{BASE}/{project.id}")

    assert resp.status_code == 202


@pytest.mark.asyncio
async def test_delete_project_with_idle_children_and_no_graphs_removes_all(client):
    project = project_store.create(name="P")
    draft = _child(project.id)
    archived = _child(project.id, status="archived")

    resp = await client.delete(f"{BASE}/{project.id}")

    assert resp.status_code == 204
    assert store.get(draft.id) is None
    assert store.get(archived.id) is None
    assert project_store.get(project.id) is None


@pytest.mark.asyncio
async def test_child_claimed_between_check_and_purge_is_not_removed(client):
    project = project_store.create(name="P")
    child = _child(project.id)
    real = projection_service.list_by_project
    calls = []

    def read_then_lose_to_execute(project_id):
        snapshot = real(project_id)
        if not calls:  # execute claims the child right after the first read
            store.update(child.id, status="importing")
        calls.append(project_id)
        return snapshot

    with patch.object(
        projection_service, "list_by_project", side_effect=read_then_lose_to_execute
    ):
        resp = await client.delete(f"{BASE}/{project.id}")

    assert resp.status_code == 409
    assert store.get(child.id).status == "importing"
    assert project_store.get(project.id) is not None


# --- Async job: delete_project ---


async def test_job_claims_child_before_deleting_its_graph():
    project = project_store.create(name="P")
    child = _child(project.id, graph_id="g-abc123")
    order = []
    real = projection_service.try_transition

    def spy(pid, to_status, *args, **kwargs):
        order.append(("claim", pid, to_status))
        return real(pid, to_status, *args, **kwargs)

    neptune = _neptune()
    neptune.delete_graph.side_effect = lambda **kw: order.append(("graph", kw))
    p1, p2 = _patch_job_deps(neptune)
    with p1, p2, patch.object(projection_service, "try_transition", side_effect=spy):
        await delete_project(project.id)

    assert order[0] == ("claim", child.id, "deleting")
    assert [step[0] for step in order].index("graph") > 0


async def test_job_removes_idle_children_graphs_and_project():
    project = project_store.create(name="P")
    child = _child(project.id, graph_id="g-abc123")
    neptune = _neptune()

    p1, p2 = _patch_job_deps(neptune)
    with p1, p2:
        await delete_project(project.id)

    neptune.delete_graph.assert_called_once_with(
        graphIdentifier="g-abc123", skipSnapshot=True
    )
    assert store.get(child.id) is None
    assert project_store.get(project.id) is None


async def test_job_retains_child_that_started_importing_and_reverts_project():
    project = project_store.create(name="P")
    project_store.update(project.id, status="deleting")
    child = _child(project.id, status="importing", graph_id="g-abc123")
    neptune = _neptune()

    p1, p2 = _patch_job_deps(neptune)
    with p1, p2:
        await delete_project(project.id)

    neptune.delete_graph.assert_not_called()
    kept = store.get(child.id)
    assert (kept.status, kept.graph_id) == ("importing", "g-abc123")
    assert project_store.get(project.id).status == "active"


async def test_job_retains_child_without_graph_that_started_importing():
    project = project_store.create(name="P")
    project_store.update(project.id, status="deleting")
    child = _child(project.id, status="importing")

    p1, p2 = _patch_job_deps(_neptune())
    with p1, p2:
        await delete_project(project.id)

    assert store.get(child.id).status == "importing"
    assert project_store.get(project.id).status == "active"


async def test_job_keeps_child_with_graph_and_project_when_graph_delete_fails():
    project = project_store.create(name="P")
    project_store.update(project.id, status="deleting")
    child = _child(project.id, graph_id="g-abc123")
    denied = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "DeleteGraph"
    )

    p1, p2 = _patch_job_deps(_neptune(delete_error=denied))
    with p1, p2:
        await delete_project(project.id)

    assert store.get(child.id).graph_id == "g-abc123"
    assert project_store.get(project.id).status == "deleting"


async def test_job_leaves_child_deleting_when_graph_delete_fails():
    # design.md Risks: the claimed child stays `deleting` so execute and unforced
    # delete-graph keep refusing it until the resume or `force` finishes the job.
    project = project_store.create(name="P")
    child = _child(project.id, graph_id="g-abc123")
    denied = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "no"}}, "DeleteGraph"
    )

    p1, p2 = _patch_job_deps(_neptune(delete_error=denied))
    with p1, p2:
        await delete_project(project.id)

    assert store.get(child.id).status == "deleting"


async def test_job_does_not_delete_other_children_graph_when_one_is_retained():
    project = project_store.create(name="P")
    project_store.update(project.id, status="deleting")
    busy = _child(project.id, status="importing", graph_id="g-busy")
    idle = _child(project.id, graph_id="g-idle")
    neptune = _neptune()

    p1, p2 = _patch_job_deps(neptune)
    with p1, p2:
        await delete_project(project.id)

    neptune.delete_graph.assert_called_once_with(
        graphIdentifier="g-idle", skipSnapshot=True
    )
    assert store.get(busy.id) is not None
    assert store.get(idle.id) is None
    assert project_store.get(project.id) is not None


# --- Startup resume ---


async def test_startup_resume_honours_the_guard():
    project = project_store.create(name="P")
    project_store.update(project.id, status="deleting")
    child = _child(project.id, status="importing", graph_id="g-abc123")
    neptune = _neptune()

    p1, p2 = _patch_job_deps(neptune)
    with p1, p2:
        before = asyncio.all_tasks()
        await resume_pending_deletions()
        spawned = asyncio.all_tasks() - before
        assert spawned, "startup resume should schedule the deletion job"
        await asyncio.gather(*spawned)

    neptune.delete_graph.assert_not_called()
    assert store.get(child.id).status == "importing"
    assert project_store.get(project.id) is not None


# --- Structural: only the guarded purge may remove a projection row ---


def _source_files():
    return [p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts]


def test_delete_from_projections_appears_only_in_the_store():
    pattern = re.compile(r"DELETE\s+FROM\s+projections\b", re.IGNORECASE)
    offenders = [
        str(p.relative_to(SRC))
        for p in _source_files()
        if p.name != "projection_store.py" and pattern.search(p.read_text())
    ]
    assert offenders == []


def test_no_caller_of_an_unguarded_projection_delete_exists():
    pattern = re.compile(r"projection_service\.delete\(|projection_store\.delete")
    offenders = [
        f"{p.relative_to(SRC)}:{n}"
        for p in _source_files()
        for n, line in enumerate(p.read_text().splitlines(), start=1)
        if pattern.search(line)
    ]
    assert offenders == []


def test_no_unguarded_projection_delete_remains_in_service_or_store():
    assert not hasattr(ProjectionService, "delete")
    assert not hasattr(ProjectionStore, "delete")
    assert not hasattr(ProjectionStore, "delete_on")
