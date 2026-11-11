# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project deletion: every failed-deletion class keeps the reference.

J1: the failure classifier must treat a cancelled graph deletion exactly like a
timed-out one. ``asyncio.gather(return_exceptions=True)`` hands a child's
``CancelledError`` back as a result value, and it is a ``BaseException``.

J2: documenting pin of the unforced take-over of a child already ``deleting``.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

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

BASE = "/api/v0/project"


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


# --- J1: failure classes of a child graph deletion ---


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(TimeoutError("graph deletion timed out"), id="timeout"),
        pytest.param(asyncio.CancelledError(), id="cancelled"),
    ],
)
async def test_failed_graph_deletion_keeps_child_reference_and_project(failure):
    project = project_store.create(name="P")
    project_store.update(project.id, status="deleting")
    child = _child(project.id, graph_id="g-abc123")

    with patch.object(
        project_deletion, "_delete_graph", AsyncMock(side_effect=failure)
    ):
        await delete_project(project.id)

    kept = store.get(child.id)
    assert kept is not None
    assert (kept.status, kept.graph_id) == ("deleting", "g-abc123")
    assert project_store.get(project.id).status == "deleting"


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(TimeoutError("graph deletion timed out"), id="timeout"),
        pytest.param(asyncio.CancelledError(), id="cancelled"),
    ],
)
async def test_failed_graph_deletion_does_not_stop_sibling_cleanup(failure):
    project = project_store.create(name="P")
    project_store.update(project.id, status="deleting")
    failing = _child(project.id, graph_id="g-fail")
    healthy = _child(project.id, graph_id="g-ok")

    async def delete_graph(graph_id):
        if graph_id == "g-fail":
            raise failure

    with patch.object(project_deletion, "_delete_graph", delete_graph):
        await delete_project(project.id)

    assert store.get(failing.id).graph_id == "g-fail"
    assert store.get(healthy.id) is None
    assert project_store.get(project.id).status == "deleting"


# --- J2: unforced delete takes over a child that is already `deleting` ---
#
# DOCUMENTING PIN, not a bug fix. `delete_project` claims with
# refuse = frozenset() if force else _RUNNING, so `deleting` is not refused on
# an unforced call. Refusing it would break the startup-resume retry (design
# D14), which re-runs this job for rows left `deleting`. The consequence is
# pinned here against the real job: a second delete_graph is issued on the
# same graph, a non-NotFound answer parks the child, and the project is wedged
# because the endpoint answers 409 "Already deleting" before `force` is read.
# If the claim's refuse-set changes, this test must be revisited on purpose.
# The mocked-job sibling (test_delete_project_tolerates_deleting_child) cannot
# see any of this.


@pytest.mark.asyncio
async def test_unforced_delete_takes_over_deleting_child_and_wedges_project(client):
    project = project_store.create(name="P")
    child = _child(project.id, status="deleting", graph_id="g-dbl")
    busy = ClientError(
        {"Error": {"Code": "ConflictException", "Message": "already deleting"}},
        "DeleteGraph",
    )
    neptune = _neptune(delete_error=busy)

    p1, p2 = _patch_job_deps(neptune)
    with p1, p2:
        resp = await client.delete(f"{BASE}/{project.id}")
        retry = await client.delete(f"{BASE}/{project.id}?force=true")

    assert resp.status_code == 202
    neptune.delete_graph.assert_called_once_with(
        graphIdentifier="g-dbl", skipSnapshot=True
    )
    parked = store.get(child.id)
    assert (parked.status, parked.graph_id) == ("deleting", "g-dbl")
    assert project_store.get(project.id).status == "deleting"
    assert retry.status_code == 409
    assert retry.json()["detail"] == "Already deleting"
