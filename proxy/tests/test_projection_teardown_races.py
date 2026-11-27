# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Teardown races the concurrency guards did not originally close.

Four defects, one test group each:

1. The project-deletion job cleared ``graph_id`` unconditionally from its
   pre-claim snapshot, so a graph recorded by a taken-over run was wiped from
   the row and left live and billing with nothing referencing it.
2. ``_delete_orphaned_graph`` waits on AWS with a blocking ``time.sleep`` and
   was called straight from a coroutine, freezing the whole proxy for minutes.
3. The delete-graph background task wrote unconditionally even though its claim
   rotated the owner token, so a stale task could stamp a terminal status over
   a row a second forced delete had taken over.
4. ``_delete_graph`` issued a raw ``delete_graph``, which fails with
   ConflictException on a graph that still has deletion protection on (the
   library default), and then polled instead of re-asking.
"""

import asyncio
import threading
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from nx_neptune_proxy.routers import projection
from nx_neptune_proxy.services import pipeline, project_deletion
from nx_neptune_proxy.services.project_deletion import delete_project
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.projection_store import store


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


def _error(code, op="DeleteGraph"):
    return ClientError({"Error": {"Code": code, "Message": code}}, op)


async def _noop_sleep(_seconds):
    """Skip the delete-graph poll interval."""
    return None


# --- 1. The pinned clear ---


def test_clear_graph_if_clears_only_the_expected_graph():
    p = _child(project_store.create(name="P").id, graph_id="g-A")

    assert projection_service.clear_graph_if(p.id, "g-A") is True
    assert store.get(p.id).graph_id is None


def test_clear_graph_if_refuses_when_the_reference_moved():
    p = _child(project_store.create(name="P").id, graph_id="g-B")

    # The caller deleted g-A, but the row now names a different, live graph.
    assert projection_service.clear_graph_if(p.id, "g-A") is False
    assert store.get(p.id).graph_id == "g-B"


@pytest.mark.asyncio
async def test_graph_recorded_mid_delete_is_deleted_not_orphaned():
    """A taken-over run's graph must not be wiped from the row and left live."""
    project = project_store.create(name="P")
    child = _child(project.id, status="importing", graph_id="g-A")
    deleted = []

    async def fake_delete(graph_id):
        deleted.append(graph_id)
        if graph_id == "g-A":
            # What `pipeline._record_graph` does on the takeover path: record
            # this run's own graph on the row the deletion job just claimed.
            store.update(child.id, graph_id="g-B")

    with patch.object(project_deletion, "_delete_graph", fake_delete):
        await delete_project(project.id, force=True)

    # g-B is picked up by the re-read, rather than silently orphaned.
    assert deleted == ["g-A", "g-B"]
    assert store.get(child.id) is None


# --- 2. The orphan delete must not occupy the event loop ---


@pytest.mark.asyncio
async def test_orphan_delete_runs_off_the_event_loop():
    loop_thread = threading.get_ident()
    worker_thread = {}
    started, release = threading.Event(), threading.Event()

    def fake(graph_id):
        worker_thread["id"] = threading.get_ident()
        started.set()
        release.wait(5)

    with patch.object(pipeline, "_delete_orphaned_graph", fake):
        pipeline._delete_orphaned_graph_off_loop("g-1")
        assert started.wait(5), "the orphan delete never started"
        # The worker is parked inside the delete; the loop must still turn.
        await asyncio.sleep(0)
        release.set()

    assert worker_thread["id"] != loop_thread


def test_orphan_delete_runs_inline_without_a_loop():
    """No event loop to protect (a sync caller, or a test): just do the work."""
    calls = []

    with patch.object(pipeline, "_delete_orphaned_graph", calls.append):
        pipeline._delete_orphaned_graph_off_loop("g-1")

    assert calls == ["g-1"]


# --- 3. The delete-graph task writes only while it owns the row ---


class _CapturedTasks:
    """Stands in for BackgroundTasks so the closure can be run on demand."""

    def __init__(self):
        self.tasks = []

    def add_task(self, fn, *args, **kwargs):
        self.tasks.append((fn, args, kwargs))


@pytest.mark.asyncio
async def test_delete_graph_task_does_not_stamp_a_taken_over_row():
    """The first task's `archived` write must not land on a second owner's row."""
    p = _child(
        project_store.create(name="P").id,
        status="complete",
        graph_id="nx-neptune-g-A",
        graph_endpoint="https://g-A.neptune-graph.amazonaws.com",
    )

    neptune = MagicMock()
    neptune.get_graph.return_value = {"name": "nx-neptune-g-A"}
    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune

    tasks = _CapturedTasks()
    with (
        patch(f"{projection.__name__}.ClientFactory", factory),
        patch(f"{projection.__name__}.assert_managed_graph", lambda *_: None),
    ):
        projection.delete_projection_graph(p.id, tasks, force=False)

        # A second forced delete-graph takes the row over before the first
        # task finishes, rotating the owner token out from under it.
        assert projection_service.try_transition(
            p.id, "deleting", refuse_when=frozenset(), owner_token="token-second"
        )

        # Graph confirmed gone, so the first task tries to archive the row.
        neptune.get_graph.side_effect = _error("ResourceNotFoundException", "GetGraph")
        with patch.object(projection.asyncio, "sleep", _noop_sleep):
            fn, args, kwargs = tasks.tasks[0]
            await fn(*args, **kwargs)

    row = store.get(p.id)
    assert row.status == "deleting", "a stale task archived the new owner's row"
    assert row.graph_id == "nx-neptune-g-A", "a stale task cleared the reference"


# --- 4. Deletion protection is cleared, and a conflict is re-asked ---


def test_delete_clears_deletion_protection_before_deleting():
    neptune = MagicMock()

    assert project_deletion._issue_delete(neptune, "g-A") is False

    neptune.update_graph.assert_called_once_with(
        graphIdentifier="g-A", deletionProtection=False
    )
    neptune.delete_graph.assert_called_once_with(
        graphIdentifier="g-A", skipSnapshot=True
    )


def test_issue_delete_reports_an_already_gone_graph():
    neptune = MagicMock()
    neptune.delete_graph.side_effect = _error("ResourceNotFoundException")

    assert project_deletion._issue_delete(neptune, "g-A") is True


def test_issue_delete_swallows_conflict_but_reraises_other_errors():
    neptune = MagicMock()
    neptune.delete_graph.side_effect = _error("ConflictException")
    assert project_deletion._issue_delete(neptune, "g-A") is False

    neptune.delete_graph.side_effect = _error("AccessDeniedException")
    with pytest.raises(ClientError):
        project_deletion._issue_delete(neptune, "g-A")


@pytest.mark.asyncio
async def test_conflicted_delete_is_reissued_rather_than_only_polled():
    """A CREATING graph refuses the first delete; polling alone never kills it."""
    neptune = MagicMock()
    # First ask refused, second accepted.
    neptune.delete_graph.side_effect = [_error("ConflictException"), None]
    # Still there on the first poll, gone on the second.
    neptune.get_graph.side_effect = [None, _error("ResourceNotFoundException")]

    factory = MagicMock()
    factory.return_value.neptune.return_value = neptune

    with (
        patch.object(project_deletion, "ClientFactory", factory),
        patch.object(project_deletion, "POLL_INTERVAL", 0),
    ):
        await project_deletion._delete_graph("g-A")

    assert neptune.delete_graph.call_count == 2
