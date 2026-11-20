# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A run whose owning task died must not wedge the row, or orphan its graph.

An ``importing``/``executing`` row is owned by a background task in this
process. Two ways that ownership can be lost mid-run:

- the process exits, so nothing is left to advance the row. Execute refuses an
  in-flight row and the UI sends no ``force``, so the projection could never be
  run again.
- the row is purged or re-claimed under the running task. Its later
  unconditional writes then landed on nothing, leaving a created graph live,
  untracked and billing with no reference anywhere to delete it by.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from nx_neptune_proxy.app import fail_orphaned_projection_runs
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.pipeline import run_pipeline
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import (
    STATUS_DELETING,
    STATUS_EXECUTING,
    STATUS_IMPORTING,
    store,
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


def _projection(status=None, **overrides):
    """A projection row, optionally claimed into ``status``.

    ``create`` takes only the user-supplied columns, so anything the pipeline
    would have written (``graph_id``) goes on afterwards.
    """
    p = store.create(
        database="mydb",
        node_query="SELECT 1",
        graph_name="my-graph",
        s3_staging_bucket="s3://bucket/staging/",
        project_id=project_store.create(name="P").id,
    )
    if overrides:
        store.update(p.id, **overrides)
    if status:
        store.try_transition(p.id, status, refuse_when=frozenset())
    return store.get(p.id)


# --- Startup: release rows left mid-run by a restart ---


class TestFailOrphanedRuns:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", [STATUS_IMPORTING, STATUS_EXECUTING])
    async def test_an_orphaned_run_is_released_as_failed(self, status):
        p = _projection(status=status)

        await fail_orphaned_projection_runs()

        row = store.get(p.id)
        assert row.status == "failed"
        assert "restart" in row.error

    @pytest.mark.asyncio
    async def test_the_graph_reference_is_kept(self):
        """The graph is still live and billing; the reference is how it gets deleted."""
        p = _projection(status=STATUS_IMPORTING, graph_id="g-live")

        await fail_orphaned_projection_runs()

        assert store.get(p.id).graph_id == "g-live"

    @pytest.mark.asyncio
    async def test_a_deleting_row_is_left_alone(self):
        """A forced delete-graph can already take that one over."""
        p = _projection(status=STATUS_DELETING, graph_id="g-x")

        await fail_orphaned_projection_runs()

        assert store.get(p.id).status == STATUS_DELETING

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["draft", "complete", "failed", "archived"])
    async def test_rows_not_mid_run_are_left_alone(self, status):
        p = _projection(status=status)

        await fail_orphaned_projection_runs()

        assert store.get(p.id).status == status

    @pytest.mark.asyncio
    async def test_a_released_row_can_be_executed_again(self, client):
        """The point of the fix: the projection is runnable, not wedged."""
        p = _projection(status=STATUS_IMPORTING)

        await fail_orphaned_projection_runs()

        with patch("nx_neptune_proxy.routers.projection.run_pipeline", AsyncMock()):
            resp = await client.post(f"/api/v0/projection/{p.id}/execute")

        assert resp.status_code == 202


# --- Mid-run loss of the row ---


def _sm_creating(graph_id):
    """A SessionManager double that reports a new graph through on_created."""
    sm = MagicMock()
    graph = MagicMock(graph_id=graph_id)
    graph.name = "nxp-my-graph"

    async def create(config=None, on_created=None):
        on_created(graph_id)
        return graph

    sm.create_graph = AsyncMock(side_effect=create)
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock(return_value="task-1")
    return sm


class TestRowLostMidRun:
    @pytest.mark.asyncio
    @patch("nx_neptune.clients.client_factory.ClientFactory")
    @patch("nx_neptune.session_manager.SessionManager")
    async def test_a_purged_row_gets_its_graph_deleted(self, mock_sm_cls, mock_cf):
        """Nothing can reference the graph any more, so the run deletes it itself."""
        p = _projection(status=STATUS_IMPORTING)
        mock_sm_cls.return_value = _sm_creating("g-orphan")
        neptune = MagicMock()
        mock_cf.return_value.neptune.return_value = neptune

        # The row goes while the pipeline is between create and record.
        with connection() as conn:
            conn.execute("DELETE FROM projections WHERE id = ?", [p.id])

        await run_pipeline(p, p.owner_token)

        neptune.delete_graph.assert_called_once_with(
            graphIdentifier="g-orphan", skipSnapshot=True
        )

    @pytest.mark.asyncio
    @patch("nx_neptune.clients.client_factory.ClientFactory")
    @patch("nx_neptune.session_manager.SessionManager")
    async def test_a_taken_over_row_keeps_the_graph_and_records_it(
        self, mock_sm_cls, mock_cf
    ):
        """The new owner decides the graph's fate, so it needs the reference."""
        p = _projection(status=STATUS_IMPORTING)
        mock_sm_cls.return_value = _sm_creating("g-taken")
        neptune = MagicMock()
        mock_cf.return_value.neptune.return_value = neptune

        store.try_transition(p.id, STATUS_DELETING, refuse_when=frozenset())

        await run_pipeline(p, p.owner_token)

        neptune.delete_graph.assert_not_called()
        row = store.get(p.id)
        assert row.graph_id == "g-taken"
        assert row.status == STATUS_DELETING, "the takeover's status must survive"

    @pytest.mark.asyncio
    @patch("nx_neptune.clients.client_factory.ClientFactory")
    @patch("nx_neptune.session_manager.SessionManager")
    async def test_an_undeletable_orphan_is_logged_not_swallowed(
        self, mock_sm_cls, mock_cf, caplog
    ):
        p = _projection(status=STATUS_IMPORTING)
        mock_sm_cls.return_value = _sm_creating("g-stuck")
        neptune = MagicMock()
        neptune.delete_graph.side_effect = ClientError(
            {"Error": {"Code": "AccessDeniedException", "Message": "no"}},
            "DeleteGraph",
        )
        mock_cf.return_value.neptune.return_value = neptune

        with connection() as conn:
            conn.execute("DELETE FROM projections WHERE id = ?", [p.id])

        await run_pipeline(p, p.owner_token)

        assert any("manually" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    @patch("nx_neptune.clients.client_factory.ClientFactory")
    @patch("nx_neptune.session_manager.SessionManager")
    async def test_a_completed_run_does_not_overwrite_a_takeover(
        self, mock_sm_cls, mock_cf
    ):
        """`complete` landing on a re-claimed row would hide the new operation."""
        p = _projection(status=STATUS_IMPORTING, graph_id="g-known")
        sm = _sm_creating("g-known")
        mock_sm_cls.return_value = sm
        neptune = MagicMock()
        neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-my-graph"}
        mock_cf.return_value.neptune.return_value = neptune
        sm.get_graph = MagicMock(return_value=MagicMock(graph_id="g-known"))

        store.try_transition(p.id, STATUS_DELETING, refuse_when=frozenset())

        await run_pipeline(p, p.owner_token)

        assert store.get(p.id).status == STATUS_DELETING


# --- Ownership is a token, not a status ---


class TestOwnerToken:
    """Status cannot distinguish "still my run" from "a different run, same status"."""

    def test_a_claim_rotates_the_token(self):
        p = _projection(status=STATUS_IMPORTING)
        first = store.get(p.id).owner_token
        assert first

        store.try_transition(p.id, STATUS_DELETING, refuse_when=frozenset())

        assert store.get(p.id).owner_token != first

    def test_a_write_from_the_previous_owner_is_rejected(self):
        p = _projection(status=STATUS_IMPORTING)
        stale = store.get(p.id).owner_token

        store.try_transition(p.id, STATUS_DELETING, refuse_when=frozenset())

        assert store.update_if_owner(p.id, stale, status="complete") is False
        assert store.get(p.id).status == STATUS_DELETING

    def test_a_stale_run_cannot_overwrite_a_later_run_in_the_same_status(self):
        """The case a status match would have missed.

        Run A is importing, is taken over, and the row is re-claimed as
        `importing` by run B. A status-matched write from A would land on B.
        """
        p = _projection(status=STATUS_IMPORTING)
        token_a = store.get(p.id).owner_token
        store.try_transition(p.id, STATUS_DELETING, refuse_when=frozenset())
        store.try_transition(p.id, STATUS_IMPORTING, refuse_when=frozenset())
        token_b = store.get(p.id).owner_token

        assert store.update_if_owner(p.id, token_a, status="failed") is False
        assert store.update_if_owner(p.id, token_b, status="failed") is True

    def test_progress_writes_from_a_taken_over_run_do_not_land(self):
        """A `deleting` row must not display "Graph ready", 100%."""
        p = _projection(status=STATUS_IMPORTING)
        stale = store.get(p.id).owner_token
        store.try_transition(
            p.id, STATUS_DELETING, refuse_when=frozenset(), step="graph_delete"
        )

        from nx_neptune_proxy.services.pipeline import _update

        _update(p.id, stale, step="ready", label="Graph ready", progress=100)

        row = store.get(p.id)
        assert (row.step, row.progress) == ("graph_delete", 0)


class TestForcedArchivePinsTheGraphId:
    """A forced archive drops the status guard, so it must pin graph_id instead."""

    def test_a_graph_recorded_since_the_read_is_not_orphaned(self):
        p = _projection(status=STATUS_IMPORTING, graph_id="g-old")

        # The caller read g-old and found it gone; meanwhile the run records g-new.
        store.update(p.id, graph_id="g-new")

        claimed = store.try_transition_if_graph(
            p.id,
            "archived",
            expected_graph_id="g-old",
            refuse_when=frozenset(),
            graph_id=None,
        )

        assert claimed is False, "must not clear a reference it never read"
        assert store.get(p.id).graph_id == "g-new"

    def test_it_still_archives_when_the_graph_is_unchanged(self):
        p = _projection(status=STATUS_IMPORTING, graph_id="g-old")

        claimed = store.try_transition_if_graph(
            p.id,
            "archived",
            expected_graph_id="g-old",
            refuse_when=frozenset(),
            graph_id=None,
        )

        assert claimed is True
        row = store.get(p.id)
        assert (row.status, row.graph_id) == ("archived", None)


class TestOrphanedGraphDelete:
    """Deleting an orphan has to survive deletion protection and CREATING."""

    @staticmethod
    def _neptune(statuses):
        neptune = MagicMock()
        neptune.get_graph.side_effect = [{"status": s} for s in statuses]
        return neptune

    def test_it_clears_deletion_protection_before_deleting(self):
        """Graphs are created protected by default, so a raw delete_graph fails."""
        from nx_neptune_proxy.services import pipeline

        neptune = self._neptune(["AVAILABLE"])
        with (
            patch("nx_neptune.clients.client_factory.ClientFactory") as cf,
            patch(
                "nx_neptune.instance_management._delete_na_instance_task"
            ) as delete_task,
        ):
            cf.return_value.neptune.return_value = neptune
            pipeline._delete_orphaned_graph("g-orphan")

        delete_task.assert_called_once()
        assert delete_task.call_args.args[1] == "g-orphan"
        neptune.delete_graph.assert_not_called()

    def test_it_waits_for_a_creating_graph_to_settle(self):
        """on_created fires when CreateGraph is accepted, so CREATING is the norm."""
        from nx_neptune_proxy.services import pipeline

        neptune = self._neptune(["CREATING", "CREATING", "AVAILABLE"])
        with (
            patch("nx_neptune.clients.client_factory.ClientFactory") as cf,
            patch(
                "nx_neptune.instance_management._delete_na_instance_task"
            ) as delete_task,
            patch.object(pipeline, "_ORPHAN_DELETE_POLL", 0),
        ):
            cf.return_value.neptune.return_value = neptune
            pipeline._delete_orphaned_graph("g-orphan")

        assert neptune.get_graph.call_count == 3
        delete_task.assert_called_once()

    def test_an_already_gone_graph_is_not_deleted_again(self):
        from nx_neptune_proxy.services import pipeline

        neptune = MagicMock()
        neptune.get_graph.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "gone"}},
            "GetGraph",
        )
        with (
            patch("nx_neptune.clients.client_factory.ClientFactory") as cf,
            patch(
                "nx_neptune.instance_management._delete_na_instance_task"
            ) as delete_task,
        ):
            cf.return_value.neptune.return_value = neptune
            pipeline._delete_orphaned_graph("g-orphan")

        delete_task.assert_not_called()

    def test_a_failed_delete_is_logged_with_the_graph_id(self, caplog):
        """The warning is the only thing telling an operator what is still billing."""
        from nx_neptune_proxy.services import pipeline

        neptune = self._neptune(["AVAILABLE"])
        with (
            patch("nx_neptune.clients.client_factory.ClientFactory") as cf,
            patch(
                "nx_neptune.instance_management._delete_na_instance_task",
                side_effect=ClientError(
                    {"Error": {"Code": "ConflictException", "Message": "protected"}},
                    "DeleteGraph",
                ),
            ),
        ):
            cf.return_value.neptune.return_value = neptune
            pipeline._delete_orphaned_graph("g-orphan")

        assert any("g-orphan" in r.getMessage() for r in caplog.records)
        assert any("manually" in r.getMessage() for r in caplog.records)
