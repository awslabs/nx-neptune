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

        await run_pipeline(p)

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

        await run_pipeline(p)

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

        await run_pipeline(p)

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

        await run_pipeline(p)

        assert store.get(p.id).status == STATUS_DELETING
