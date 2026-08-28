# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Regression tests for the graph_name prefix-collision reset vulnerability.

Previously an unvalidated / short / empty ``graph_name`` collapsed to just the
managed prefix and was matched against existing graphs with ``startswith``, so
the pipeline could adopt and reset an unrelated graph. The fix:

1. Constrains ``graph_name`` in the request schema (min length + pattern), so it
   can no longer be empty or a 1-2 char prefix-colliding value.
2. Reworks the pipeline so it only reuses a graph recorded for the projection by
   *exact* ``graph_id`` (and only after confirming it is managed by this tool);
   a first run always creates a brand-new graph rather than adopting one by name.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nx_neptune_proxy.services.pipeline import run_pipeline
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store
from nx_neptune_proxy.services.project_store import store as project_store

# --- Schema validation (defense-in-depth) ---


class TestGraphNameSchemaValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_name", ["", "a", "ab", "-abc", "has space", "bad!char"]
    )
    async def test_rejects_invalid_graph_name(self, client, test_project_id, bad_name):
        """Empty, too-short, or illegal graph_name is rejected with 422."""
        resp = await client.post(
            "/api/v0/projection",
            json={
                "database": "mydb",
                "graph_name": bad_name,
                "project_id": test_project_id,
            },
        )
        assert resp.status_code == 422

    @pytest.mark.asyncio
    @pytest.mark.parametrize("ok_name", ["abc", "Test-Graph", "my_graph_1", "ABC123"])
    async def test_accepts_valid_graph_name(self, client, test_project_id, ok_name):
        """Valid names (incl. uppercase and underscore) are accepted."""
        resp = await client.post(
            "/api/v0/projection",
            json={
                "database": "mydb",
                "graph_name": ok_name,
                "project_id": test_project_id,
            },
        )
        assert resp.status_code == 201
        assert resp.json()["graph_name"] == ok_name


# --- Pipeline adoption logic ---


def _make_projection(**overrides):
    defaults = dict(
        database="mydb",
        node_query="SELECT * FROM t",
        graph_name="my-graph",
        s3_staging_bucket="s3://bucket/staging/",
    )
    defaults.update(overrides)
    if "project_id" not in defaults:
        defaults["project_id"] = project_store.create(name="Test Project").id
    return store.create(**defaults)


@pytest.mark.asyncio
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_first_run_never_adopts_existing_graph(mock_sm_cls, mock_cf, clear_store):
    """With no recorded graph_id, the pipeline must create a new graph and must
    NOT list/adopt an existing graph by prefix."""
    projection = _make_projection()
    assert projection.graph_id is None

    sm = mock_sm_cls.return_value
    created = MagicMock(graph_id="g-new", name="nxp-my-graph")
    sm.get_or_create_graph = AsyncMock(return_value=created)
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock(return_value="task-1")

    await run_pipeline(projection)

    # Brand-new graph created; no reset of any pre-existing graph.
    sm.get_or_create_graph.assert_awaited_once()
    sm.reset_graph.assert_not_called()
    # The neptune client is only consulted when a graph_id is recorded.
    mock_cf.return_value.neptune.assert_not_called()


@pytest.mark.asyncio
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_retry_resets_only_the_recorded_graph_by_id(
    mock_sm_cls, mock_cf, clear_store
):
    """With a recorded graph_id pointing at a managed AVAILABLE graph, the
    pipeline reuses that exact graph and resets it."""
    projection = _make_projection()
    store.update(projection.id, graph_id="g-mine")
    projection = store.get(projection.id)

    neptune = mock_cf.return_value.neptune.return_value
    neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-my-graph"}

    sm = mock_sm_cls.return_value
    recorded = MagicMock(graph_id="g-mine")
    recorded.name = (
        "nxp-my-graph"  # .name must be set explicitly (reserved MagicMock kwarg)
    )
    sm.get_graph.return_value = recorded
    sm.get_or_create_graph = AsyncMock()
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock(return_value="task-1")

    await run_pipeline(projection)

    # Looked up by EXACT id, reset only that graph, did not create a new one.
    neptune.get_graph.assert_called_once_with(graphIdentifier="g-mine")
    sm.get_graph.assert_called_once_with("g-mine")
    sm.reset_graph.assert_awaited_once_with("nxp-my-graph")
    sm.get_or_create_graph.assert_not_called()


@pytest.mark.asyncio
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_recorded_graph_not_managed_fails_without_reset(
    mock_sm_cls, mock_cf, clear_store
):
    """If the recorded graph does not carry the managed prefix, the pipeline
    refuses to reset it and marks the projection failed."""
    projection = _make_projection()
    store.update(projection.id, graph_id="g-foreign")
    projection = store.get(projection.id)

    neptune = mock_cf.return_value.neptune.return_value
    # AVAILABLE, but name lacks the managed prefix (nxp-).
    neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "prod-analytics"}

    sm = mock_sm_cls.return_value
    sm.reset_graph = AsyncMock()
    sm.get_or_create_graph = AsyncMock()

    await run_pipeline(projection)

    sm.reset_graph.assert_not_called()
    sm.get_or_create_graph.assert_not_called()
    assert store.get(projection.id).status == "failed"
