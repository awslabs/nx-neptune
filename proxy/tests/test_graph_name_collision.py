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

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from nx_neptune_proxy.services.pipeline import run_pipeline
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store

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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name, status", [("abc", 201), ("ab", 400), ("-abc", 400), ("bad!char", 400)]
    )
    async def test_import_validates_graph_name(self, client, name, status):
        """Imported project files get the same graph_name rules."""
        payload = {
            "version": "1.0",
            "project": {"name": "P"},
            "projections": [{"database": "mydb", "graph_name": name}],
        }
        resp = await client.post("/api/v0/project/import", content=json.dumps(payload))
        assert resp.status_code == status


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


def _created_graph(graph_id, name=None):
    """Mock create_graph that reports the id through on_created, like the real one."""
    graph = MagicMock(graph_id=graph_id)
    graph.name = name

    async def create(config=None, on_created=None):
        on_created(graph_id)
        return graph

    return AsyncMock(side_effect=create)


@pytest.mark.asyncio
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_first_run_never_adopts_existing_graph(mock_sm_cls, mock_cf, clear_store):
    """With no recorded graph_id, the pipeline must create a new graph and must
    NOT list/adopt an existing graph by prefix."""
    projection = _make_projection()
    assert projection.graph_id is None

    sm = mock_sm_cls.return_value
    sm.create_graph = _created_graph("g-new", "nxp-my-graph")
    sm.get_or_create_graph = AsyncMock()
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock(return_value="task-1")

    await run_pipeline(projection)

    # Brand-new graph created; no reset of any pre-existing graph.
    sm.create_graph.assert_awaited_once()
    sm.get_or_create_graph.assert_not_called()
    sm.reset_graph.assert_not_called()
    # The neptune client is only consulted when a graph_id is recorded.
    mock_cf.return_value.neptune.assert_not_called()
    assert store.get(projection.id).graph_id == "g-new"


@pytest.mark.asyncio
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_first_run_records_graph_id_when_wait_fails(
    mock_sm_cls, mock_cf, clear_store
):
    """If creation is accepted but the wait for AVAILABLE fails, the graph id is
    still recorded, so a retry or project deletion can find the graph."""
    from botocore.exceptions import ClientError

    projection = _make_projection()

    async def create_then_fail(config=None, on_created=None):
        on_created("g-new")
        raise ClientError(
            {"Error": {"Code": "MaxAttemptsReached", "Message": "timeout"}},
            "wait_until_complete",
        )

    sm = mock_sm_cls.return_value
    sm.create_graph = AsyncMock(side_effect=create_then_fail)
    sm.import_from_table = AsyncMock()

    await run_pipeline(projection)

    result = store.get(projection.id)
    assert result.status == "failed"
    assert result.graph_id == "g-new"
    assert result.graph_endpoint == "https://g-new.neptune-graph.amazonaws.com"
    sm.import_from_table.assert_not_called()


@pytest.mark.asyncio
@patch("nx_neptune.session_manager.SessionManager.import_from_table")
@patch("nx_neptune.session_manager.instance_management.create_na_instance")
@patch("boto3.client")
async def test_first_run_does_not_reuse_sibling_projection_graph(
    mock_boto3_client, mock_create, mock_import, clear_store
):
    """A projection named 'sales' must not reuse the graph of a projection
    named 'sales2', even though 'nxp-sales2-...' starts with 'nxp-sales'."""
    sibling = {
        "name": "nxp-sales2-1b4e28ba-2fa1-41d2-883f-0016d3cca427",
        "id": "g-sales2",
        "status": "AVAILABLE",
    }
    own = {
        "name": "nxp-sales-6fa459ea-ee8a-4ca4-894e-db77e160355e",
        "id": "g-sales",
        "status": "AVAILABLE",
    }
    mock_client = MagicMock()
    mock_boto3_client.return_value = mock_client
    mock_client.get_caller_identity.return_value = {"Arn": "test-arn"}
    mock_client.list_graphs.return_value = {"graphs": [sibling, own]}

    def create(**kwargs):
        kwargs["on_created"]("g-sales")
        return "g-sales"

    mock_create.side_effect = create
    mock_import.return_value = "task-1"

    projection = _make_projection(graph_name="sales")
    await run_pipeline(projection)

    mock_create.assert_awaited_once()
    result = store.get(projection.id)
    assert result.status == "complete"
    assert result.graph_id == "g-sales"


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
    sm.create_graph = AsyncMock()
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock(return_value="task-1")

    await run_pipeline(projection)

    # Looked up by EXACT id, reset only that graph, did not create a new one.
    neptune.get_graph.assert_called_once_with(graphIdentifier="g-mine")
    sm.get_graph.assert_called_once_with("g-mine")
    sm.reset_graph.assert_awaited_once_with("nxp-my-graph")
    sm.get_or_create_graph.assert_not_called()
    sm.create_graph.assert_not_called()


def _client_error(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "GetGraph")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "get_graph",
    [
        {"side_effect": _client_error("ResourceNotFoundException")},
        {"return_value": {"status": "DELETING", "name": "nxp-my-graph"}},
    ],
    ids=["not_found", "deleting"],
)
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_stale_recorded_graph_creates_new(
    mock_sm_cls, mock_cf, clear_store, get_graph
):
    """If the recorded graph is gone or being deleted, the pipeline creates a
    new graph and does not reset or adopt any existing one."""
    projection = _make_projection()
    store.update(projection.id, graph_id="g-stale")
    projection = store.get(projection.id)

    neptune = mock_cf.return_value.neptune.return_value
    neptune.get_graph.configure_mock(**get_graph)

    sm = mock_sm_cls.return_value
    sm.create_graph = _created_graph("g-new")
    sm.get_or_create_graph = AsyncMock()
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock(return_value="task-1")

    await run_pipeline(projection)

    neptune.get_graph.assert_called_once_with(graphIdentifier="g-stale")
    sm.create_graph.assert_awaited_once()
    sm.get_or_create_graph.assert_not_called()
    sm.reset_graph.assert_not_called()
    assert store.get(projection.id).graph_id == "g-new"


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["ThrottlingException", "AccessDeniedException"])
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_recorded_graph_lookup_error_fails_and_keeps_id(
    mock_sm_cls, mock_cf, clear_store, code
):
    """If looking up the recorded graph fails for any reason other than not
    found, the run fails and keeps the recorded id instead of creating a new graph."""
    projection = _make_projection()
    store.update(projection.id, graph_id="g-mine")
    projection = store.get(projection.id)

    neptune = mock_cf.return_value.neptune.return_value
    neptune.get_graph.side_effect = _client_error(code)

    sm = mock_sm_cls.return_value
    sm.create_graph = AsyncMock()
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock()

    await run_pipeline(projection)

    sm.create_graph.assert_not_called()
    sm.reset_graph.assert_not_called()
    sm.import_from_table.assert_not_called()
    result = store.get(projection.id)
    assert result.status == "failed"
    assert result.graph_id == "g-mine"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["CREATING", "STOPPED", "FAILED", "RESETTING"])
@patch("nx_neptune.clients.client_factory.ClientFactory")
@patch("nx_neptune.session_manager.SessionManager")
async def test_recorded_graph_not_available_fails_and_keeps_id(
    mock_sm_cls, mock_cf, clear_store, status
):
    """If the recorded graph still exists but is not AVAILABLE, the run fails
    and keeps the recorded id instead of creating a second graph."""
    projection = _make_projection()
    store.update(projection.id, graph_id="g-mine")
    projection = store.get(projection.id)

    neptune = mock_cf.return_value.neptune.return_value
    neptune.get_graph.return_value = {"status": status, "name": "nxp-my-graph"}

    sm = mock_sm_cls.return_value
    sm.create_graph = AsyncMock()
    sm.reset_graph = AsyncMock()
    sm.import_from_table = AsyncMock()

    await run_pipeline(projection)

    sm.create_graph.assert_not_called()
    sm.reset_graph.assert_not_called()
    sm.import_from_table.assert_not_called()
    result = store.get(projection.id)
    assert result.status == "failed"
    assert result.graph_id == "g-mine"
    assert status in result.error


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
    sm.create_graph = AsyncMock()

    await run_pipeline(projection)

    sm.reset_graph.assert_not_called()
    sm.get_or_create_graph.assert_not_called()
    sm.create_graph.assert_not_called()
    assert store.get(projection.id).status == "failed"
