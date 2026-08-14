# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.services import graph_state_machine


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://localhost", headers={"X-Requested-With": "nx-neptune"})


@pytest.fixture(autouse=True)
def clear_inflight():
    """Ensure inflight state is clean between tests."""
    graph_state_machine._inflight.clear()
    yield
    graph_state_machine._inflight.clear()


# --- GET /{graph_id}/actions ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_actions_available_graph(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/graphs/g-123/actions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["graph_id"] == "g-123"
    assert data["status"] == "AVAILABLE"
    assert "stop" in data["actions"]
    assert "delete" in data["actions"]
    assert "start" not in data["actions"]
    assert data["inflight"] is None


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_actions_stopped_graph(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "STOPPED", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/graphs/g-123/actions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "STOPPED"
    assert "start" in data["actions"]
    assert "delete" in data["actions"]
    assert "stop" not in data["actions"]


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_actions_transient_state(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "STOPPING", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/graphs/g-123/actions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "STOPPING"
    assert data["actions"] == []


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_actions_empty_status_returns_502(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/graphs/g-123/actions")
    assert resp.status_code == 502


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_actions_missing_status_returns_502(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/graphs/g-123/actions")
    assert resp.status_code == 502


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_actions_graph_not_found_returns_404(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Graph not found"}},
        "GetGraph",
    )
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/graphs/g-missing/actions")
    assert resp.status_code == 404


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_actions_with_inflight(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "STOPPING", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    graph_state_machine._inflight["g-123"] = {"action": "stop", "error": None}

    resp = await client.get("/api/v0/graphs/g-123/actions")
    assert resp.status_code == 200
    data = resp.json()
    assert data["inflight"]["action"] == "stop"
    assert data["inflight"]["error"] is None


# --- POST /{graph_id}/{action} ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.execute_transition")
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_perform_stop_accepted(mock_cf, mock_exec, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-123/stop")
    assert resp.status_code == 202
    data = resp.json()
    assert data["graph_id"] == "g-123"
    assert data["action"] == "stop"
    assert data["status"] == "accepted"


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.execute_transition")
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_perform_start_accepted(mock_cf, mock_exec, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "STOPPED", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-123/start")
    assert resp.status_code == 202
    data = resp.json()
    assert data["action"] == "start"


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.execute_transition")
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_perform_delete_accepted(mock_cf, mock_exec, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-test-graph"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-123/delete")
    assert resp.status_code == 202
    data = resp.json()
    assert data["action"] == "delete"


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_perform_invalid_transition_returns_409(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "STOPPED", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-123/stop")
    assert resp.status_code == 409
    assert "Cannot stop" in resp.json()["detail"]


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_perform_unknown_action_returns_409(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-123/restart")
    assert resp.status_code == 409


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_perform_empty_status_returns_502(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-123/stop")
    assert resp.status_code == 502


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_perform_graph_not_found_returns_404(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "Graph not found"}},
        "GetGraph",
    )
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-missing/stop")
    assert resp.status_code == 404


# --- GET /{graph_id}/inflight ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_inflight_no_operation(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.get("/api/v0/graphs/g-123/inflight")
    assert resp.status_code == 200
    data = resp.json()
    assert data["graph_id"] == "g-123"
    assert data["inflight"] is None


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_inflight_with_operation(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "STOPPING", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    graph_state_machine._inflight["g-123"] = {"action": "stop", "error": None}

    resp = await client.get("/api/v0/graphs/g-123/inflight")
    assert resp.status_code == 200
    data = resp.json()
    assert data["inflight"]["action"] == "stop"
    assert data["inflight"]["error"] is None


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_get_inflight_with_error(mock_cf, client):
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-test"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    graph_state_machine._inflight["g-123"] = {"action": "stop", "error": "Timeout"}

    resp = await client.get("/api/v0/graphs/g-123/inflight")
    assert resp.status_code == 200
    data = resp.json()
    assert data["inflight"]["action"] == "stop"
    assert data["inflight"]["error"] == "Timeout"


# --- DELETE /{graph_id}/inflight ---


@pytest.mark.asyncio
async def test_dismiss_inflight_clears_error(client):
    graph_state_machine._inflight["g-123"] = {"action": "stop", "error": "Timeout"}

    resp = await client.request("DELETE", "/api/v0/graphs/g-123/inflight")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cleared"] is True

    # Verify it's actually cleared
    assert graph_state_machine._inflight.get("g-123") is None


@pytest.mark.asyncio
async def test_dismiss_inflight_noop_when_nothing(client):
    resp = await client.request("DELETE", "/api/v0/graphs/g-123/inflight")
    assert resp.status_code == 200
    assert resp.json()["cleared"] is True


# --- Prefix guard on delete ---


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_delete_rejects_unmanaged_graph(mock_cf, client):
    """Delete action should return 403 for graphs without the nxp- prefix."""
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "foreign-graph"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-456/delete")
    assert resp.status_code == 403
    assert "not managed" in resp.json()["detail"]


@pytest.mark.asyncio
@patch("nx_neptune_proxy.routers.graph.execute_transition")
@patch("nx_neptune_proxy.routers.graph.ClientFactory")
async def test_delete_allows_managed_graph(mock_cf, mock_exec, client):
    """Delete action should succeed for graphs with the nxp- prefix."""
    mock_neptune = MagicMock()
    mock_neptune.get_graph.return_value = {"status": "AVAILABLE", "name": "nxp-my-graph"}
    mock_cf.return_value.neptune.return_value = mock_neptune

    resp = await client.post("/api/v0/graphs/g-123/delete")
    assert resp.status_code == 202
