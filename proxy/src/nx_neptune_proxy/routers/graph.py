# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Router for graph instance actions (start, stop, delete)."""

from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException
from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune_proxy.utils.aws_helper import assert_managed_graph, get_graph_or_exception
from nx_neptune_proxy.services.graph_state_machine import (
    InvalidTransitionError,
    available_actions,
    execute_transition,
    get_inflight,
    clear_inflight,
)
import nx_neptune_proxy.services.graph_ops  # noqa: F401 — registers transitions

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v0/graphs", tags=["graphs"])


@router.get("/{graph_id}/actions", summary="List available actions for a graph")
def get_available_actions(graph_id: str):
    """Return valid actions based on the graph's current Neptune status."""
    client = ClientFactory().neptune()
    resp = client.get_graph(graphIdentifier=graph_id)
    status = resp.get("status")
    if not status:
        logger.warning(f"Graph {graph_id} returned empty status: {resp}")
        raise HTTPException(status_code=502, detail="Graph returned no status")
    actions = available_actions(status)
    inflight = get_inflight(graph_id)
    return {
        "graph_id": graph_id,
        "status": status,
        "actions": actions,
        "inflight": inflight,
    }


@router.post("/{graph_id}/{action}", summary="Execute a graph action", status_code=202)
def perform_action(graph_id: str, action: str, background_tasks: BackgroundTasks):
    """Initiate a state transition (stop, start, delete) as a background task."""
    client = ClientFactory().neptune()
    resp = get_graph_or_exception(client, graph_id)
    current_status = resp.get("status")
    if not current_status:
        logger.warning(f"Graph {graph_id} returned empty status: {resp}")
        raise HTTPException(status_code=502, detail="Graph returned no status")

    # Prefix guard: only allow destructive actions on graphs managed by this tool
    if action == "delete":
        assert_managed_graph(resp.get("name"))

    try:
        # Validate early (execute_transition also validates, but fail fast for the user)
        valid = available_actions(current_status)
        if action not in valid:
            raise InvalidTransitionError(
                f"Cannot {action} graph in {current_status} state. "
                f"Valid actions: {valid or 'none'}"
            )
    except InvalidTransitionError as e:
        raise HTTPException(status_code=409, detail=str(e))

    background_tasks.add_task(execute_transition, graph_id, action, current_status)
    return {"graph_id": graph_id, "action": action, "status": "accepted"}


@router.get("/{graph_id}/inflight", summary="Check in-flight operation status")
def get_inflight_status(graph_id: str):
    """Return current in-flight operation info, including any error."""
    inflight = get_inflight(graph_id)
    if not inflight:
        return {"graph_id": graph_id, "inflight": None}
    return {"graph_id": graph_id, "inflight": inflight}


@router.delete("/{graph_id}/inflight", summary="Dismiss inflight error")
def dismiss_inflight(graph_id: str):
    """Clear the in-flight error for a graph (user dismissed the alert)."""
    clear_inflight(graph_id)
    return {"graph_id": graph_id, "cleared": True}
