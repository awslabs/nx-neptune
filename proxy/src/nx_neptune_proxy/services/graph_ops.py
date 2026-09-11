# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Concrete graph operations — stop, start, delete."""

from __future__ import annotations

import logging

from botocore.exceptions import ClientError
from nx_neptune.clients.client_factory import ClientFactory

from nx_neptune_proxy.services.graph_state_machine import (
    Transition,
    register_transition,
)

logger = logging.getLogger(__name__)


# --- Actions ---


async def _stop_graph(graph_id: str) -> None:
    """Issue stop-graph API call."""
    client = ClientFactory().neptune()
    client.stop_graph(graphIdentifier=graph_id)


async def _start_graph(graph_id: str) -> None:
    """Issue start-graph API call."""
    client = ClientFactory().neptune()
    client.start_graph(graphIdentifier=graph_id)


async def _delete_graph(graph_id: str) -> None:
    """Issue delete-graph API call (skip snapshot)."""
    client = ClientFactory().neptune()
    client.delete_graph(graphIdentifier=graph_id, skipSnapshot=True)


# --- Probe ---


async def _poll_graph_status(graph_id: str) -> str | None:
    """Check the current graph status. Returns None if graph is gone (deleted)."""
    client = ClientFactory().neptune()
    try:
        resp = client.get_graph(graphIdentifier=graph_id)
        return resp.get("status")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return "DELETED"
        raise


# --- Register transitions ---

register_transition(
    "stop",
    Transition(
        from_states=("AVAILABLE",),
        transient_state="STOPPING",
        target_state="STOPPED",
        action=_stop_graph,
        poll=_poll_graph_status,
    ),
)

register_transition(
    "start",
    Transition(
        from_states=("STOPPED",),
        transient_state="STARTING",
        target_state="AVAILABLE",
        action=_start_graph,
        poll=_poll_graph_status,
    ),
)

register_transition(
    "delete",
    Transition(
        from_states=("AVAILABLE", "STOPPED"),
        transient_state="DELETING",
        target_state="DELETED",
        action=_delete_graph,
        poll=_poll_graph_status,
    ),
)
