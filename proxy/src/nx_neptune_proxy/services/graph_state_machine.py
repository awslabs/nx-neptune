# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Graph instance state machine — valid transitions, execution, and polling."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)

# Poll every 10s, timeout after 10min
DEFAULT_POLL_INTERVAL = 10.0
DEFAULT_TIMEOUT = 600.0


@dataclass
class Transition:
    """Defines a valid state transition for a graph action."""

    from_states: tuple[str, ...]
    transient_state: str
    target_state: str
    action: Callable[[str], Awaitable[None]]
    poll: Callable[[str], Awaitable[str | None]]


# Registry of action name → Transition
_TRANSITIONS: dict[str, Transition] = {}

# In-flight operations: graph_id → {action, error}
_inflight: dict[str, dict] = {}


def register_transition(action: str, transition: Transition) -> None:
    """Register a named transition in the global registry."""
    _TRANSITIONS[action] = transition


def available_actions(current_status: str) -> list[str]:
    """Return action names valid for the given graph status."""
    return [
        action
        for action, t in _TRANSITIONS.items()
        if current_status in t.from_states
    ]


def get_inflight(graph_id: str) -> dict | None:
    """Return in-flight operation info for a graph, or None."""
    return _inflight.get(graph_id)


def clear_inflight(graph_id: str) -> None:
    """Remove in-flight tracking for a graph."""
    _inflight.pop(graph_id, None)


async def execute_transition(
    graph_id: str,
    action: str,
    current_status: str,
    poll_interval: float = DEFAULT_POLL_INTERVAL,
    timeout: float = DEFAULT_TIMEOUT,
) -> None:
    """
    Execute a state transition: validate → kick off action → poll until done.

    Raises InvalidTransitionError if action isn't valid for current_status.
    Raises TransitionFailed on timeout or unexpected state.
    """
    transition = _TRANSITIONS.get(action)
    if not transition:
        raise InvalidTransitionError(f"Unknown action: {action}")
    if current_status not in transition.from_states:
        raise InvalidTransitionError(
            f"Cannot {action} graph in {current_status} state"
        )

    # Track in-flight
    _inflight[graph_id] = {"action": action, "error": None}

    try:
        # Kick off the AWS operation
        await transition.action(graph_id)

        # Poll until terminal state or timeout
        elapsed = 0.0
        while elapsed < timeout:
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
            status = await transition.poll(graph_id)

            if status == transition.target_state:
                _inflight.pop(graph_id, None)
                logger.info(f"Graph {graph_id} transition {action} complete → {status}")
                return

            if status and status not in (transition.transient_state, *transition.from_states):
                raise TransitionFailed(
                    f"Graph entered unexpected state '{status}' during {action}"
                )

        raise TransitionFailed(
            f"Timeout ({timeout}s) waiting for {action} on graph {graph_id}"
        )

    except Exception as e:
        logger.error(f"Transition {action} failed for {graph_id}: {e}")
        _inflight[graph_id] = {"action": action, "error": str(e)}
        raise


class InvalidTransitionError(Exception):
    """Raised when an action is not valid for the current graph state."""
    pass


class TransitionFailed(Exception):
    """Raised when a transition fails or times out."""
    pass
