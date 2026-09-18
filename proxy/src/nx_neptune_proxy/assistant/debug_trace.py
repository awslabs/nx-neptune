# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Debug tracing for the assistant (opt-in via log level).

Everything here is a no-op unless the ``nx_neptune_proxy`` logger is at
``DEBUG`` (i.e. ``LOG_LEVEL=DEBUG``). At DEBUG you get, at the finest level:

- which sub-agent / supervisor was invoked, with its formatted prompt;
- which tools the model called during that run, with their inputs (live via a
  callback handler) and a post-run summary from the result metrics;
- the raw model result for the run;
- the full conversation history for a supervisor turn.

Design: tracing is gated behind :func:`debug_enabled` so production/normal runs
pay nothing (no string building, no handler) — the callback handler is only
attached when DEBUG is on. Untrusted values (prompts, tool inputs, model text)
are logged as data via ``%`` args, never used as format strings.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger("nx_neptune_proxy")

# Truncate very large payloads so a single turn can't flood the log.
_MAX_CHARS = 4000


def debug_enabled() -> bool:
    """True when the assistant logger is emitting DEBUG."""
    return logger.isEnabledFor(logging.DEBUG)


def _clip(text: str) -> str:
    if len(text) > _MAX_CHARS:
        return text[:_MAX_CHARS] + f"... [+{len(text) - _MAX_CHARS} chars]"
    return text


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return repr(value)


class DebugCallbackHandler:
    """Strands callback handler that logs tool invocations at DEBUG.

    Reads the same ``event -> contentBlockStart -> start -> toolUse`` block the
    built-in ``PrintingCallbackHandler`` uses, so it tracks each tool the model
    calls (name + input) — but routes it to the logger instead of stdout, tagged
    with the owning agent name. Only attached when DEBUG is on.
    """

    def __init__(self, agent_name: str):
        self._agent = agent_name
        self._tool_count = 0

    def __call__(self, **kwargs: Any) -> None:
        event = kwargs.get("event") or {}
        tool_use = (
            event.get("contentBlockStart", {}).get("start", {}).get("toolUse")
        )
        if tool_use:
            self._tool_count += 1
            name = tool_use.get("name", "?")
            tool_input = tool_use.get("input")
            logger.debug(
                "[trace] %s -> tool #%d: %s input=%s",
                self._agent,
                self._tool_count,
                name,
                _clip(_as_text(tool_input)) if tool_input is not None else "{}",
            )


def make_callback_handler(agent_name: str) -> Optional[DebugCallbackHandler]:
    """A tool-tracing handler when DEBUG is on, else ``None``.

    ``None`` preserves the existing behavior (Strands' output is suppressed);
    the debug handler is purely additive and only logs tool calls.
    """
    return DebugCallbackHandler(agent_name) if debug_enabled() else None


def log_invocation(agent_name: str, prompt: str) -> None:
    """Log that a sub-agent/supervisor is being invoked, with its prompt."""
    if not debug_enabled():
        return
    logger.debug("[trace] invoke %s; prompt=\n%s", agent_name, _clip(prompt))


def log_result(agent_name: str, result: Any, statistics: Optional[dict] = None) -> None:
    """Log the raw result of a sub-agent run, plus tool/usage metrics."""
    if not debug_enabled():
        return
    logger.debug("[trace] %s result=\n%s", agent_name, _clip(_as_text(result)))
    if statistics:
        tools = statistics.get("tools")
        if tools:
            logger.debug("[trace] %s tools used=%s", agent_name, _as_text(tools))
        usage = statistics.get("usage")
        if usage:
            logger.debug("[trace] %s token usage=%s", agent_name, _as_text(usage))


def log_history(agent_name: str, history: list[dict]) -> None:
    """Log the full conversation history at the finest level."""
    if not debug_enabled() or not history:
        return
    lines = [f'  {t.get("role", "?")}: {_clip(_as_text(t.get("text", "")))}'
             for t in history]
    logger.debug("[trace] %s conversation history:\n%s", agent_name, "\n".join(lines))
