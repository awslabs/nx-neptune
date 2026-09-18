# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Trace logging for the assistant (two tiers).

INFO (always on at the default level) gives the routing story — enough to see
what the assistant is doing without noise:

- which sub-agent / supervisor was invoked;
- which tools each one called;
- a short one-line summary of what each agent replied.

DEBUG (opt-in via ``LOG_LEVEL=DEBUG``) adds the finest detail:

- the full formatted prompt for each invocation;
- tool call *inputs*;
- the full raw model result and token/tool usage metrics;
- the full conversation history for a supervisor turn.

Untrusted values (prompts, tool inputs, model text) are logged as data via
``%`` args, never used as format strings, and clipped so a turn can't flood the
log (short clip at INFO, larger clip at DEBUG).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger("nx_neptune_proxy")

# Truncate very large payloads so a single turn can't flood the log.
_MAX_CHARS = 4000


def debug_enabled() -> bool:
    """True when the assistant logger is emitting DEBUG (finest detail)."""
    return logger.isEnabledFor(logging.DEBUG)


def _clip(text: str) -> str:
    if len(text) > _MAX_CHARS:
        return text[:_MAX_CHARS] + f"... [+{len(text) - _MAX_CHARS} chars]"
    return text


# Short clip for INFO-level one-liners (routing story, not full payloads).
_INFO_CHARS = 200


def _clip_short(text: str) -> str:
    text = " ".join(text.split())  # collapse newlines for a tidy one-liner
    if len(text) > _INFO_CHARS:
        return text[:_INFO_CHARS] + "..."
    return text


def _as_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except (TypeError, ValueError):
        return repr(value)


class DebugCallbackHandler:
    """Strands callback handler that logs tool invocations.

    Reads the same ``event -> contentBlockStart -> start -> toolUse`` block the
    built-in ``PrintingCallbackHandler`` uses, so it tracks each tool the model
    calls — but routes it to the logger instead of stdout, tagged with the
    owning agent name. The "which tool" line is INFO (always useful for seeing
    what an agent is doing); the tool *input* is only added at DEBUG.
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
            if debug_enabled():
                tool_input = tool_use.get("input")
                logger.info(
                    "[trace] %s -> tool #%d: %s input=%s",
                    self._agent,
                    self._tool_count,
                    name,
                    _clip(_as_text(tool_input)) if tool_input is not None else "{}",
                )
            else:
                logger.info(
                    "[trace] %s -> tool #%d: %s",
                    self._agent,
                    self._tool_count,
                    name,
                )


def make_callback_handler(agent_name: str) -> Optional[DebugCallbackHandler]:
    """A tool-tracing handler whenever INFO (or finer) is on, else ``None``.

    The handler logs which tools an agent calls at INFO, and adds tool inputs at
    DEBUG. ``None`` preserves the old behavior of suppressing Strands' output.
    """
    return DebugCallbackHandler(agent_name) if logger.isEnabledFor(logging.INFO) else None


def log_invocation(agent_name: str, prompt: str) -> None:
    """Log that a sub-agent/supervisor is being invoked.

    INFO: a one-line "which agent is working" marker. DEBUG: the full prompt too.
    """
    if debug_enabled():
        logger.info("[trace] invoke %s; prompt=\n%s", agent_name, _clip(prompt))
    elif logger.isEnabledFor(logging.INFO):
        logger.info("[trace] invoke %s", agent_name)


def log_result(agent_name: str, result: Any, statistics: Optional[dict] = None) -> None:
    """Log the result of a sub-agent run.

    INFO: a short one-line reply summary. DEBUG: the full raw result plus
    tool/usage metrics.
    """
    if debug_enabled():
        logger.info("[trace] %s result=\n%s", agent_name, _clip(_as_text(result)))
        if statistics:
            tools = statistics.get("tools")
            if tools:
                logger.info("[trace] %s tools used=%s", agent_name, _as_text(tools))
            usage = statistics.get("usage")
            if usage:
                logger.info("[trace] %s token usage=%s", agent_name, _as_text(usage))
    elif logger.isEnabledFor(logging.INFO):
        logger.info("[trace] %s replied: %s", agent_name, _clip_short(_as_text(result)))


def log_history(agent_name: str, history: list[dict]) -> None:
    """Log the full conversation history — DEBUG only (finest level)."""
    if not debug_enabled() or not history:
        return
    lines = [f'  {t.get("role", "?")}: {_clip(_as_text(t.get("text", "")))}'
             for t in history]
    logger.info("[trace] %s conversation history:\n%s", agent_name, "\n".join(lines))
