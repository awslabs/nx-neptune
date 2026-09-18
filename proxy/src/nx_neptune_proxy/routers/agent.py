# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Agent chat endpoint.

Exposes a single conversational endpoint. The agent orchestrates its tools
server-side; the client only ever sees the conversation (its message in, the
assistant's reply out). Registered behind the same auth/CSRF/origin middleware
as every other router.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from nx_neptune_proxy.agent.agent import get_session_agent, reset_session
from nx_neptune_proxy.utils.sanitize import sanitize_error_message

logger = logging.getLogger("nx_neptune_proxy")

router = APIRouter(prefix="/api/v0/agent", tags=["agent"])

# Serialize turns against the single shared conversation so overlapping requests
# can't corrupt the agent's message history.
_session_lock = asyncio.Lock()


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=8000)
    # The project this conversation is scoped to (the UI knows which project is
    # open). The catalog/database/schema are NOT parameters — the user provides
    # and confirms them through the conversation itself.
    project_id: str | None = None


class ChatResponse(BaseModel):
    reply: str


def _grounding_preamble(req: ChatRequest) -> str:
    """Provide the project scope, if known. Catalog/database are intentionally
    NOT injected here — the user supplies and confirms them in the conversation.
    """
    if req.project_id:
        return f"Context: this conversation is for project_id={req.project_id}.\n\n"
    return ""


@router.post("/chat", summary="Chat with the projection-drafting agent")
async def chat(req: ChatRequest) -> ChatResponse:
    """Send a user turn to the agent and return its reply.

    The agent may call tools (schema lookup, draft creation) internally; only
    the conversational reply is returned to the client.
    """
    prompt = _grounding_preamble(req) + req.message
    async with _session_lock:
        agent = get_session_agent()
        try:
            result = await agent.invoke_async(prompt)
        except Exception as e:
            logger.error("Agent chat failed: %s", sanitize_error_message(str(e)))
            raise HTTPException(status_code=502, detail="Agent request failed")

    reply = str(result)
    # Log the reply server-side so a turn can be verified from the logs. Kept as
    # a single structured line (not the raw token stream) so it doesn't interleave
    # with other output. INFO so it shows at the default log level.
    logger.info("Agent reply (request): %s", reply)
    return ChatResponse(reply=reply)


@router.post("/reset", summary="Clear the conversation and start fresh")
async def reset() -> dict:
    """Discard the current shared conversation.

    Because the tool keeps a single process-wide conversation, this lets a user
    start a new projection discussion without stale context from a prior topic.
    """
    async with _session_lock:
        reset_session()
    return {"status": "reset"}
