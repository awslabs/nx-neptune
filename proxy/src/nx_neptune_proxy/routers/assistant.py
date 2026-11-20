# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI assistant endpoints (spec §9, Group F).

Exposes the Strands supervisor to the chat drawer:

- ``POST /assistant/session`` — mint a session id (holds history + the
  per-session discovery cache).
- ``POST /assistant/message`` — run one turn through the supervisor and return
  the assembled :class:`AssistantReply` (text + suggest-only descriptors).
- ``GET  /assistant/models`` — the Bedrock model(s) the drawer's selector may
  choose from (default first).

The supervisor and its Bedrock model are built lazily, per ``model_id``, on the
first message so importing this module never requires ``strands-agents`` and the
session/models endpoints work without it. All supervisors share one process-wide
:class:`SessionStore` so a model switch mid-conversation keeps the same history.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from nx_neptune_proxy.assistant.bedrock import get_bedrock_model
from nx_neptune_proxy.assistant.schemas import AssistantReply, PageContext
from nx_neptune_proxy.assistant.session import SessionStore
from nx_neptune_proxy.assistant.supervisor import Supervisor
from nx_neptune_proxy.config import get_settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v0/assistant", tags=["assistant"])


# --- Wire contracts (snake_case; the client maps camelCase → these) -------


class SessionResponse(BaseModel):
    session_id: str


class MessageRequest(BaseModel):
    text: str
    session_id: Optional[str] = None
    page_context: Optional[PageContext] = None
    # Optional per-request Bedrock model override (drawer's model selector).
    model: Optional[str] = None


class ModelsResponse(BaseModel):
    models: list[str] = Field(default_factory=list)
    default: str


# --- Lazily-built, process-wide supervisor(s) -----------------------------

_session_store = SessionStore()
# model_id -> Supervisor, so a per-request override reuses its own agent stack
# while sharing the one session store above.
_supervisors: dict[str, Supervisor] = {}


def _get_supervisor(model_id: Optional[str]) -> Supervisor:
    """Return the supervisor for ``model_id`` (default when unset), building it
    (and its Bedrock model — the first import of ``strands``) on first use."""
    resolved = model_id or get_settings().bedrock_model
    supervisor = _supervisors.get(resolved)
    if supervisor is None:
        try:
            model = get_bedrock_model(resolved)
        except ImportError as exc:  # strands-agents not installed
            logger.error("Assistant unavailable: %s", exc)
            raise HTTPException(
                status_code=503,
                detail="AI assistant is unavailable: strands-agents is not "
                "installed on the server.",
            ) from exc
        supervisor = Supervisor(bedrock_model=model, session_store=_session_store)
        _supervisors[resolved] = supervisor
    return supervisor


# --- Endpoints ------------------------------------------------------------


@router.post("/session", summary="Start an assistant session")
def create_session() -> SessionResponse:
    session = _session_store.create()
    return SessionResponse(session_id=session.session_id)


@router.post("/message", summary="Send a message to the assistant")
def post_message(body: MessageRequest) -> AssistantReply:
    supervisor = _get_supervisor(body.model)
    try:
        return supervisor.handle_message(
            body.text, body.session_id, body.page_context
        )
    except Exception as exc:  # surface agent/Bedrock failures as 502
        logger.exception("Assistant turn failed")
        raise HTTPException(
            status_code=502, detail=f"Assistant failed: {exc}"
        ) from exc


@router.get("/models", summary="List selectable assistant models")
def list_models() -> ModelsResponse:
    default = get_settings().bedrock_model
    return ModelsResponse(models=[default], default=default)
