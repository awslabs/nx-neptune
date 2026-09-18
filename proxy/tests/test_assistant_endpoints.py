# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assistant HTTP endpoints (spec §9, Group F): session mint, one-turn message,
and the model list. The Supervisor/Bedrock stack is mocked so the routes are
testable without ``strands-agents`` installed."""

from unittest.mock import MagicMock, patch

import pytest

from nx_neptune_proxy.assistant.schemas import (
    AssistantReply,
    FieldProposal,
    JumpAction,
)
from nx_neptune_proxy.routers import assistant as assistant_router


@pytest.mark.asyncio
async def test_create_session_returns_id(client):
    async with client as c:
        resp = await c.post("/api/v0/assistant/session")
    assert resp.status_code == 200
    assert resp.json()["session_id"]


@pytest.mark.asyncio
async def test_message_returns_assembled_reply(client):
    reply = AssistantReply(
        text="Here you go.",
        jumps=[JumpAction(kind="new-import", label="New import")],
        proposal=FieldProposal(catalog="AwsDataCatalog", database="tpch"),
    )
    sup = MagicMock()
    sup.handle_message.return_value = reply

    with patch.object(
        assistant_router, "_get_supervisor", return_value=sup
    ) as get_sup:
        async with client as c:
            resp = await c.post(
                "/api/v0/assistant/message",
                json={
                    "text": "import tpch",
                    "session_id": "s1",
                    "page_context": {"page": "import"},
                    "model": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                },
            )

        assert resp.status_code == 200
        body = resp.json()
        assert body["text"] == "Here you go."
        assert body["jumps"][0]["kind"] == "new-import"
        assert body["proposal"]["database"] == "tpch"

        # model override is threaded to supervisor selection + page_context parsed.
        get_sup.assert_called_once_with("us.anthropic.claude-sonnet-4-5-20250929-v1:0")

    _, args, _kwargs = sup.handle_message.mock_calls[0]
    assert args[0] == "import tpch"
    assert args[1] == "s1"
    assert args[2].page == "import"


@pytest.mark.asyncio
async def test_message_maps_agent_failure_to_502(client):
    sup = MagicMock()
    sup.handle_message.side_effect = RuntimeError("bedrock exploded")

    with patch.object(assistant_router, "_get_supervisor", return_value=sup):
        async with client as c:
            resp = await c.post(
                "/api/v0/assistant/message", json={"text": "hi"}
            )

    assert resp.status_code == 502
    assert "bedrock exploded" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_models_lists_default(client):
    async with client as c:
        resp = await c.get("/api/v0/assistant/models")
    assert resp.status_code == 200
    body = resp.json()
    assert body["default"] in body["models"]
    assert body["models"]


@pytest.mark.asyncio
async def test_message_requires_token(bare_client):
    async with bare_client as c:
        resp = await c.post("/api/v0/assistant/message", json={"text": "hi"})
    assert resp.status_code in (401, 403)


def test_get_supervisor_503_when_strands_missing():
    """When ``get_bedrock_model`` can't import strands, the route raises 503."""
    from fastapi import HTTPException

    with patch.object(
        assistant_router, "get_bedrock_model", side_effect=ImportError("no strands")
    ):
        assistant_router._supervisors.clear()
        with pytest.raises(HTTPException) as excinfo:
            assistant_router._get_supervisor("some-model")
    assert excinfo.value.status_code == 503
