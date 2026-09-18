# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Agent wiring + chat-route guardrail tests.

- The agent is constructed with the two projection tools and never touches
  Bedrock during these tests (no live model calls).
- The /api/v0/agent/chat route inherits the same auth + CSRF protections as
  every other state-changing route.
"""

from unittest.mock import MagicMock, patch

import pytest

from nx_neptune_proxy.agent.agent import build_agent


class TestAgentWiring:
    """build_agent wires the tools and prompt without calling Bedrock."""

    @patch("nx_neptune_proxy.agent.agent.BedrockModel")
    def test_agent_has_both_tools(self, mock_model):
        mock_model.return_value = MagicMock()
        agent = build_agent(model_id="test-model")
        # All projection tools are registered by name.
        assert "create_project" in agent.tool_names
        assert "list_databases" in agent.tool_names
        assert "get_schema" in agent.tool_names
        assert "create_projection_draft" in agent.tool_names

    @patch("nx_neptune_proxy.agent.agent.BedrockModel")
    def test_system_prompt_encodes_contract(self, mock_model):
        mock_model.return_value = MagicMock()
        agent = build_agent(model_id="test-model")
        prompt = agent.system_prompt or ""
        # The node/edge contract must be taught to the model.
        assert "~id" in prompt and "~label" in prompt
        assert "~from" in prompt and "~to" in prompt
        # Draft-only guardrail is stated.
        assert "DRAFT" in prompt

    @patch("nx_neptune_proxy.agent.agent.BedrockModel")
    def test_model_id_override_used(self, mock_model):
        mock_model.return_value = MagicMock()
        build_agent(model_id="my-custom-model")
        mock_model.assert_called_once()
        _, kwargs = mock_model.call_args
        assert kwargs.get("model_id") == "my-custom-model"


class TestChatRouteGuardrails:
    """The chat route sits behind the same middleware as other routes."""

    @pytest.mark.asyncio
    async def test_chat_without_csrf_header_rejected(self, bare_client):
        """POST without X-Requested-With must be blocked before reaching the agent."""
        resp = await bare_client.post(
            "/api/v0/agent/chat", json={"message": "hello"}
        )
        assert resp.status_code == 403
        assert resp.json()["error"] == "csrf_rejected"

    @pytest.mark.asyncio
    async def test_chat_without_token_rejected(self, bare_client):
        """Even with the CSRF header, a missing auth token must be rejected."""
        resp = await bare_client.post(
            "/api/v0/agent/chat",
            json={"message": "hello"},
            headers={"X-Requested-With": "nx-neptune"},
        )
        # require_token dependency rejects (401/403), never reaches the agent.
        assert resp.status_code in (401, 403)

    @pytest.mark.asyncio
    async def test_chat_empty_message_validation(self, client):
        """An empty message fails schema validation (422), not a model call."""
        resp = await client.post("/api/v0/agent/chat", json={"message": ""})
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_chat_authorized_invokes_agent_mocked(self, client):
        """A valid request reaches the agent; Bedrock is mocked (no live call)."""
        with patch(
            "nx_neptune_proxy.routers.agent.get_session_agent"
        ) as mock_get:
            fake_agent = MagicMock()

            async def _fake_invoke(prompt):
                return "Here is a proposed node query ..."

            fake_agent.invoke_async.side_effect = _fake_invoke
            mock_get.return_value = fake_agent

            resp = await client.post(
                "/api/v0/agent/chat",
                json={"message": "model my transactions"},
            )

        assert resp.status_code == 200
        assert "proposed node query" in resp.json()["reply"]
        fake_agent.invoke_async.assert_called_once()

    @pytest.mark.asyncio
    async def test_chat_reuses_single_session_agent(self, client):
        """Two chat turns reuse the SAME agent instance (stateful session)."""
        from nx_neptune_proxy.agent import agent as agent_mod

        agent_mod.reset_session()
        with patch("nx_neptune_proxy.agent.agent.build_agent") as mock_build:
            fake_agent = MagicMock()

            async def _fake_invoke(prompt):
                return "ok"

            fake_agent.invoke_async.side_effect = _fake_invoke
            mock_build.return_value = fake_agent

            await client.post("/api/v0/agent/chat", json={"message": "turn 1"})
            await client.post("/api/v0/agent/chat", json={"message": "turn 2"})

            # build_agent called once (lazy singleton); same agent handled both.
            mock_build.assert_called_once()
            assert fake_agent.invoke_async.call_count == 2
        agent_mod.reset_session()

    @pytest.mark.asyncio
    async def test_reset_starts_fresh_session(self, client):
        """POST /reset discards the agent so the next turn builds a new one."""
        from nx_neptune_proxy.agent import agent as agent_mod

        agent_mod.reset_session()
        with patch("nx_neptune_proxy.agent.agent.build_agent") as mock_build:
            mock_build.side_effect = lambda: MagicMock(
                invoke_async=MagicMock(side_effect=lambda p: _acoro("r"))
            )

            resp = await client.post("/api/v0/agent/reset")
            assert resp.status_code == 200
            assert resp.json()["status"] == "reset"
            # After reset, the singleton is cleared.
            assert agent_mod._session_agent is None
        agent_mod.reset_session()


async def _acoro(v):
    return v
