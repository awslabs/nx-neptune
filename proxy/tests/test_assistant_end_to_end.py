# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""End-to-end assistant flow (spec §9, Group I).

Exercises the whole Python path — HTTP endpoint → real ``Supervisor`` → real
tool closures → real specialist agents (prompt/parse + guardrails) → assembled
``AssistantReply`` serialized back over HTTP. Only the two LLM boundaries are
faked: the supervisor's routing agent (``_build_supervisor``) and each
specialist's ``_execute_agent`` return canned JSON, so no Bedrock / strands is
needed. This is the seam the TS client's ``remote.ts`` maps in Group G.
"""

from contextlib import ExitStack
from unittest.mock import patch

import pytest

from nx_neptune_proxy.assistant.agents.discovery import DiscoveryAgent
from nx_neptune_proxy.assistant.agents.navigation import NavigationAgent
from nx_neptune_proxy.assistant.agents.page_action import PageActionAgent
from nx_neptune_proxy.assistant.agents.query_planner import QueryPlannerAgent
from nx_neptune_proxy.assistant.agents.sql_mapping import SqlMappingAgent
from nx_neptune_proxy.assistant.supervisor import Supervisor
from nx_neptune_proxy.routers import assistant as assistant_router

# Canned specialist model replies (the JSON contracts of §9.5).
_DISCOVERY = (
    '{"tables": [{"name": "malware", '
    '"columns": [{"name": "id", "type": "string"}, '
    '{"name": "name", "type": "string"}], "sample_rows": null}]}'
)
_MAPPING = (
    '{"node_queries": [{"sql": "SELECT id AS \\"~id\\", \'Malware\' AS \\"~label\\", '
    'name FROM malware"}], "edge_queries": []}'
)
_PLAN = '{"graph_queries": [{"cypher": "MATCH (n) RETURN n"}]}'
_JUMPS = '{"jumps": [{"kind": "new-import", "label": "New import", "project_id": "p1"}]}'


def _fake_specialist_llms(stack: ExitStack):
    """Patch every specialist's LLM boundary to return canned JSON."""
    for cls, reply in (
        (DiscoveryAgent, _DISCOVERY),
        (SqlMappingAgent, _MAPPING),
        (QueryPlannerAgent, _PLAN),
        (NavigationAgent, _JUMPS),
        (PageActionAgent, '{"actions": []}'),
    ):
        stack.enter_context(patch.object(cls, "_execute_agent", return_value=reply))


@pytest.fixture(autouse=True)
def _fresh_supervisor_cache():
    assistant_router._supervisors.clear()
    yield
    assistant_router._supervisors.clear()


@pytest.mark.asyncio
async def test_generate_import_end_to_end(client):
    """A generate_import turn flows discovery → mapping → planner into a proposal
    on the HTTP response."""

    def fake_build(self, ctx):
        tools = {f.__name__: f for f in self._make_tools(ctx)}

        def run(_prompt):
            tools["generate_import"]("import malware", "AwsDataCatalog", "tpch",
                                     graph_name="threats")
            return "I mapped the malware table into an import."

        return run

    with ExitStack() as stack:
        # Route builds a real Supervisor; give it a dummy model (unused: the
        # supervisor + specialist LLMs are all faked below).
        stack.enter_context(
            patch.object(assistant_router, "get_bedrock_model", return_value=object())
        )
        stack.enter_context(patch.object(Supervisor, "_build_supervisor", fake_build))
        _fake_specialist_llms(stack)

        async with client as c:
            resp = await c.post(
                "/api/v0/assistant/message",
                json={"text": "import the malware table", "session_id": "e2e-1",
                      "page_context": {"page": "import"}},
            )

    assert resp.status_code == 200
    body = resp.json()
    assert body["text"] == "I mapped the malware table into an import."
    proposal = body["proposal"]
    assert proposal["catalog"] == "AwsDataCatalog"
    assert proposal["database"] == "tpch"
    assert proposal["graph_name"] == "threats"
    assert proposal["node_queries"][0]["sql"].startswith("SELECT id")
    assert proposal["edge_queries"] is None  # empty list collapses to None
    assert proposal["graph_queries"][0]["cypher"] == "MATCH (n) RETURN n"


@pytest.mark.asyncio
async def test_navigation_end_to_end(client):
    """A navigation turn returns mapped jump descriptors on the HTTP response."""

    def fake_build(self, ctx):
        tools = {f.__name__: f for f in self._make_tools(ctx)}

        def run(_prompt):
            tools["navigate"]("start a new import")
            return "Here's a shortcut to a new import."

        return run

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(assistant_router, "get_bedrock_model", return_value=object())
        )
        stack.enter_context(patch.object(Supervisor, "_build_supervisor", fake_build))
        _fake_specialist_llms(stack)

        async with client as c:
            resp = await c.post(
                "/api/v0/assistant/message",
                json={"text": "take me to a new import", "session_id": "e2e-2",
                      "page_context": {"page": "graphs", "project_id": "p1"}},
            )

    assert resp.status_code == 200
    body = resp.json()
    assert body["proposal"] is None
    assert body["jumps"][0]["kind"] == "new-import"
    assert body["jumps"][0]["project_id"] == "p1"


@pytest.mark.asyncio
async def test_discovery_cache_reused_across_turns(client):
    """A second import turn against the same database reuses the discovery cache
    (discovery LLM runs once; mapping re-runs each turn) — verified end-to-end
    through two HTTP requests sharing a session."""

    def fake_build(self, ctx):
        tools = {f.__name__: f for f in self._make_tools(ctx)}

        def run(_prompt):
            tools["generate_import"]("import", "AwsDataCatalog", "tpch")
            return "done"

        return run

    with ExitStack() as stack:
        stack.enter_context(
            patch.object(assistant_router, "get_bedrock_model", return_value=object())
        )
        stack.enter_context(patch.object(Supervisor, "_build_supervisor", fake_build))
        disc = stack.enter_context(
            patch.object(DiscoveryAgent, "_execute_agent", return_value=_DISCOVERY)
        )
        stack.enter_context(
            patch.object(SqlMappingAgent, "_execute_agent", return_value=_MAPPING)
        )
        stack.enter_context(
            patch.object(QueryPlannerAgent, "_execute_agent", return_value=_PLAN)
        )

        async with client as c:
            await c.post("/api/v0/assistant/message",
                         json={"text": "one", "session_id": "e2e-3"})
            await c.post("/api/v0/assistant/message",
                         json={"text": "two", "session_id": "e2e-3"})

    assert disc.call_count == 1  # discovery cached across the two turns
