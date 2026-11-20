# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Assistant specialist agents (spec §9.5, Group D): prompt/parse contracts and
the page-action snapshot filter. Strands is never constructed here — each
agent's ``_execute_agent`` is patched to return a canned model reply."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from nx_neptune_proxy.assistant.agents import (
    DiscoveryAgent,
    NavigationAgent,
    PageActionAgent,
    QueryPlannerAgent,
    SqlMappingAgent,
)
from nx_neptune_proxy.assistant.agents.base_specialist import SpecialistAgent
from nx_neptune_proxy.assistant.agents.navigation import list_projects
from nx_neptune_proxy.assistant.schemas import (
    DiscoveryResult,
    GraphSchema,
    PageContext,
    PageContextAction,
    PageContextGraphTarget,
    SqlMappingResult,
)


def _canned(agent, reply: str):
    """Patch an agent's underlying execution to return a fixed model reply."""
    return patch.object(agent, "_execute_agent", return_value=reply)


# --- Discovery ------------------------------------------------------------


def test_discovery_parses_tables_and_columns():
    agent = DiscoveryAgent(bedrock_model=None)
    reply = (
        '{"tables": [{"name": "malware", '
        '"columns": [{"name": "id", "type": "string"}], "sample_rows": null}]}'
    )
    with _canned(agent, reply):
        result = agent.discover("cat", "db", "import malware as nodes")
    assert isinstance(result, DiscoveryResult)
    assert result.tables[0].name == "malware"
    assert result.tables[0].columns[0].type == "string"
    assert result.tables[0].sample_rows is None


# --- SQL Mapping ----------------------------------------------------------


def test_sql_mapping_parses_node_and_edge_queries():
    agent = SqlMappingAgent(bedrock_model=None)
    discovery = DiscoveryResult.model_validate(
        {"tables": [{"name": "t", "columns": [{"name": "id", "type": "string"}]}]}
    )
    reply = (
        '{"description": "Rows of t become nodes keyed by id.", '
        '"node_queries": [{"description": "One node per row of t.", '
        '"sql": "SELECT id AS \\"~id\\" FROM t"}], '
        '"edge_queries": []}'
    )
    with _canned(agent, reply):
        result = agent.map_schema(discovery, "make nodes")
    assert result.description == "Rows of t become nodes keyed by id."
    assert result.node_queries[0].sql == 'SELECT id AS "~id" FROM t'
    assert result.node_queries[0].description == "One node per row of t."
    assert result.edge_queries == []


# --- Query Planner --------------------------------------------------------


def test_query_planner_parses_queries_and_empty():
    agent = QueryPlannerAgent(bedrock_model=None)
    discovery = DiscoveryResult.model_validate({"tables": []})
    mapping = SqlMappingResult()

    with _canned(
        agent,
        '{"description": "Lists all nodes so you can see what loaded.", '
        '"graph_queries": [{"description": "Returns every node.", '
        '"cypher": "MATCH (n) RETURN n"}]}',
    ):
        result = agent.plan(mapping, "show everything", discovery)
    assert result.description == "Lists all nodes so you can see what loaded."
    assert result.graph_queries[0].cypher == "MATCH (n) RETURN n"
    assert result.graph_queries[0].description == "Returns every node."

    with _canned(agent, '{"graph_queries": []}'):
        assert agent.plan(mapping, "just import", discovery).graph_queries == []

    # Discovery is optional — proposing against an already-imported graph model.
    with _canned(agent, '{"graph_queries": [{"cypher": "MATCH (n) RETURN n"}]}'):
        assert agent.plan(mapping, "explore the graph").graph_queries[0].cypher == (
            "MATCH (n) RETURN n"
        )


def test_query_planner_grounds_on_live_schema_when_present():
    agent = QueryPlannerAgent(bedrock_model=None)
    schema = GraphSchema(
        node_labels=["Person"], edge_labels=["KNOWS"], node_properties=["name"]
    )
    # A non-empty live schema is authoritative: it is passed into the prompt and
    # the (empty) SQL mapping is not what grounds the plan.
    with patch.object(agent, "_execute_agent", return_value='{"graph_queries": []}') as ex:
        agent.plan(SqlMappingResult(), "explore", graph_schema=schema)
    model = ex.call_args.kwargs["model"]
    assert "LIVE GRAPH SCHEMA" in model and "Person" in model and "KNOWS" in model

    # An empty live schema falls back to the predicted (SQL) model path.
    with patch.object(agent, "_execute_agent", return_value='{"graph_queries": []}') as ex:
        agent.plan(SqlMappingResult(), "explore", graph_schema=GraphSchema())
    assert "PREDICTED MODEL" in ex.call_args.kwargs["model"]


# --- Navigation -----------------------------------------------------------


def test_navigation_parses_jumps():
    agent = NavigationAgent(bedrock_model=None)
    reply = (
        '{"jumps": [{"kind": "open-projections", "label": "Open TPCH", '
        '"project_id": "p1"}]}'
    )
    with _canned(agent, reply):
        jumps = agent.navigate("show the TPCH project", project_id=None)
    assert jumps[0].kind == "open-projections"
    assert jumps[0].project_id == "p1"


def test_navigation_no_jumps():
    agent = NavigationAgent(bedrock_model=None)
    with _canned(agent, '{"jumps": []}'):
        assert agent.navigate("what is a graph?") == []


@patch("nx_neptune_proxy.assistant.agents.navigation.ProjectStore")
def test_list_projects_tool_maps_id_and_name(mock_store):
    mock_store.return_value.list.return_value = [
        SimpleNamespace(id="p1", name="TPCH"),
        SimpleNamespace(id="p2", name="MITRE"),
    ]
    assert list_projects() == [
        {"id": "p1", "name": "TPCH"},
        {"id": "p2", "name": "MITRE"},
    ]


# --- Page-Action (snapshot filter) ---------------------------------------


def _page_context() -> PageContext:
    return PageContext(
        page="graphs",
        actions=[PageContextAction(key="refresh", label="Refresh")],
        graph_targets=[
            PageContextGraphTarget(id="g-1", name="malware", actions=["stop", "delete"])
        ],
    )


def test_page_action_filters_to_registered_keys_and_targets():
    agent = PageActionAgent(bedrock_model=None)
    reply = (
        '{"actions": ['
        '{"kind": "page-action", "page": "graphs", "label": "Refresh", '
        '"action_key": "refresh"},'
        '{"kind": "page-action", "page": "graphs", "label": "Bogus", '
        '"action_key": "not-registered"},'
        '{"kind": "graph-action", "page": "graphs", "label": "Stop", '
        '"graph_id": "g-1", "graph_action": "stop", "destructive": true},'
        '{"kind": "graph-action", "page": "graphs", "label": "Restart", '
        '"graph_id": "g-1", "graph_action": "restart"},'
        '{"kind": "graph-action", "page": "graphs", "label": "Stop ghost", '
        '"graph_id": "g-unknown", "graph_action": "stop"}'
        "]}"
    )
    with _canned(agent, reply):
        actions = agent.suggest(_page_context(), "clean up")

    # Only the registered page key and the valid (g-1, stop) target survive.
    kept = {(a.kind, a.action_key or a.graph_action) for a in actions}
    assert kept == {("page-action", "refresh"), ("graph-action", "stop")}


# --- SpecialistAgent lazy build ------------------------------------------


def test_specialist_builds_agent_once_and_caches():
    class _Dummy(SpecialistAgent):
        NAME = "dummy"
        SYSTEM_PROMPT = "sp"

        def _format_prompt(self, **kwargs):
            return "prompt"

    agent = _Dummy(bedrock_model="model")
    built = MagicMock(return_value="result")
    with patch(
        "nx_neptune_proxy.assistant.agents.base_specialist.build_agent",
        return_value=built,
    ) as mock_build:
        assert agent.execute_task() == "result"
        assert agent.execute_task() == "result"
    mock_build.assert_called_once_with("dummy", "sp", "model", [])
    assert built.call_count == 2  # agent reused, invoked per turn
