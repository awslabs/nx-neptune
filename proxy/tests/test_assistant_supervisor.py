# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Supervisor orchestration (spec §9.2/§9.9, Group E): the agents-as-tools
layer, generate_import chaining + discovery cache, and reply assembly. The
Strands supervisor LLM is faked; the specialist agents are mocked."""

from unittest.mock import MagicMock, patch

from nx_neptune_proxy.assistant.schemas import (
    ChatAction,
    CypherQuery,
    DiscoveryResult,
    GraphSchema,
    JumpAction,
    PageContext,
    QueryPlanResult,
    SqlMappingResult,
    SqlQuery,
)
from nx_neptune_proxy.assistant.session import SessionStore
from nx_neptune_proxy.assistant.supervisor import Supervisor, TurnContext


def _supervisor_with_mock_specialists():
    sup = Supervisor(bedrock_model=None, session_store=SessionStore())
    sup._navigation = MagicMock()
    sup._discovery = MagicMock()
    sup._sql_mapping = MagicMock()
    sup._query_planner = MagicMock()
    sup._page_action = MagicMock()

    sup._navigation.navigate.return_value = [
        JumpAction(kind="new-import", label="New import")
    ]
    sup._discovery.discover.return_value = DiscoveryResult(tables=[])
    sup._sql_mapping.map_schema.return_value = SqlMappingResult(
        node_queries=[SqlQuery(sql='SELECT 1 AS "~id"')], edge_queries=[]
    )
    sup._query_planner.plan.return_value = QueryPlanResult(graph_queries=[])
    sup._page_action.suggest.return_value = [
        ChatAction(kind="page-action", page="import", label="Execute", action_key="x")
    ]
    return sup


def _tools(sup, ctx):
    return {f.__name__: f for f in sup._make_tools(ctx)}


# --- individual tools -----------------------------------------------------


def test_navigate_tool_records_jumps_and_passes_project_id():
    sup = _supervisor_with_mock_specialists()
    ctx = TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(page="graphs", project_id="p1"),
    )
    tools = _tools(sup, ctx)

    tools["navigate"]("open projections")
    assert len(ctx.jumps) == 1
    sup._navigation.navigate.assert_called_once_with("open projections", "p1")


def _import_ctx(sup):
    """A turn on the import page with a project in context (imports require one)."""
    return TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(page="import", project_id="p1"),
    )


def test_generate_import_chains_and_builds_proposal():
    sup = _supervisor_with_mock_specialists()
    ctx = _import_ctx(sup)
    tools = _tools(sup, ctx)

    tools["generate_import"]("import it", "AwsDataCatalog", "tpch", graph_name="g")

    assert ctx.proposal.catalog == "AwsDataCatalog"
    assert ctx.proposal.database == "tpch"
    assert ctx.proposal.graph_name == "g"
    assert len(ctx.proposal.node_queries) == 1
    assert ctx.proposal.edge_queries is None  # empty list collapses to None
    sup._discovery.discover.assert_called_once()
    sup._sql_mapping.map_schema.assert_called_once()
    sup._query_planner.plan.assert_called_once()


def test_propose_queries_uses_existing_graph_model_without_import():
    sup = _supervisor_with_mock_specialists()
    sup._query_planner.plan.return_value = QueryPlanResult(
        graph_queries=[CypherQuery(cypher="MATCH (n) RETURN n LIMIT 10")]
    )
    ctx = TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(
            page="import",
            projection_id="proj-1",
            graph_status="complete",
            node_queries=[SqlQuery(sql='SELECT 1 AS "~id"')],
            edge_queries=[SqlQuery(sql='SELECT 1 AS "~from"')],
        ),
    )
    tools = _tools(sup, ctx)

    tools["propose_queries"]("show me the top nodes")

    # Only graph queries are proposed; import fields stay untouched.
    assert ctx.proposal.graph_queries[0].cypher == "MATCH (n) RETURN n LIMIT 10"
    assert ctx.proposal.catalog is None
    assert ctx.proposal.node_queries is None
    # Planner is grounded on the page's node/edge model, and no import runs.
    mapping = sup._query_planner.plan.call_args.args[0]
    assert len(mapping.node_queries) == 1 and len(mapping.edge_queries) == 1
    sup._discovery.discover.assert_not_called()
    sup._sql_mapping.map_schema.assert_not_called()


def test_generate_import_relays_agent_descriptions():
    sup = _supervisor_with_mock_specialists()
    sup._sql_mapping.map_schema.return_value = SqlMappingResult(
        description="Customers become nodes, orders become edges.",
        node_queries=[
            SqlQuery(sql='SELECT 1 AS "~id"', description="One node per customer.")
        ],
        edge_queries=[
            SqlQuery(
                sql='SELECT 1 AS "~from"', description="Links orders to customers."
            )
        ],
    )
    sup._query_planner.plan.return_value = QueryPlanResult(
        description="Finds your most connected customers.",
        graph_queries=[
            CypherQuery(cypher="MATCH (n) RETURN n", description="Top customers.")
        ],
    )
    ctx = _import_ctx(sup)
    tools = _tools(sup, ctx)

    reply = tools["generate_import"]("import it", "AwsDataCatalog", "tpch")

    # The tool return (what the supervisor LLM relays) carries the overall
    # intents plus each query's own purpose.
    assert "Customers become nodes, orders become edges." in reply
    assert "Finds your most connected customers." in reply
    assert "One node per customer." in reply
    assert "Links orders to customers." in reply
    assert "Top customers." in reply


def test_propose_queries_relays_planner_description():
    sup = _supervisor_with_mock_specialists()
    sup._query_planner.plan.return_value = QueryPlanResult(
        description="Samples people so you can eyeball the data.",
        graph_queries=[
            CypherQuery(
                cypher="MATCH (n:Person) RETURN n LIMIT 25",
                description="Shows 25 sample people.",
            )
        ],
    )
    ctx = TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(page="import", projection_id="p-1"),
    )
    tools = _tools(sup, ctx)

    reply = tools["propose_queries"]("show me people")
    assert "Samples people so you can eyeball the data." in reply
    assert "Shows 25 sample people." in reply


def test_propose_queries_grounds_on_live_schema_when_graph_exists():
    sup = _supervisor_with_mock_specialists()
    sup._query_planner.plan.return_value = QueryPlanResult(
        graph_queries=[CypherQuery(cypher="MATCH (n:Person) RETURN n LIMIT 25")]
    )
    ctx = TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(
            page="import",
            projection_id="proj-1",
            graph_status="complete",
            graph_id="g-123",
            node_queries=[SqlQuery(sql='SELECT 1 AS "~id"')],
        ),
    )
    tools = _tools(sup, ctx)

    live = GraphSchema(node_labels=["Person"], edge_labels=["KNOWS"])
    with patch(
        "nx_neptune_proxy.assistant.supervisor.fetch_graph_schema",
        return_value=live,
    ) as mock_fetch:
        tools["propose_queries"]("show me the top people")

    # The live schema was read for the graph and handed to the planner as the
    # authoritative model (not the SQL mapping).
    mock_fetch.assert_called_once_with("g-123")
    assert sup._query_planner.plan.call_args.kwargs["graph_schema"] is live
    assert ctx.proposal.graph_queries[0].cypher == "MATCH (n:Person) RETURN n LIMIT 25"


def test_propose_queries_falls_back_to_sql_when_schema_fetch_fails():
    sup = _supervisor_with_mock_specialists()
    sup._query_planner.plan.return_value = QueryPlanResult(
        graph_queries=[CypherQuery(cypher="MATCH (n) RETURN n LIMIT 25")]
    )
    ctx = TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(
            page="import", graph_status="complete", graph_id="g-123"
        ),
    )
    tools = _tools(sup, ctx)

    with patch(
        "nx_neptune_proxy.assistant.supervisor.fetch_graph_schema",
        side_effect=RuntimeError("boom"),
    ):
        tools["propose_queries"]("explore")

    # A failed live-schema read degrades to the predicted model — no schema is
    # passed, and the plan still runs.
    assert sup._query_planner.plan.call_args.kwargs["graph_schema"] is None
    sup._query_planner.plan.assert_called_once()


def test_propose_queries_without_graph_model_still_proposes():
    sup = _supervisor_with_mock_specialists()
    sup._query_planner.plan.return_value = QueryPlanResult(
        graph_queries=[CypherQuery(cypher="MATCH (n) RETURN n LIMIT 10")]
    )
    ctx = TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(page="import"),
    )
    tools = _tools(sup, ctx)

    tools["propose_queries"]("give me a query")

    assert ctx.proposal.graph_queries[0].cypher == "MATCH (n) RETURN n LIMIT 10"
    sup._query_planner.plan.assert_called_once()


def test_update_import_fields_sets_only_bucket_and_skips_specialists():
    sup = _supervisor_with_mock_specialists()
    ctx = _import_ctx(sup)
    tools = _tools(sup, ctx)

    tools["update_import_fields"](bucket="nx-neptune-staging")

    assert ctx.proposal.bucket == "nx-neptune-staging"
    assert ctx.proposal.database is None
    assert ctx.proposal.node_queries is None
    sup._discovery.discover.assert_not_called()
    sup._sql_mapping.map_schema.assert_not_called()
    sup._query_planner.plan.assert_not_called()


def test_update_import_fields_merges_without_clobbering_queries():
    sup = _supervisor_with_mock_specialists()
    ctx = _import_ctx(sup)
    tools = _tools(sup, ctx)

    tools["generate_import"]("import it", "cat", "db", graph_name="g")
    assert len(ctx.proposal.node_queries) == 1

    tools["update_import_fields"](bucket="nx-neptune-staging")

    assert ctx.proposal.bucket == "nx-neptune-staging"
    assert len(ctx.proposal.node_queries) == 1  # preserved
    assert ctx.proposal.catalog == "cat"
    assert ctx.proposal.graph_name == "g"


def test_generate_import_reuses_discovery_cache_within_session():
    sup = _supervisor_with_mock_specialists()
    ctx = _import_ctx(sup)
    tools = _tools(sup, ctx)

    tools["generate_import"]("first", "cat", "db")
    tools["generate_import"]("second", "cat", "db")  # same DB → cache hit

    assert sup._discovery.discover.call_count == 1
    assert sup._sql_mapping.map_schema.call_count == 2  # mapping still re-runs


def test_generate_import_reruns_discovery_when_database_changes():
    sup = _supervisor_with_mock_specialists()
    ctx = _import_ctx(sup)
    tools = _tools(sup, ctx)

    tools["generate_import"]("a", "cat", "db1")
    tools["generate_import"]("b", "cat", "db2")

    assert sup._discovery.discover.call_count == 2


def test_generate_import_asks_when_catalog_or_database_missing():
    sup = _supervisor_with_mock_specialists()
    ctx = TurnContext(session=sup._sessions.create())
    tools = _tools(sup, ctx)

    tools["generate_import"]("import something", "", "")

    assert ctx.question is not None
    assert ctx.proposal is None
    sup._discovery.discover.assert_not_called()


def test_generate_import_asks_for_project_when_missing():
    sup = _supervisor_with_mock_specialists()
    # Catalog is known but there is no project in the page context.
    ctx = TurnContext(
        session=sup._sessions.create(),
        page_context=PageContext(page="import"),
    )
    tools = _tools(sup, ctx)

    tools["generate_import"]("import it", "AwsDataCatalog", "tpch")

    assert ctx.question is not None
    assert ctx.proposal is None
    assert [j.kind for j in ctx.jumps] == ["new-project"]
    sup._discovery.discover.assert_not_called()


def test_suggest_page_actions_requires_context():
    sup = _supervisor_with_mock_specialists()
    ctx = TurnContext(session=sup._sessions.create(), page_context=None)
    tools = _tools(sup, ctx)

    tools["suggest_page_actions"]("do something")
    assert ctx.actions == []
    sup._page_action.suggest.assert_not_called()


def test_suggest_page_actions_records_actions():
    sup = _supervisor_with_mock_specialists()
    ctx = TurnContext(
        session=sup._sessions.create(), page_context=PageContext(page="import")
    )
    tools = _tools(sup, ctx)

    tools["suggest_page_actions"]("what can I do")
    assert len(ctx.actions) == 1


# --- handle_message assembly ---------------------------------------------


def test_handle_message_assembles_reply_and_records_history():
    sup = _supervisor_with_mock_specialists()

    def fake_build(ctx):
        tools = _tools(sup, ctx)

        def run(prompt):
            tools["navigate"]("go")
            return "Here are your options."

        return run

    with patch.object(sup, "_build_supervisor", side_effect=fake_build):
        reply = sup.handle_message(
            "take me to a new import", "s1", PageContext(page="import")
        )

    assert reply.text == "Here are your options."
    assert len(reply.jumps) == 1
    assert reply.proposal is None
    session = sup._sessions.get("s1")
    assert [t["role"] for t in session.history] == ["user", "assistant"]


# --- session store --------------------------------------------------------


def test_session_store_create_and_get_or_create():
    store = SessionStore()
    s = store.create()
    assert store.get(s.session_id) is s
    assert store.get_or_create(s.session_id) is s
    # unknown id becomes a new session keyed by that id
    made = store.get_or_create("brand-new")
    assert made.session_id == "brand-new"
    assert store.get("brand-new") is made
