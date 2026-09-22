# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Supervisor agent (spec §9.2 / §9.9).

One Strands agent whose tools are the specialist agents (agents-as-tools). It
routes each turn to only the tools it needs and returns an ``AssistantReply``.
The tools have **no execution authority** (§9.3): they record *descriptors*
(jumps, a field proposal, page actions) into a per-turn :class:`TurnContext`
and return a short text summary to the supervisor LLM; after the turn, the
structured artifacts are read back from the context and assembled into the
reply. The supervisor's final message becomes ``reply.text``.

``generate_import`` chains Discovery → SQL Mapping → Query Planner into a single
field proposal, reusing the session's discovery cache.
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from nx_neptune_proxy.assistant.agents.base_specialist import as_tools, build_agent
from nx_neptune_proxy.assistant.agents.discovery import DiscoveryAgent
from nx_neptune_proxy.assistant.agents.navigation import NavigationAgent
from nx_neptune_proxy.assistant.agents.page_action import PageActionAgent
from nx_neptune_proxy.assistant.agents.query_planner import QueryPlannerAgent
from nx_neptune_proxy.assistant.agents.sql_mapping import SqlMappingAgent
from nx_neptune_proxy.assistant.athena_tools import list_buckets as _list_buckets
from nx_neptune_proxy.assistant.athena_tools import list_catalogs as _list_catalogs
from nx_neptune_proxy.assistant.debug_trace import (
    log_history,
    log_invocation,
    log_result,
)
from nx_neptune_proxy.assistant.graph_tools import fetch_graph_schema
from nx_neptune_proxy.assistant.schemas import (
    AssistantReply,
    ChatAction,
    FieldProposal,
    JumpAction,
    PageContext,
    SqlMappingResult,
)
from nx_neptune_proxy.assistant.session import Session, SessionStore
from nx_neptune_proxy.assistant.skills import CAPABILITY_HINTS

logger = logging.getLogger(__name__)


@dataclass
class TurnContext:
    """Collects the structured descriptors produced during one turn."""

    session: Session
    page_context: Optional[PageContext] = None
    jumps: list[JumpAction] = field(default_factory=list)
    actions: list[ChatAction] = field(default_factory=list)
    proposal: Optional[FieldProposal] = None
    question: Optional[str] = None


SUPERVISOR_SYSTEM_PROMPT = """You are the assistant for a graph-import web app \
(relational data in Amazon Athena → an Amazon Neptune graph). You help the user \
by routing their request to your tools and replying in plain language.

You can only *propose* — never act. Navigation, form changes, and page actions \
are applied by the user clicking what you propose. Do not claim you performed \
an action.

Tools (call only the ones a request needs):
- navigate(request): propose cross-page navigation (e.g. "start a new import", \
"open the TPCH projections").
- list_catalogs(): list the Athena data catalogs available to import from. Use \
this when the user asks what catalogs/data sources are available, or has no \
catalog in mind — name the real options instead of telling them to look it up \
elsewhere.
- list_buckets(): list the S3 buckets available for an import's export/staging \
(the same list the import page's bucket selector shows). Use this when the user \
asks which buckets/output locations exist, or needs to choose a bucket — name \
real options instead of asking them to type one.
- generate_import(request, catalog, database, bucket, graph_name): propose the \
import mapping (schema discovery → node/edge SQL → optional graph queries). You \
MUST know the Athena catalog first. When the current page context includes a \
catalog (and database), use those values unless the user names different ones; \
if no catalog is available at all, call list_catalogs() to offer options rather \
than calling this tool with an empty catalog. database is optional: pass it to \
target one database, or leave it empty to let discovery pick the most relevant \
database in the catalog. bucket and graph_name are optional — if the user needs to \
pick a bucket, call list_buckets() to offer the real options.
- propose_queries(request): propose openCypher queries to run against the graph \
that already exists, WITHOUT rebuilding the import. Use this instead of \
generate_import when the user wants to query/explore/analyze a graph and the \
page context shows one is already set up (a projection_id, a graph_status, or \
node/edge queries are present). Do NOT re-run generate_import just to get \
queries when the import already exists.
- suggest_page_actions(request): surface actions available on the current page \
(e.g. stopping a graph, executing an import).

A pure navigation request must not trigger import generation, and vice versa. \
Keep your final reply short: the proposed jumps, form fields, and actions are \
attached to your message automatically, so summarize rather than repeat them. \
When generate_import or propose_queries returns a plain-language description of \
what the mapping models or what the queries accomplish, relay that intent to \
the user (verbatim or lightly summarized) so they understand WHAT was proposed \
and WHY — do not drop it in favor of bare counts.

# Reading page state
The page context is a live snapshot of what the user is looking at. Trust it \
over your assumptions:
- A projection_id (or graph_status / node/edge queries) means a projection \
already EXISTS — do not tell the user to create one, and do not call \
generate_import to "start" it again. Prefer propose_queries for query requests.
- graph_status tells you where the import is: "draft" (configured, not run), \
"executing" (import running), "complete" (graph is ready to query), "failed". \
When it is "complete" the graph is live — propose openCypher via propose_queries \
rather than regenerating the import.
- You can still propose openCypher regardless of graph_status; the user may want \
the queries ready before or while the import runs.

# When a graph is worth it
""" + CAPABILITY_HINTS


class Supervisor:
    NAME = "supervisor"

    def __init__(self, bedrock_model, session_store: SessionStore):
        self._model = bedrock_model
        self._sessions = session_store
        self._navigation = NavigationAgent(bedrock_model)
        self._discovery = DiscoveryAgent(bedrock_model)
        self._sql_mapping = SqlMappingAgent(bedrock_model)
        self._query_planner = QueryPlannerAgent(bedrock_model)
        self._page_action = PageActionAgent(bedrock_model)

    # --- Public entry point ----------------------------------------------

    def handle_message(
        self,
        text: str,
        session_id: Optional[str],
        page_context: Optional[PageContext] = None,
    ) -> AssistantReply:
        session = self._sessions.get_or_create(session_id)
        ctx = TurnContext(session=session, page_context=page_context)

        log_history(self.NAME, session.history)
        agent = self._build_supervisor(ctx)
        prompt = self._format_prompt(text, page_context, session)
        log_invocation(self.NAME, prompt)
        result = agent(prompt)
        reply_text = str(result)
        log_result(self.NAME, reply_text)

        session.add_turn("user", text)
        session.add_turn("assistant", reply_text)
        return self._assemble_reply(reply_text, ctx)

    # --- Tool layer (agents-as-tools) ------------------------------------

    def _make_tools(self, ctx: "TurnContext") -> list:
        """Build the per-turn tool functions bound to ``ctx`` (plain funcs;
        wrapped as Strands tools by :meth:`_build_supervisor`)."""

        def navigate(request: str) -> str:
            """Propose cross-page navigation (jumps) for the user's request."""
            project_id = ctx.page_context.project_id if ctx.page_context else None
            jumps = self._navigation.navigate(request, project_id)
            ctx.jumps.extend(jumps)
            return f"Proposed {len(jumps)} navigation option(s)."

        def list_catalogs() -> str:
            """List the Athena data catalogs available to import from.

            Use this when the user asks what catalogs/data sources exist, or has
            no catalog in mind yet — so you can name real options instead of
            asking them to look it up elsewhere. Metadata only, no query cost."""
            catalogs = _list_catalogs()
            if not catalogs:
                return "No Athena catalogs are available."
            named = ", ".join(
                f'{c["name"]} ({c["type"]})' if c.get("type") else c["name"]
                for c in catalogs
            )
            return f"Available Athena catalogs: {named}."

        def list_buckets() -> str:
            """List the S3 buckets available for an import's export/staging.

            Use this when the user asks which buckets/output locations exist, or
            needs to pick a bucket for an import — so you can name real options
            (the same list the import page's bucket selector shows) instead of
            asking them to type one. Read-only, region-filtered."""
            buckets = _list_buckets()
            if not buckets:
                return "No S3 buckets are available in the configured region."
            return f"Available S3 buckets: {', '.join(buckets)}."

        def generate_import(
            request: str,
            catalog: str,
            database: str,
            bucket: str = "",
            graph_name: str = "",
        ) -> str:
            """Propose the import mapping for the given Athena catalog:
            schema discovery, node/edge SQL, and optional openCypher graph
            queries. ``database`` is optional — when empty, discovery picks the
            most relevant database in the catalog for the request."""
            if not catalog:
                ctx.question = "Which Athena catalog should I import from?"
                return "Need the catalog before generating an import."

            # An import always belongs to a project (the projection is created
            # under one). Without a project in context there is nowhere to save
            # the import, so ask the user to create a project first and offer the
            # jump rather than generating a proposal that cannot be saved.
            project_id = ctx.page_context.project_id if ctx.page_context else None
            if not project_id:
                ctx.question = (
                    "You'll need a project before I can set up an import. "
                    "Create a new project first, then ask me again."
                )
                ctx.jumps.append(
                    JumpAction(kind="new-project", label="Create a new project")
                )
                return "No project in context; asked the user to create one first."

            discovery = ctx.session.get_discovery(catalog, database)
            if discovery is None:
                discovery = self._discovery.discover(catalog, database, request)
                ctx.session.cache_discovery(catalog, database, discovery)

            mapping = self._sql_mapping.map_schema(discovery, request)
            # The mapping's node/edge queries define the graph model (labels,
            # properties, edge types); pass them to the planner so its openCypher
            # targets the same model rather than re-deriving it from discovery.
            plan = self._query_planner.plan(mapping, request, discovery)

            ctx.proposal = FieldProposal(
                catalog=catalog,
                database=database or None,
                bucket=bucket or None,
                graph_name=graph_name or None,
                node_queries=mapping.node_queries or None,
                edge_queries=mapping.edge_queries or None,
                graph_queries=plan.graph_queries or None,
            )
            # Relay the agents' plain-language intent so the supervisor can pass
            # it (or a summary) on to the user, not just the query counts.
            summary = (
                f"Proposed an import: {len(mapping.node_queries)} node and "
                f"{len(mapping.edge_queries)} edge query(ies)."
            )
            if mapping.description:
                summary += f" Graph model: {mapping.description}"
            if plan.graph_queries and plan.description:
                summary += f" Suggested queries: {plan.description}"
            return summary

        def propose_queries(request: str) -> str:
            """Propose openCypher graph queries to run against the existing
            graph, WITHOUT regenerating the import.

            Use this — not generate_import — when a graph/projection already
            exists (the page context has a projection_id, graph_status, or
            node/edge queries) and the user wants to query, explore, or analyze
            it. When the graph is already live (a graph_id is present) its real
            schema is read from the database and used as the authoritative
            model; otherwise the page's node/edge queries define the predicted
            model, or the planner proposes general-purpose queries if none are
            present. Works regardless of whether the import has finished
            running."""
            pc = ctx.page_context
            node_queries = list(pc.node_queries) if pc else []
            edge_queries = list(pc.edge_queries) if pc else []
            mapping = SqlMappingResult(
                node_queries=node_queries, edge_queries=edge_queries
            )
            # Once the graph exists, its live schema is authoritative — read it
            # so the planner grounds on the real labels/edge types/properties
            # rather than the predicted node/edge SQL. Best-effort: a failed
            # read falls back to the SQL model below.
            graph_schema = None
            graph_id = pc.graph_id if pc else None
            if graph_id and pc and pc.graph_available:
                try:
                    graph_schema = fetch_graph_schema(graph_id)
                except Exception as exc:  # noqa: BLE001 - degrade to SQL model
                    logger.warning(
                        "Live graph schema fetch failed for %s: %s", graph_id, exc
                    )
            plan = self._query_planner.plan(
                mapping, request, graph_schema=graph_schema
            )
            # Only the graph_queries change; leaving the other fields None means
            # the client applies just the openCypher without touching the form's
            # catalog/database/SQL (spec §9.5).
            ctx.proposal = FieldProposal(graph_queries=plan.graph_queries or None)
            # Relay the planner's plain-language intent to the supervisor so the
            # user hears what the queries accomplish, not just how many there are.
            summary = f"Proposed {len(plan.graph_queries)} openCypher query(ies)."
            if plan.description:
                summary += f" {plan.description}"
            return summary

        def suggest_page_actions(request: str) -> str:
            """Surface actions available on the current page for the request."""
            if ctx.page_context is None:
                return "No page context is available for this turn."
            actions = self._page_action.suggest(ctx.page_context, request)
            ctx.actions.extend(actions)
            return f"Surfaced {len(actions)} page action(s)."

        return [
            navigate,
            list_catalogs,
            list_buckets,
            generate_import,
            propose_queries,
            suggest_page_actions,
        ]

    def _build_supervisor(self, ctx: "TurnContext"):
        return build_agent(
            self.NAME,
            SUPERVISOR_SYSTEM_PROMPT,
            self._model,
            as_tools(self._make_tools(ctx)),
        )

    # --- Prompt & reply assembly -----------------------------------------

    def _format_prompt(
        self,
        text: str,
        page_context: Optional[PageContext],
        session: Session,
    ) -> str:
        parts = []
        if session.history:
            recent = session.history[-6:]
            convo = "\n".join(f'{t["role"]}: {t["text"]}' for t in recent)
            parts.append(f"# Conversation so far\n{convo}")
        if page_context is not None:
            parts.append(
                f"# Current page\n{page_context.model_dump_json(exclude_none=True)}"
            )
        parts.append(f"# User request\n{text}")
        return "\n\n".join(parts)

    def _assemble_reply(self, reply_text: str, ctx: "TurnContext") -> AssistantReply:
        return AssistantReply(
            text=reply_text,
            proposal=ctx.proposal,
            jumps=ctx.jumps,
            actions=ctx.actions,
            question=ctx.question,
        )
