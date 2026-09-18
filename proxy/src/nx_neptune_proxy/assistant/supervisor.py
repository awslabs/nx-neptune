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
from nx_neptune_proxy.assistant.athena_tools import list_catalogs as _list_catalogs
from nx_neptune_proxy.assistant.schemas import (
    AssistantReply,
    ChatAction,
    FieldProposal,
    JumpAction,
    PageContext,
)
from nx_neptune_proxy.assistant.session import Session, SessionStore

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
- generate_import(request, catalog, database, bucket, graph_name): propose the \
import mapping (schema discovery → node/edge SQL → optional graph queries). You \
MUST know the Athena catalog first. When the current page context includes a \
catalog (and database), use those values unless the user names different ones; \
if no catalog is available at all, call list_catalogs() to offer options rather \
than calling this tool with an empty catalog. database is optional: pass it to \
target one database, or leave it empty to search every database in the catalog \
for relevant tables. bucket and graph_name are optional.
- suggest_page_actions(request): surface actions available on the current page \
(e.g. stopping a graph, executing an import).

A pure navigation request must not trigger import generation, and vice versa. \
Keep your final reply short: the proposed jumps, form fields, and actions are \
attached to your message automatically, so summarize rather than repeat them.
"""


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

        agent = self._build_supervisor(ctx)
        result = agent(self._format_prompt(text, page_context, session))
        reply_text = str(result)

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

        def generate_import(
            request: str,
            catalog: str,
            database: str,
            bucket: str = "",
            graph_name: str = "",
        ) -> str:
            """Propose the import mapping for the given Athena catalog:
            schema discovery, node/edge SQL, and optional openCypher graph
            queries. ``database`` is optional — when empty, discovery searches
            every database in the catalog for relevant tables."""
            if not catalog:
                ctx.question = "Which Athena catalog should I import from?"
                return "Need the catalog before generating an import."

            discovery = ctx.session.get_discovery(catalog, database)
            if discovery is None:
                discovery = self._discovery.discover(catalog, database, request)
                ctx.session.cache_discovery(catalog, database, discovery)

            mapping = self._sql_mapping.map_schema(discovery, request)
            plan = self._query_planner.plan(discovery, request)

            ctx.proposal = FieldProposal(
                catalog=catalog,
                database=database or None,
                bucket=bucket or None,
                graph_name=graph_name or None,
                node_queries=mapping.node_queries or None,
                edge_queries=mapping.edge_queries or None,
                graph_queries=plan.graph_queries or None,
            )
            return (
                f"Proposed an import: {len(mapping.node_queries)} node and "
                f"{len(mapping.edge_queries)} edge query(ies)."
            )

        def suggest_page_actions(request: str) -> str:
            """Surface actions available on the current page for the request."""
            if ctx.page_context is None:
                return "No page context is available for this turn."
            actions = self._page_action.suggest(ctx.page_context, request)
            ctx.actions.extend(actions)
            return f"Surfaced {len(actions)} page action(s)."

        return [navigate, list_catalogs, generate_import, suggest_page_actions]

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
