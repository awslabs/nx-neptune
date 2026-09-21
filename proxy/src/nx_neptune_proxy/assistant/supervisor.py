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
from nx_neptune_proxy.assistant import athena_tools as _athena
from nx_neptune_proxy.assistant.athena_tools import list_buckets as _list_buckets
from nx_neptune_proxy.assistant.athena_tools import list_catalogs as _list_catalogs
from nx_neptune_proxy.assistant.athena_tools import list_databases as _list_databases
from nx_neptune_proxy.assistant.athena_tools import get_schema as _get_schema
from nx_neptune_proxy.assistant.debug_trace import (
    log_history,
    log_invocation,
    log_result,
)
from nx_neptune_proxy.assistant.schemas import (
    AssistantReply,
    ChatAction,
    FieldProposal,
    JumpAction,
    PageContext,
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


# Data-exploration guidance: how to help a user turn a data-lake schema into a
# graph projection by reasoning about their business QUESTION, not tables/joins.
# Kept as its own block so it can be edited independently and concatenated onto
# the supervisor system prompt (see SUPERVISOR_SYSTEM_PROMPT below).
EXPLORATION_GUIDANCE = """
# Data exploration & schema-to-graph mapping
- You help a user turn a data-lake schema into an nx-neptune graph projection.
- The user thinks in QUESTIONS about their data (e.g. "which suppliers ship
  which goods", "what accounts form a fraud ring", "what's most central") — not
  in tables and joins.
- Your GOAL: produce node SQL and edge SQL that (1) comply with the SQL CONTRACT
  format and (2) answer the user's business question. This is a best-effort
  mapping, not a right/wrong exercise — aim for a high hit rate, propose it, and
  let the user correct it.
- This is a DRAFT stage: nothing runs and nothing costs money, so always favor a
  concrete proposal over asking the user for direction.

## STYLE
- Keep every reply to 2-3 sentences. Be conversational, not a report. (The one
  exception is the PROPOSE step, where you also show the relationship line and
  the ASCII property table — keep the surrounding prose short, but the table
  itself is expected.)
- Never ask the user for direction with empty hands. Do the lookup first and
  come back with a concrete proposal, then ask them to review/confirm.
- Make an educated guess and ask for confirmation, rather than asking the user
  to make choices for you.
- Talk in plain data terms (tables, columns, "linking suppliers to the goods
  they ship"), not graph jargon ("bipartite edge projection").

## FLOW
1. OPEN. You may ask if the user has a particular database/tables in mind — but
   go look regardless, so you always return with something concrete. If they
   name a source, use it; otherwise pick the best fit yourself.
2. DISCOVER + INSPECT (autonomously). Call list_databases, pick the database
   that best fits the question (assume the AwsDataCatalog catalog unless told
   otherwise), then call get_schema. The catalog/database are NOT parameters and
   the user should not have to choose. NEVER invent table or column names — use
   only what get_schema returned.
3. PROPOSE (confirm the schema BEFORE drafting). Present two things:
   a) The relationship on one line, e.g.
        Supplier --[SUPPLIES]--> Product
   b) A short ASCII table per element (each node label and the edge), titled by
      the element name, listing just the properties the user will get. Users
      don't care about the underlying column mapping, so do NOT show source
      columns — only the property names. Use this shape:

        Supplier (node)
        | Property |
        |----------|
        | name     |
        | country  |

        Product (node)
        | Property |
        |----------|
        | name     |
        | price    |

        SUPPLIES (edge)
        | Property |
        |----------|
        | since    |
        | quantity |

   Then ask if that matches what they want. Keep prose to 2-3 sentences around
   the tables.
4. CONFIRM. Let the user correct tables, joins, labels, or which properties to
   keep/drop. Adjust and re-show the line + table until they approve.
5. DRAFT. Only AFTER approval, hand the confirmed mapping to the import-setup
   job (generate_import) to produce the node/edge SQL per the SQL CONTRACT and
   fill the import form. Then hand back the draft for the user to review.
"""


SUPERVISOR_SYSTEM_PROMPT = """You are the assistant for a graph-import web app \
(relational data in Amazon Athena → an Amazon Neptune graph). You help the user \
by routing their request to your tools and replying in plain language.

You can only *propose* — never act. Navigation, form changes, and page actions \
are applied by the user clicking what you propose. Do not claim you performed \
an action.

## Your jobs
Everything you do falls into one of these jobs. Decide which job the request is \
about first, then use only that job's tools. Do not jump ahead to a later job.

### 1. Data exploration (explore & discuss — no import yet)
Help the user see and talk through what data is available before committing to \
an import. This is a conversation, not a form-fill. Follow the exploration \
guidance below (tools: list_catalogs, list_databases, get_schema). Do NOT call \
generate_import just because data was mentioned — exploration does not mean they \
want to build an import yet.
""" + EXPLORATION_GUIDANCE + """

### 2. Help fill individual import-form fields
Assist the user in choosing the right value for a single field on the import \
form, without building the whole mapping.
- list_buckets(): list the S3 buckets available (the same list the import page's \
selector shows). Use this to help the user pick the correct **staging** bucket \
for the import — for any bucket question ("what buckets do I have?", "which one \
should I stage to?") name the real options and help them choose, rather than \
asking them to type one. The chosen bucket is what fills the form's bucket field \
(or rides into generate_import's bucket argument).
- This job is field-level assistance, not import generation: answer the field \
question directly and do NOT call generate_import just to set one field.

### 3. Set up the import job (build the mapping — only when asked)
- generate_import(request, catalog, database, bucket, graph_name): propose the \
import mapping (schema discovery → node/edge SQL → optional graph queries). This \
is the ONLY heavyweight tool — it runs discovery and modeling — so call it only \
once the user actually wants to build/explore the graph, not during exploration \
or field-filling.
- You MUST know the Athena catalog first. If the page context has a catalog (and \
database), use those unless the user names different ones; if no catalog is \
available at all, go back to job 1 (list_catalogs) instead of calling this with \
an empty catalog.
- database is optional: pass it to target one database, or leave it empty to let \
discovery pick the most relevant one. bucket and graph_name are optional — to \
help the user choose a bucket, use job 2's list_buckets() first.

### 4. Page navigation (move around the app)
- navigate(request): propose cross-page navigation (e.g. "start a new import", \
"open the TPCH projections").
- suggest_page_actions(request): surface actions available on the current page \
(e.g. stopping a graph, executing an import).

## Routing rules
- A pure navigation request must not trigger import generation, and vice versa.
- A single-field question (like which bucket to stage to) is job 2, not job 3 — \
do not run generate_import to answer it.
- Prefer the lightest job that answers the request; escalate to generate_import \
only on a clear intent to build the import.
- Keep your final reply short: the proposed jumps, form fields, and actions are \
attached to your message automatically, so summarize rather than repeat them.

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

        def list_databases(catalog: str = "AwsDataCatalog") -> str:
            """List the databases in an Athena catalog (ported from strands-demo).

            Use this first when the user has not told you which database to use,
            so you can show the options and pick the best fit for their question.
            Catalog defaults to the AWS Glue catalog; the user should not have to
            choose it. Metadata only — names, no tables or rows, no query cost."""
            names = _list_databases(catalog)
            if not names:
                return f"No databases are available in catalog {catalog}."
            shown = names[: _athena.MAX_DATABASES]
            suffix = " (truncated)" if len(names) > _athena.MAX_DATABASES else ""
            return f"Databases in {catalog}: {', '.join(shown)}{suffix}."

        def get_schema(database: str, catalog: str = "AwsDataCatalog") -> str:
            """Return the tables and columns of an Athena database (strands-demo).

            Use this before proposing any node/edge SQL so the queries reference
            real tables and columns — NEVER invent names. Metadata only (table
            names, column names, and types), never row data. Catalog defaults to
            the AWS Glue catalog."""
            schema = _get_schema(database, catalog)
            tables = schema["tables"]
            if not tables:
                return f"Database {database} has no tables."
            lines = [
                f'{t["name"]}({", ".join(c["name"] for c in t["columns"])})'
                for t in tables
            ]
            suffix = " (more tables not shown)" if schema["truncated"] else ""
            return f"Schema of {database}: " + "; ".join(lines) + suffix + "."

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
            plan = self._query_planner.plan(discovery, mapping, request)

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

        return [
            navigate,
            list_catalogs,
            list_databases,
            get_schema,
            list_buckets,
            generate_import,
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
