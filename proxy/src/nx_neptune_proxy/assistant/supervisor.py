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
from nx_neptune_proxy.assistant.athena_tools import validate_bucket as _validate_bucket
from nx_neptune_proxy.assistant.athena_tools import (
    validate_sql_queries as _validate_sql_queries,
)
from nx_neptune_proxy.assistant.athena_tools import list_catalogs as _list_catalogs
from nx_neptune_proxy.assistant.athena_tools import list_databases as _list_databases
from nx_neptune_proxy.assistant.athena_tools import get_schema as _get_schema
from nx_neptune_proxy.assistant.debug_trace import (
    log_history,
    log_invocation,
    log_result,
)
from nx_neptune_proxy.assistant.graph_tools import (
    fetch_graph_schema,
    validate_opencypher,
)
from nx_neptune_proxy.assistant.schemas import (
    AssistantReply,
    ChatAction,
    FieldProposal,
    JumpAction,
    PageContext,
    SqlMappingResult,
    SqlQuery,
)
from nx_neptune_proxy.assistant.session import Session, SessionStore
from nx_neptune_proxy.assistant.skills import CAPABILITY_HINTS

logger = logging.getLogger(__name__)


def _query_bullets(queries) -> str:
    """Render each query's plain-language description as a bulleted block so the
    supervisor can relay per-query intent (not just an overall summary). Queries
    without a description are skipped; returns "" when none have one."""
    lines = [f"- {q.description}" for q in queries if getattr(q, "description", None)]
    return "\n".join(lines)


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


# Field-fill guidance: how to help a user choose the value for a single Import
# form field (e.g. the S3 staging bucket) without building the whole mapping.
# Kept as its own block so it can grow independently and be concatenated under
# the "Help fill individual import-form fields" job (see SUPERVISOR_SYSTEM_PROMPT).
FIELD_FILL_GUIDANCE = """
Assist the user in choosing the right value for a single field on the import
form, without building the whole mapping. This job is field-level assistance,
not import generation — do NOT call generate_import just to set one field.

## Choosing the S3 staging bucket
The staging bucket is the working area Athena writes query results to before the
graph import reads them (per projection). Help the user land on a good one with
this flow:

1. LIST. Call list_buckets() to get the real buckets (region-filtered — the same
   list the import page's selector shows). Never invent a bucket name.
2. RECOMMEND a few. Present the real options and suggest a sensible default,
   using these hints (state briefly why you suggest it):
   - Prefer the configured staging bucket if it appears in the list — it is the
     one the rest of the app already stages into, so it is the safest default.
   - A bucket whose name clearly matches the app/staging purpose is a weak hint,
     not a rule. (Note: the "nxp-" prefix is a graph-name convention, NOT a
     bucket-naming rule — do not recommend a bucket just because it starts with
     nxp-.)
   - Do NOT use region as a tiebreaker: the list is already region-filtered, so
     every option is in-region.
   - If nothing stands out, just name the real options plainly and let the user
     choose.
3. USER PICKS. Let the user choose one bucket. Their pick IS the confirmation —
   do not ask again whether to set it.
4. VALIDATE the pick. As soon as the user picks, call validate_bucket(bucket) on
   that one bucket (this mirrors pressing Validate on their choice).
5. SET (no second confirmation). If validation passes, immediately call
   update_import_fields(bucket=<picked>) to set the form's bucket field, then
   reply confirming it's set. Do NOT ask "should I set this?" first — the pick
   already told you to. NEVER say you set the bucket unless you actually called
   update_import_fields — claiming it without the tool call leaves the form
   unchanged. If validation FAILS, report the failed check and let the user pick
   again — do not set a bucket that failed.
"""


# SQL-validation guidance: how to check the node/edge Athena SQL already on the
# import form, and apply a fix when a query is wrong. Validation is read-only and
# never builds/runs the import; the fix tool edits a single query in place. Kept
# as its own block so it can grow independently and be concatenated under the
# "Validate and fix the import SQL" job (see SUPERVISOR_SYSTEM_PROMPT).
SQL_VALIDATION_GUIDANCE = """
- validate_sql_queries(): validate the node/edge Athena SQL currently on the
  import form (runs each with LIMIT 0 against the staging bucket and checks the
  required columns — ~id for nodes, ~from/~to for edges). Call this when the user
  asks to validate/verify/check their SQL (node/edge) queries. It reads the
  queries, catalog, database, and bucket from page context — you do not pass them
  in. This is a read-only check: it never builds or runs the import. If the form
  has no staging bucket, it refuses and explains why; relay that rather than
  claiming the SQL was checked.
- update_sql_queries(query_type, index, sql): apply a corrected node/edge query
  to the form. Finding a bug is not enough — when the user asks you to FIX a
  failing query, you MUST call this with the corrected SQL, or the form stays
  unchanged. query_type is "node"/"edge" and index is the 1-based position
  validate_sql_queries reported (e.g. "node query 1" -> index 1). Only that one
  query changes; the others are preserved. Never say you fixed a query unless you
  called this tool. After applying a fix, ALWAYS call validate_sql_queries again
  in the same turn to confirm the correction actually validates, and report that
  result — do not tell the user it is fixed/valid unless the re-check passed. If
  it still fails, relay the new error and try again rather than claiming success.
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
""" + FIELD_FILL_GUIDANCE + """

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
- propose_queries(request): propose openCypher queries to run against the graph \
that already exists, WITHOUT rebuilding the import. Use this instead of \
generate_import when the user wants to query/explore/analyze a graph and the \
page context shows one is already set up (a projection_id, a graph_status, or \
node/edge queries are present). Do NOT re-run generate_import just to get \
queries when the import already exists.
- validate_graph_queries(queries): validate openCypher syntax against the live \
graph via Neptune Analytics EXPLAIN (read-only — it does not run the queries). \
Call this only when the user explicitly asks to validate/verify/check openCypher, \
passing the exact query strings. It needs a live, available graph (graph_status \
"complete" / a graph_id present); if none exists, it says so — relay that rather \
than pretending the queries were checked.

### 4. Validate and fix the import SQL
""" + SQL_VALIDATION_GUIDANCE + """

### 5. Page navigation (move around the app)
- navigate(request): propose cross-page navigation (e.g. "start a new import", \
"open the TPCH projections").
- suggest_page_actions(request): surface actions available on the current page \
(e.g. stopping a graph, executing an import).

## Routing rules
- A pure navigation request must not trigger import generation, and vice versa.
- A single-field question (like which bucket to stage to) is job 2, not job 3 — \
do not run generate_import to answer it.
- A validate/verify/check request for the SQL is job 4 (validate_sql_queries), \
never job 3 — do not run generate_import to validate existing queries.
- Prefer the lightest job that answers the request; escalate to generate_import \
only on a clear intent to build the import.
- Keep your final reply short: the proposed jumps, form fields, and actions are \
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

        def validate_bucket(bucket: str) -> str:
            """Validate ONE picked S3 bucket, like pressing Validate on it.

            Run this only after the user has picked a specific bucket (step 4 of
            the field-fill flow) — not on your recommendations. It checks that
            the bucket exists/is accessible and (when a region is configured) is
            in the expected region, reusing the same checks as the UI's Validate
            button. Report the result; only set the bucket field after it passes
            and the user confirms."""
            checks = _validate_bucket(bucket)
            failed = [c for c in checks if not c["passed"]]
            if not failed:
                return f"Bucket '{bucket}' is valid: " + "; ".join(
                    c["message"] for c in checks
                )
            return f"Bucket '{bucket}' failed validation: " + "; ".join(
                f'{c["check"]}: {c["message"]}' for c in failed
            )

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
            # Relay the agents' plain-language intent — the overall model plus a
            # per-query purpose — so the supervisor passes it on to the user
            # rather than just the query counts.
            summary = (
                f"Proposed an import: {len(mapping.node_queries)} node and "
                f"{len(mapping.edge_queries)} edge query(ies)."
            )
            if mapping.description:
                summary += f"\nGraph model: {mapping.description}"
            node_bullets = _query_bullets(mapping.node_queries)
            if node_bullets:
                summary += f"\nNode queries:\n{node_bullets}"
            edge_bullets = _query_bullets(mapping.edge_queries)
            if edge_bullets:
                summary += f"\nEdge queries:\n{edge_bullets}"
            if plan.graph_queries:
                if plan.description:
                    summary += f"\nSuggested queries: {plan.description}"
                query_bullets = _query_bullets(plan.graph_queries)
                if query_bullets:
                    summary += f"\n{query_bullets}"
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
            # When the page can run queries (the Details page), offer each
            # proposed query as an inline "Run Query" button so the user can
            # execute it straight from the chat (spec §9.3).
            if pc and pc.can_run_queries:
                for q in plan.graph_queries:
                    label = q.description or q.cypher
                    if len(label) > 60:
                        label = label[:57] + "…"
                    ctx.actions.append(
                        ChatAction(
                            kind="run-query",
                            page=pc.page,
                            label=f"Run: {label}",
                            query=q.cypher,
                        )
                    )
            # Relay the planner's intent — overall summary plus each query's
            # purpose — so the user hears what the queries accomplish, not just
            # how many there are.
            summary = f"Proposed {len(plan.graph_queries)} openCypher query(ies)."
            if plan.description:
                summary += f" {plan.description}"
            query_bullets = _query_bullets(plan.graph_queries)
            if query_bullets:
                summary += f"\n{query_bullets}"
            return summary

        def validate_graph_queries(queries: list[str]) -> str:
            """Validate openCypher queries against the live graph using Neptune
            Analytics EXPLAIN (read-only — plans each query without running it).

            Use when the user asks to validate / verify / check / confirm the
            syntax of openCypher — whether queries just proposed by
            propose_queries or ones the user provided. Pass the exact query
            strings to check.

            Requires a live graph: EXPLAIN always targets a graphIdentifier, so
            the page context must carry a graph_id and the graph must be
            available (import complete). Returns a per-query valid/invalid
            verdict; invalid queries include the engine's error message (bad
            syntax, unknown procedure, or bad algorithm parameter)."""
            pc = ctx.page_context
            graph_id = pc.graph_id if pc else None
            if not graph_id or not pc.graph_available:
                return (
                    "Cannot validate: openCypher validation runs EXPLAIN against "
                    "a live Neptune Analytics graph, so it needs an imported, "
                    "available graph. This page has no live graph yet — validation "
                    "is only possible once the import is complete."
                )
            checked = [q for q in queries if q and q.strip()]
            if not checked:
                return "No openCypher queries to validate."
            lines: list[str] = []
            all_valid = True
            for q in checked:
                valid, err = validate_opencypher(graph_id, q)
                if valid:
                    lines.append(f"✓ valid: {q}")
                else:
                    all_valid = False
                    lines.append(f"✗ invalid: {q}\n    {err}")
            header = (
                "All queries are valid."
                if all_valid
                else "Some queries failed validation — see the errors below."
            )
            return header + "\n" + "\n".join(lines)

        def validate_sql_queries() -> str:
            """Validate the Import form's node/edge Athena SQL, like pressing the
            page's Validate Query button.

            Reads the node/edge SQL, catalog, database, and staging bucket
            straight from the current page context — you do NOT pass the queries
            in. Each query is run with LIMIT 0 against the staging bucket and
            checked for the required output columns (``~id`` for nodes;
            ``~from``/``~to`` for edges).

            Use when the user asks to validate / verify / check their SQL (node
            or edge) queries on the import form. REFUSE (return the message
            explaining why) when the form has no staging bucket set — validation
            writes results to that bucket, so it cannot run without one. Also
            report plainly when there are no SQL queries on the form yet.
            Returns a per-query valid/invalid verdict with the engine's error
            message for any that fail."""
            pc = ctx.page_context
            bucket = (pc.s3_staging_bucket if pc else None) or ""
            if not bucket.strip():
                return (
                    "I can't validate the SQL queries: the import form has no S3 "
                    "staging bucket set. Validation runs each query against that "
                    "bucket, so pick/set a staging bucket first, then ask again."
                )

            node_queries = list(pc.node_queries) if pc else []
            edge_queries = list(pc.edge_queries) if pc else []
            labeled = [
                (f"node query {i + 1}", q.sql, "node")
                for i, q in enumerate(node_queries)
                if q.sql and q.sql.strip()
            ] + [
                (f"edge query {i + 1}", q.sql, "edge")
                for i, q in enumerate(edge_queries)
                if q.sql and q.sql.strip()
            ]
            if not labeled:
                return "There are no node or edge SQL queries on the form to validate."

            catalog = (pc.catalog if pc else None) or "AwsDataCatalog"
            database = (pc.database if pc else None) or ""
            checks = _validate_sql_queries(labeled, catalog, database, bucket)
            failed = [c for c in checks if not c["passed"]]
            if not failed:
                return "All SQL queries are valid: " + "; ".join(
                    f'{c["check"]}: {c["message"]}' for c in checks
                )
            return "Some SQL queries failed validation: " + "; ".join(
                f'{c["check"]}: {c["message"]}' for c in failed
            )

        def update_sql_queries(query_type: str, index: int, sql: str) -> str:
            """Apply a corrected node/edge SQL query to the import form.

            Use this to actually FIX a query on the form — e.g. after
            validate_sql_queries reports a syntax error, call this with the
            corrected SQL so the form is updated (finding the bug is not enough;
            you MUST call this to change the form). Do NOT claim you fixed a
            query unless you called this tool.

            - query_type: "node" or "edge" — which list the query is in.
            - index: 1-based position within that list (the same numbering
              validate_sql_queries reports, e.g. "node query 1" -> index 1).
            - sql: the full corrected SQL for that one query.

            Only the single targeted query changes; every other node/edge query
            (and its description) on the form is preserved. Read the current
            queries from page context — this replaces just the one at ``index``."""
            qtype = (query_type or "").strip().lower()
            if qtype not in ("node", "edge"):
                return 'query_type must be "node" or "edge".'
            if not sql or not sql.strip():
                return "No SQL provided; pass the full corrected query."

            pc = ctx.page_context
            nodes = list(pc.node_queries) if pc else []
            edges = list(pc.edge_queries) if pc else []
            target = nodes if qtype == "node" else edges
            # 1-based index mirrors what validate_sql_queries reports.
            pos = index - 1
            if pos < 0 or pos >= len(target):
                return (
                    f"There is no {qtype} query {index} on the form "
                    f"(it has {len(target)} {qtype} query(ies))."
                )
            # Preserve the query's description; only the SQL text changes.
            target[pos] = SqlQuery(sql=sql, description=target[pos].description)

            # Reflect the fix in the page-context snapshot too, so a follow-up
            # validate_sql_queries call *in this same turn* checks the corrected
            # SQL rather than the stale text (validation reads from page
            # context, while the client applies changes from ctx.proposal).
            if pc is not None:
                if qtype == "node":
                    pc.node_queries = nodes
                else:
                    pc.edge_queries = edges

            # Merge onto any proposal already built this turn; send the FULL
            # node/edge lists because the client replaces each array wholesale
            # (a partial list would drop the other queries).
            base = ctx.proposal.model_dump() if ctx.proposal else {}
            base["node_queries"] = [q.model_dump() for q in nodes] or None
            base["edge_queries"] = [q.model_dump() for q in edges] or None
            ctx.proposal = FieldProposal(**base)
            return f"Updated {qtype} query {index} on the form."

        def update_import_fields(
            bucket: str = "",
            graph_name: str = "",
            database: str = "",
        ) -> str:
            """Set individual Import form fields WITHOUT rebuilding the mapping.

            This is how you actually APPLY a single-field choice to the form —
            e.g. step 5 of the bucket flow: after the user picks and confirms a
            staging bucket, call update_import_fields(bucket=...) to set it. Pass
            ONLY the fields the user confirmed; the rest are left untouched so
            any node/edge/graph queries already on the form are preserved. Do not
            claim a field is set unless you called this tool."""
            updates = {
                k: v
                for k, v in (
                    ("bucket", bucket),
                    ("graph_name", graph_name),
                    ("database", database),
                )
                if v
            }
            if not updates:
                return "No fields to update; specify a bucket, graph_name, or database."
            # Merge onto any proposal already built this turn so setting one
            # field never clobbers previously proposed queries.
            base = ctx.proposal.model_dump() if ctx.proposal else {}
            base.update(updates)
            ctx.proposal = FieldProposal(**base)
            return f"Set import field(s): {', '.join(sorted(updates))}."

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
            validate_bucket,
            generate_import,
            propose_queries,
            validate_graph_queries,
            validate_sql_queries,
            update_sql_queries,
            update_import_fields,
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
