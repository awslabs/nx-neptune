# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pydantic contracts for the AI assistant agent pipeline (spec §9).

These are the JSON contracts between the supervisor, the specialist agents,
and the client. Field naming is snake_case to match the rest of the proxy API
(the TS layer consumes snake_case directly — see ``api/index.ts``); the client
maps the reply into its own ``JumpAction``/``ChatAction`` shapes (Group G).
"""

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field

# --- Shared query shapes --------------------------------------------------


class SqlQuery(BaseModel):
    """A single Athena SQL statement (node or edge query)."""

    sql: str


class CypherQuery(BaseModel):
    """A single openCypher statement for the post-import Graph Queries."""

    cypher: str


# --- Schema Discovery agent (§9.5 / §9.6) ---------------------------------


class ColumnInfo(BaseModel):
    name: str
    type: str


class DiscoveredTable(BaseModel):
    name: str
    # The database this table lives in. Set when discovery spans every database
    # in a catalog (database omitted from generate_import); lets the SQL Mapping
    # agent qualify names unambiguously. None for single-database discovery.
    database: Optional[str] = None
    columns: list[ColumnInfo] = Field(default_factory=list)
    # Populated only when the optional, guarded sampling tool ran (§9.6).
    # Untrusted data — used purely as mapping context, never executed.
    sample_rows: Optional[list[dict[str, Any]]] = None


class DiscoveryResult(BaseModel):
    tables: list[DiscoveredTable] = Field(default_factory=list)


# --- SQL Mapping agent (§9.5) ---------------------------------------------


class SqlMappingResult(BaseModel):
    node_queries: list[SqlQuery] = Field(default_factory=list)
    edge_queries: list[SqlQuery] = Field(default_factory=list)


# --- Query Planner agent (§9.5) -------------------------------------------


class QueryPlanResult(BaseModel):
    graph_queries: list[CypherQuery] = Field(default_factory=list)


# --- Import proposal (merged generate_import output, §9.5) -----------------


class FieldProposal(BaseModel):
    """Fields the assistant proposes for the Import form.

    Only the fields an agent actually produced are set, so a follow-up turn can
    update just the graph queries without clobbering the SQL (spec §9.5).
    """

    catalog: Optional[str] = None
    database: Optional[str] = None
    bucket: Optional[str] = None
    graph_name: Optional[str] = None
    node_queries: Optional[list[SqlQuery]] = None
    edge_queries: Optional[list[SqlQuery]] = None
    graph_queries: Optional[list[CypherQuery]] = None


# --- Navigation agent output (§9.5 / §9.7) --------------------------------


class JumpAction(BaseModel):
    """A cross-page navigation the client executes via ``runJump``.

    ``project_id`` is used by ``new-import`` (optional) and ``open-projections``;
    ``new-project`` ignores it.
    """

    kind: Literal["new-import", "new-project", "open-projections"]
    label: str
    project_id: Optional[str] = None


# --- Page-Action agent output (§9.5) --------------------------------------


class ChatAction(BaseModel):
    """A page/graph action the client executes via ``runChatAction``.

    ``action_key``/``enabled`` apply to ``page-action``; ``graph_id``/
    ``graph_action`` apply to ``graph-action``. Constrained to keys/targets
    present in the request's page context (§9.4).
    """

    kind: Literal["page-action", "graph-action"]
    page: str
    label: str
    action_key: Optional[str] = None
    enabled: Optional[bool] = None
    destructive: Optional[bool] = None
    graph_id: Optional[str] = None
    graph_action: Optional[str] = None


# --- Page context (agent input, §9.4) -------------------------------------


class PageContextAction(BaseModel):
    key: str
    label: str
    enabled: bool = True


class PageContextGraphTarget(BaseModel):
    id: str
    name: str
    actions: list[str] = Field(default_factory=list)


class PageContext(BaseModel):
    """Snapshot of the active PageBridge sent with each turn (spec §9.4).

    The Navigation and Page-Action agents may reference only the keys/targets
    present here — the assistant never invents an action a page didn't register.
    """

    page: str
    project_id: Optional[str] = None
    # The Import page's currently-selected Athena catalog/database, so the
    # supervisor can call generate_import without asking the user to re-state
    # what the form already shows. None on pages that have no such selection.
    catalog: Optional[str] = None
    database: Optional[str] = None
    actions: list[PageContextAction] = Field(default_factory=list)
    graph_targets: list[PageContextGraphTarget] = Field(default_factory=list)


# --- Assembled reply (§9.8) -----------------------------------------------


class AssistantReply(BaseModel):
    """What ``POST /assistant/message`` returns; maps 1:1 onto the client's
    ``ChatMessage`` (spec §9.8)."""

    text: str
    proposal: Optional[FieldProposal] = None
    jumps: list[JumpAction] = Field(default_factory=list)
    actions: list[ChatAction] = Field(default_factory=list)
    # Set instead of a proposal when an agent needs to ask before proceeding.
    question: Optional[str] = None
