# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Query Planner agent (spec §9.5, Req 5).

Proposes post-import openCypher queries the user might run against the graph.
Grounded in the always-on capability catalog (§9.13) with an on-demand
``neptune_skill_reference`` tool for deep dives. openCypher is validated at run
time by the Phase 1 endpoint. Returns an empty list when no query is warranted.
"""

from typing import Optional

from nx_neptune_proxy.assistant.agents.base_specialist import (
    SpecialistAgent,
    as_tools,
)
from nx_neptune_proxy.assistant.schemas import (
    DiscoveryResult,
    GraphSchema,
    QueryPlanResult,
    SqlMappingResult,
)
from nx_neptune_proxy.assistant.skills import CAPABILITY_CATALOG, load_neptune_skill

SYSTEM_PROMPT = (
    """You suggest read-only openCypher queries to run against a Neptune graph. \
The "Data model" section describes the graph's shape and comes in one of two \
kinds, labeled inline — read that label and ground your queries accordingly:

- LIVE GRAPH SCHEMA: the graph is already imported and live. This is the actual \
schema read from the database, so it is AUTHORITATIVE — use exactly the node \
labels, edge types, and property names it lists, with the same spelling and \
case. Do NOT invent labels or properties it does not contain.
- PREDICTED MODEL: the graph is not yet imported. Its shape is fixed by the \
node/edge import queries — the SELECTs define node labels ("~label" on a node \
query), node properties (the other projected columns), edge types ("~label" on \
an edge query), and how nodes connect ("~from"/"~to"). Your openCypher MUST \
match that model exactly; do NOT invent labels or properties the import queries \
did not create.

Either way the described graph EXISTS or will exist with that exact shape — the \
model is what you query against, NOT a signal that the work is done. Having a \
data model is a reason to propose queries, never a reason to decline.

Given that graph model and the user's intent, propose read-only openCypher \
queries that answer what the user wants to explore. Use the capability catalog \
below to ground your proposals in what a graph actually unlocks and which \
Neptune Analytics algorithms the backend exposes — do not propose procedures \
listed there as unavailable. Only propose an algorithm call when the request \
calls for that kind of analysis.

Default to proposing at least one useful query. When the request names \
something concrete to explore or analyze, answer it directly. When the request \
is a general ask (e.g. "suggest a query", "what can I do with this graph?") or \
gives little detail, propose a sensible starter query against the given model — \
for example, sampling nodes of a label with ``MATCH (n:Label) RETURN n LIMIT \
25`` or traversing an edge type. Only return an empty list when the request is \
genuinely not about querying the graph at all (e.g. pure navigation or import \
configuration). When you need exact openCypher idioms or a deeper use-case \
example, call the neptune_skill_reference tool.

"""
    + CAPABILITY_CATALOG
    + """
# OUTPUT REQUIREMENTS
Return ONLY JSON, no prose, no Markdown, with this exact structure:
{"description": "One plain-language sentence summarizing what this set of \
queries helps the user explore or analyze.", \
"graph_queries": [{"description": "What THIS query answers and what the user \
learns from its results (e.g. \\"finds the 10 most connected accounts\\").", \
"cypher": "MATCH (n) RETURN n LIMIT 10"}]}
Every query MUST include its own "description". Use {"graph_queries": [], \
"description": "..."} when no query is warranted, with the description \
explaining why. All descriptions are for the user, so write them for a \
non-expert and do not restate the raw openCypher. Each "cypher" value must be a \
single line.
"""
)

USER_PROMPT = """# Data model
{model}

# Request
{request}
"""

# Placeholder when no relational schema is available in the predicted-model path
# — the node/edge import queries below are then the only source of the model.
_NO_SCHEMA = "(not provided — derive the model from the node/edge import queries below)"


def neptune_skill_reference(topic: str) -> str:
    """Load the full text of a Neptune reference for a deep dive.

    Use when the capability catalog is insufficient — e.g. exact openCypher
    idioms or a fuller use-case example. ``topic`` must be one of: "querying",
    "use-cases", "graphrag", "data-modeling".
    """
    return load_neptune_skill(topic)


class QueryPlannerAgent(SpecialistAgent):
    NAME = "query_planner"
    SYSTEM_PROMPT = SYSTEM_PROMPT

    def _tools(self):
        # On-demand reference loader (§9.13). Wrapped as a Strands tool lazily,
        # so importing this module never requires strands-agents.
        return as_tools([neptune_skill_reference])

    def _format_prompt(self, **kwargs) -> str:
        return USER_PROMPT.format(
            model=kwargs["model"],
            request=kwargs["request"],
        )

    def plan(
        self,
        mapping: SqlMappingResult,
        request: str,
        discovery: Optional[DiscoveryResult] = None,
        graph_schema: Optional[GraphSchema] = None,
    ) -> QueryPlanResult:
        """Propose openCypher grounded on the graph's data model.

        The model has two sources, in priority order:

        - ``graph_schema`` — the LIVE schema read from an imported graph. When
          present and non-empty it is authoritative: the planner grounds on the
          real labels/edge types/properties and the SQL model is ignored.
        - ``mapping`` (+ optional ``discovery``) — the PREDICTED model from the
          node/edge import SELECTs, used pre-import when no graph exists yet.

        ``discovery`` is available during import generation and omitted when
        proposing against an existing graph. Either way a model is supplied, so
        queries can be proposed regardless of whether an import was just run."""
        if graph_schema is not None and not graph_schema.is_empty:
            model = (
                "LIVE GRAPH SCHEMA (authoritative — the graph is imported; these "
                "are the real labels, edge types, and properties):\n"
                + graph_schema.model_dump_json(exclude_none=True)
            )
        else:
            schema = (
                discovery.model_dump_json(exclude_none=True)
                if discovery
                else _NO_SCHEMA
            )
            model = (
                "PREDICTED MODEL (the graph is not yet imported; derive labels, "
                "properties, and edge types from the node/edge import queries):\n"
                f"## Relational schema\n{schema}\n"
                "## Node/edge import queries\n"
                + mapping.model_dump_json(exclude_none=True)
            )
        raw = self.execute_task(model=model, request=request)
        return self.extract_json(raw, QueryPlanResult)
