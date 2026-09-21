# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Query Planner agent (spec §9.5, Req 5).

Proposes post-import openCypher queries the user might run against the graph.
Grounded in the always-on capability catalog (§9.13) with an on-demand
``neptune_skill_reference`` tool for deep dives. openCypher is validated at run
time by the Phase 1 endpoint. Returns an empty list when no query is warranted.
"""

from nx_neptune_proxy.assistant.agents.base_specialist import (
    SpecialistAgent,
    as_tools,
)
from nx_neptune_proxy.assistant.schemas import (
    DiscoveryResult,
    QueryPlanResult,
    SqlMappingResult,
)
from nx_neptune_proxy.assistant.skills import CAPABILITY_CATALOG, load_neptune_skill

SYSTEM_PROMPT = (
    """You suggest openCypher queries to run against a Neptune \
graph once it has been imported from the described schema.

The graph's shape is fixed by the node/edge import queries you are given — \
those SELECTs define the exact node labels ("~label" on a node query), node \
properties (the other projected columns), edge types ("~label" on an edge \
query), and how nodes connect ("~from"/"~to"). Your openCypher MUST match that \
model exactly: use only those labels, property names, and edge types, with the \
same spelling and case. Do NOT invent labels or properties from the raw schema \
that the import queries did not create.

Given that graph model and the user's intent, propose read-only openCypher \
queries that answer what the user wants to explore. Use the capability catalog \
below to ground your proposals in what a graph actually unlocks and which \
Neptune Analytics algorithms the backend exposes — do not propose procedures \
listed there as unavailable. Only propose an algorithm call when the request \
calls for that kind of analysis. When you need exact openCypher idioms or a \
deeper use-case example, call the neptune_skill_reference tool. If the request \
does not call for any post-import query, return an empty list.

"""
    + CAPABILITY_CATALOG
    + """
# OUTPUT REQUIREMENTS
Return ONLY JSON, no prose, no Markdown, with this exact structure:
{"graph_queries": [{"cypher": "MATCH (n) RETURN n LIMIT 10"}]}
Use {"graph_queries": []} when no query is warranted. Each "cypher" value \
must be a single line.
"""
)

USER_PROMPT = """# Schema
{schema}

# Import queries (define the graph model)
{mapping}

# Request
{request}
"""


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
            schema=kwargs["schema"],
            mapping=kwargs["mapping"],
            request=kwargs["request"],
        )

    def plan(
        self,
        discovery: DiscoveryResult,
        mapping: SqlMappingResult,
        request: str,
    ) -> QueryPlanResult:
        """Propose post-import openCypher. ``mapping`` (the node/edge import
        queries) is the source of truth for the graph model, so the planner
        targets the same labels/properties/edges the SQL Mapping agent chose."""
        schema = discovery.model_dump_json(exclude_none=True)
        raw = self.execute_task(
            schema=schema,
            mapping=mapping.model_dump_json(exclude_none=True),
            request=request,
        )
        return self.extract_json(raw, QueryPlanResult)
