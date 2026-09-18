# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Query Planner agent (spec §9.5, Req 5).

Proposes post-import openCypher queries the user might run against the graph.
Pure LLM over discovery context; no tools. openCypher is validated at run time
by the Phase 1 endpoint. Returns an empty list when no query is warranted.
"""

from nx_neptune_proxy.assistant.agents.base_specialist import SpecialistAgent
from nx_neptune_proxy.assistant.schemas import DiscoveryResult, QueryPlanResult

SYSTEM_PROMPT = """You suggest openCypher queries to run against a Neptune \
graph once it has been imported from the described schema.

Given the schema and the user's intent, propose read-only openCypher queries \
that answer what the user wants to explore. Base labels/relationships on the \
schema. If the request does not call for any post-import query, return an \
empty list.

# OUTPUT REQUIREMENTS
Return ONLY JSON, no prose, no Markdown, with this exact structure:
{"graph_queries": [{"cypher": "MATCH (n) RETURN n LIMIT 10"}]}
Use {"graph_queries": []} when no query is warranted. Each "cypher" value \
must be a single line.
"""

USER_PROMPT = """# Schema
{schema}

# Request
{request}
"""


class QueryPlannerAgent(SpecialistAgent):
    NAME = "query_planner"
    SYSTEM_PROMPT = SYSTEM_PROMPT

    def _format_prompt(self, **kwargs) -> str:
        return USER_PROMPT.format(schema=kwargs["schema"], request=kwargs["request"])

    def plan(self, discovery: DiscoveryResult, request: str) -> QueryPlanResult:
        schema = discovery.model_dump_json(exclude_none=True)
        raw = self.execute_task(schema=schema, request=request)
        return self.extract_json(raw, QueryPlanResult)
