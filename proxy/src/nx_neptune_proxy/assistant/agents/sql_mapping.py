# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""SQL Mapping agent (spec §9.5).

Turns a discovered schema + the user's intent into the node/edge ``SELECT``
queries the import pipeline expects, aliasing the reserved graph columns
(``~id`` / ``~label`` / ``~from`` / ``~to``). Pure LLM over discovery context;
no tools. The SQL is untrusted output — validated at run time by the Phase 1
endpoint and only executed after the user accepts the proposal.
"""

from nx_neptune_proxy.assistant.agents.base_specialist import SpecialistAgent
from nx_neptune_proxy.assistant.schemas import DiscoveryResult, SqlMappingResult
from nx_neptune_proxy.assistant.skills import DATA_MODELING_GUIDANCE

SYSTEM_PROMPT = (
    """You map a relational schema to Amazon Neptune node/edge \
load queries for a graph import.

Given the schema and the user's intent, write Athena SQL SELECT statements:
- Node queries MUST alias the primary key as "~id" and may alias a label \
column as "~label".
- Edge queries MUST alias the source key as "~from" and the target key as \
"~to", and may alias a type as "~label".
- Alias reserved columns with double quotes exactly: AS "~id", AS "~from", etc.
- Use only tables and columns present in the provided schema.

Shape the model for graph traversal using the guidance below — the node/edge \
queries you emit ARE the graph model, so the labels, properties, and edges you \
choose here are what post-import queries will run against.

If the request is too ambiguous to map safely (e.g. unclear which tables \
become nodes vs. edges), do not guess.

"""
    + DATA_MODELING_GUIDANCE
    + """
# OUTPUT REQUIREMENTS
Return ONLY JSON, no prose, no Markdown, with this exact structure:
{"node_queries": [{"sql": "SELECT id AS \\"~id\\" FROM t"}], \
"edge_queries": [{"sql": "SELECT a AS \\"~from\\", b AS \\"~to\\" FROM e"}]}
Each "sql" value must be a single line.
"""
)

USER_PROMPT = """# Schema
{schema}

# Request
{request}
"""


class SqlMappingAgent(SpecialistAgent):
    NAME = "sql_mapping"
    SYSTEM_PROMPT = SYSTEM_PROMPT

    def _format_prompt(self, **kwargs) -> str:
        return USER_PROMPT.format(schema=kwargs["schema"], request=kwargs["request"])

    def map_schema(
        self, discovery: DiscoveryResult, request: str
    ) -> SqlMappingResult:
        schema = discovery.model_dump_json(exclude_none=True)
        raw = self.execute_task(schema=schema, request=request)
        return self.extract_json(raw, SqlMappingResult)
