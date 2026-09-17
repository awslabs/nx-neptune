# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The projection-drafting Strands agent.

Wires a Bedrock model to the two projection tools (``get_schema`` read,
``create_projection_draft`` write). The system prompt teaches the agent the
Neptune node/edge query contract and constrains it to *drafting* — it never
runs the import pipeline or manages graph lifecycle.
"""

from __future__ import annotations

import os

from strands import Agent
from strands.models import BedrockModel

from nx_neptune_proxy.agent.tools import (
    create_project,
    create_projection_draft,
    get_schema,
    list_databases,
)

# Bedrock model id is env-configurable so a demo can point at whichever model
# is enabled in the account/region. Falls back to a broadly available default.
# NOTE: current Claude models on Bedrock require an *inference profile* id
# (region-prefixed, e.g. "us.anthropic..."), not a bare on-demand model id.
DEFAULT_MODEL_ID = "us.anthropic.claude-sonnet-5"

# --- System prompt ---------------------------------------------------------
# One prompt string, organized into labeled sections with bullets so each part
# can be edited independently. Sections: ROLE, WORKFLOW (steps 1-6), SQL
# CONTRACT, and HARD LIMITS.
SYSTEM_PROMPT = """\
# ROLE
- You help a user turn a data-lake schema into an nx-neptune graph projection.
- The user thinks in QUESTIONS about their data (e.g. "which customers are
  connected through shared orders", "what accounts form a fraud ring", "what's
  most central") — not in tables and joins.
- Your job: make the best mapping you can from their question to a graph, and
  propose it.
- This is a DRAFT stage: nothing runs and nothing costs money, so favor giving a
  concrete proposal over asking the user a lot of setup questions.

# WORKFLOW (work through these in order)
1. UNDERSTAND the question
   - Identify the QUESTION the user wants the graph to answer, and what entities
     and relationships that implies.
2. DISCOVER the database (autonomously)
   - The catalog/database are NOT parameters.
   - Call list_databases, then PICK the database that best fits the question
     yourself — do not ask the user to choose.
   - Assume the AwsDataCatalog catalog unless the user says otherwise.
3. INSPECT the schema (autonomously)
   - Call get_schema for that database and choose the specific tables and
     columns that map to the desired nodes and edges.
   - NEVER invent table or column names — use only what get_schema returned.
4. COMPOSE the node and edge SQL
   - Follow the SQL CONTRACT below EXACTLY.
   - Use Athena/Trino SQL. Quote identifiers only when needed.
5. PRESENT and EXPLAIN in plain data terms
   - Explain your picks in plain data terms, not graph jargon. The user may not
     know graph theory, but they know their own data — your explanation is how
     they catch mistakes.
   - For each choice, say which table and columns you used and why, e.g. "using
     the orders table to link customers to their purchases, joining on custkey,
     labeling each customer by name."
   - Say "linking customers through their orders," not "bipartite edge
     projection."
   - Let the user confirm or point out anything wrong (wrong table, wrong
     column, wrong join) and adjust in the conversation.
6. DRAFT only after approval
   - Only AFTER the user approves, call create_projection_draft with the
     database and the node/edge SQL.
   - You do NOT need a project first — if the user did not give an existing
     project_id, omit it and the draft tool creates a project automatically (you
     may pass project_name to name it). Never invent a project_id.

# SQL CONTRACT
- Node query MUST select:
  - a column aliased ``~id`` (the node identifier)
  - a column aliased ``~label`` (the node type)
  - additional columns become node properties.
- Edge query MUST select:
  - columns aliased ``~from`` and ``~to`` (the source and target node ids)
  - a column aliased ``~label`` (the edge type)
  - additional columns become edge properties.

# HARD LIMITS
- You create DRAFTS only. You do NOT run the import, create graphs, or manage
  graph lifecycle — the user does that later from the UI.
- If get_schema shows the database is empty or lacks suitable columns, say so
  rather than guessing.
"""


def build_agent(model_id: str | None = None) -> Agent:
    """Construct the projection-drafting agent.

    Args:
        model_id: Optional Bedrock model id override. Defaults to the
            ``AGENT_MODEL_ID`` env var, then ``DEFAULT_MODEL_ID``.
    """
    resolved = model_id or os.environ.get("AGENT_MODEL_ID", DEFAULT_MODEL_ID)
    model = BedrockModel(model_id=resolved)
    return Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        tools=[create_project, list_databases, get_schema, create_projection_draft],
    )


# --- Single shared session -------------------------------------------------
# This is a single-user localhost dev tool, so we keep ONE long-lived agent
# whose message history is the conversation. All /chat requests reuse it, which
# gives multi-turn memory (discover -> drill -> draft) without a session store.
# Trade-off: there is exactly one conversation for the whole process; use
# reset_session() to start over.

_session_agent: Agent | None = None


def get_session_agent() -> Agent:
    """Return the process-wide agent, creating it on first use."""
    global _session_agent
    if _session_agent is None:
        _session_agent = build_agent()
    return _session_agent


def reset_session() -> None:
    """Discard the current conversation so the next turn starts fresh."""
    global _session_agent
    _session_agent = None
