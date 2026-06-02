import json
import logging

import boto3
from strands import Agent

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a SQL expert that generates Athena SQL projections for Neptune Analytics graph import.

## Neptune CSV Format Rules

**Vertex CSV** columns:
- `~id` (REQUIRED) — unique vertex identifier
- `~label` (recommended) — vertex type label

**Edge CSV** columns:
- `~id` (REQUIRED) — unique edge identifier
- `~from` (REQUIRED) — source vertex ID (must match a vertex ~id)
- `~to` (REQUIRED) — target vertex ID (must match a vertex ~id)
- `~label` (recommended) — edge type label

**Property columns** use `propertyname:type` header syntax. Supported types: Bool, Byte, Short, Int, Long, Float, Double, String, Date.

## Your Task

Generate exactly TWO Athena SQL SELECT statements:
1. A vertex projection — must alias columns to produce `~id` and `~label` headers, plus any properties
2. An edge projection — must alias columns to produce `~id`, `~from`, `~to`, and `~label` headers, plus any properties

Use SQL column aliases like: `column_name AS "~id"`, `'LabelValue' AS "~label"`

## Response Format

If you have enough information, respond with ONLY this JSON (no markdown, no explanation):
{"type": "sql", "vertexSql": "<vertex SELECT statement>", "edgeSql": "<edge SELECT statement>"}

If you need clarification from the user, respond with ONLY this JSON:
{"type": "question", "message": "<your question>"}

## Available Table Schemas

"""


def build_system_prompt(table_schemas: str) -> str:
    return SYSTEM_PROMPT + table_schemas


def fetch_table_schemas(region: str, catalog: str, database: str) -> str:
    """Fetch DESCRIBE TABLE results for all tables in the database."""
    athena = boto3.client("athena", region_name=region)
    resp = athena.list_table_metadata(CatalogName=catalog, DatabaseName=database)
    tables = resp.get("TableMetadataList", [])

    schema_parts = []
    for table in tables:
        name = table["Name"]
        columns = table.get("Columns", [])
        col_strs = [f"  {c['Name']} ({c['Type']})" for c in columns]
        schema_parts.append(f"Table: {name}\nColumns:\n" + "\n".join(col_strs))

    return "\n\n".join(schema_parts)


def generate_sql(
    region: str,
    catalog: str,
    database: str,
    model_id: str,
    user_message: str,
    conversation_history: list[dict] | None = None,
) -> dict:
    """Generate Neptune-compatible SQL via Strands Agent.

    Returns dict with either:
      {"type": "sql", "vertexSql": "...", "edgeSql": "..."}
      {"type": "question", "message": "..."}
    """
    table_schemas = fetch_table_schemas(region, catalog, database)
    system_prompt = build_system_prompt(table_schemas)

    logger.info("LLM model: %s", model_id)
    logger.info("System prompt:\n%s", system_prompt)
    logger.info("User message: %s", user_message)
    if conversation_history:
        logger.info("Conversation history: %s", conversation_history)

    agent = Agent(model=model_id, system_prompt=system_prompt)

    # Build messages: replay conversation history then send current message
    if conversation_history:
        for msg in conversation_history:
            agent.messages.append({"role": msg["role"], "content": [{"text": msg["content"]}]})

    result = agent(user_message)
    response_text = str(result).strip()

    logger.info("LLM raw response: %s", response_text)

    # Parse JSON from response (handle possible markdown fencing)
    if response_text.startswith("```"):
        lines = response_text.split("\n")
        response_text = "\n".join(lines[1:-1]).strip()

    try:
        parsed = json.loads(response_text)
    except json.JSONDecodeError:
        # If the model didn't return valid JSON, treat as a question
        logger.warning("LLM returned non-JSON response: %s", response_text[:200])
        return {"type": "question", "message": response_text}

    if parsed.get("type") == "sql":
        return {
            "type": "sql",
            "vertexSql": parsed.get("vertexSql", ""),
            "edgeSql": parsed.get("edgeSql", ""),
        }
    elif parsed.get("type") == "question":
        return {"type": "question", "message": parsed.get("message", "")}
    else:
        return {"type": "question", "message": response_text}
