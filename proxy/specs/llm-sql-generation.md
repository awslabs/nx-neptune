# Spec: LLM-Assisted SQL Projection Generation

## Summary

Add an LLM-powered feature to the proxy that generates Neptune-compatible SQL projections (vertex + edge) from Athena table schemas and user intent. The LLM uses the Strands Agents SDK with no tools — all schema information is pre-fetched via boto3 and injected into the prompt.

## User Flow

1. User navigates to the existing `index.html` form
2. User fills in **AWS Region**, **Athena Catalog**, and **Athena Database**
3. User clicks a new **"Generate SQL with AI"** button, which opens an inline panel/section
4. The panel shows:
   - A **Model ID** text input (default: `us.anthropic.claude-sonnet-4-5-20250929-v1:0`)
   - A **free-text intent** field (e.g., "I want to model fraud rings between accounts and transactions")
   - A **Submit** button
5. On submit, the backend:
   - Calls `athena:ListTableMetadata` to get all tables in the database
   - Calls `athena:GetTableMetadata` for each table to get column schemas (equivalent to DESCRIBE TABLE)
   - Builds a prompt containing: table schemas, Neptune CSV format rules, and user intent
   - Sends the prompt to the LLM via Strands Agent
6. The LLM responds with either:
   - **SQL output**: one vertex projection + one edge projection (with `~id`, `~label`, `~from`, `~to` aliases)
   - **Clarifying question**: displayed to the user in the panel
7. If clarifying question → user types response → appended to conversation → re-sent to LLM
8. If SQL → injected into the `sqlQuery` textarea on the main form (semicolon-separated: vertex SQL; edge SQL)
9. Conversation state is ephemeral (lost on page reload)

## Architecture

```
┌─────────────┐       POST /api/v0/llm/generate        ┌──────────────────┐
│  index.html │  ──────────────────────────────────────▶│  FastAPI backend │
│  (new panel)│                                         │                  │
│             │◀──────────────────────────────────────── │  1. boto3 athena │
│  Shows SQL  │       { sql | question }                │     (describe)   │
│  or question│                                         │  2. Strands Agent│
└─────────────┘                                         │     (no tools)   │
                                                        └──────────────────┘
```

## API Design

### `POST /api/v0/llm/generate`

**Request:**
```json
{
  "region": "us-east-1",
  "athenaCatalog": "AwsDataCatalog",
  "athenaDatabase": "my_database",
  "modelId": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
  "userMessage": "I want to model fraud rings between accounts and transactions",
  "conversationHistory": [
    {"role": "user", "content": "..."},
    {"role": "assistant", "content": "..."}
  ]
}
```

**Response (SQL generated):**
```json
{
  "type": "sql",
  "vertexSql": "SELECT account_id AS \"~id\", 'Account' AS \"~label\", name, balance FROM accounts",
  "edgeSql": "SELECT txn_id AS \"~id\", sender_id AS \"~from\", receiver_id AS \"~to\", 'TRANSFERRED' AS \"~label\", amount FROM transactions"
}
```

**Response (clarifying question):**
```json
{
  "type": "question",
  "message": "I see tables `accounts`, `transactions`, and `merchants`. Which table should represent the edge relationships — transactions between accounts, or purchases at merchants?"
}
```

## Prompt Design

The system prompt sent to the Strands Agent includes:

1. **Role**: You are a SQL expert that generates Athena SQL projections for Neptune Analytics graph import.

2. **Neptune CSV format rules** (embedded in prompt):
   - Vertex CSV requires: `~id` (required), `~label` (recommended)
   - Edge CSV requires: `~id` (required), `~from` (required), `~to` (required), `~label` (recommended)
   - Property columns use `propertyname:type` syntax (types: Bool, Byte, Short, Int, Long, Float, Double, String, Date)
   - Column aliases in SQL must match these headers exactly

3. **Output format instructions**:
   - If you have enough information, respond with EXACTLY this JSON format:
     ```
     {"type": "sql", "vertexSql": "<SQL>", "edgeSql": "<SQL>"}
     ```
   - If you need clarification, respond with:
     ```
     {"type": "question", "message": "<your question>"}
     ```
   - SQL must use column aliases to produce Neptune-compatible headers
   - Each SQL statement must be a valid Athena SQL SELECT

4. **Table schemas** (injected dynamically):
   ```
   Table: accounts
   Columns: account_id (string), name (string), balance (double), created_at (timestamp)

   Table: transactions
   Columns: txn_id (string), sender_id (string), receiver_id (string), amount (double), ts (timestamp)
   ```

5. **User intent**: (the free-text from the form)

## Backend Implementation

### New file: `proxy/src/nx_neptune_proxy/llm_generator.py`

Responsibilities:
- Fetch table schemas from Athena via boto3 (`list_table_metadata` + `get_table_metadata`)
- Build the system prompt with Neptune format rules + table schemas
- Initialize a Strands `Agent` with the specified model ID, no tools
- Send user message (with conversation history) to the agent
- Parse the agent response as JSON (`type: sql` or `type: question`)
- Return structured response to the API layer

### New endpoint in `main.py`

- `POST /api/v0/llm/generate` — calls `llm_generator` and returns the result

### UI changes in `index.html`

- New collapsible section: "Generate SQL with AI"
- Model ID input (defaulted)
- Free-text intent textarea
- Submit button
- Response area (shows either generated SQL or clarifying question)
- "Use this SQL" button that injects the result into the main `sqlQuery` textarea
- Conversation history maintained in JS memory (array of `{role, content}` objects)

## Dependencies

Add to `proxy/pyproject.toml`:
```
"strands-agents>=0.1",
```

## Constraints & Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Tools for Strands Agent | None | Schema is pre-fetched; simpler architecture |
| Projections per request | 1 vertex + 1 edge | Keep simple for v1; extensible later |
| Conversation persistence | Ephemeral (JS memory) | Sufficient for iterative refinement |
| Schema source | All tables in database | User doesn't need to pre-select; LLM can reason about all available tables |
| Model selection | User-configurable text field | Flexibility without hardcoding |
| SQL validation | Deferred to existing preview step | LLM only generates; user validates via schema preview |

## Future Extensions

- Multiple vertex + edge projections (N vertex SQL + M edge SQL)
- Persist conversation history across reloads (session storage or backend state)
- Auto-suggest model based on region availability
- Streaming LLM responses for better UX on slow models
