# Post-Import OpenCypher Query

## Summary

Add an expandable "Post-Import Query" text box to the Import page that lets users specify a single OpenCypher query to run against the graph once the import pipeline completes successfully. This enables common post-import operations (creating indexes, computing properties, running a quick sanity check) without leaving the Import dialog.

## Current Behavior

The import pipeline (`services/pipeline.py`) runs three steps:
1. Create/reset Neptune Analytics graph
2. Run Athena queries and import CSV data
3. Mark projection as `complete`

After completion, the UI shows a "Graph ready" card with the endpoint. The user must navigate elsewhere to run queries.

## Proposed Behavior

### UI Changes (`ui-src/src/pages/Import.tsx`)

- Add a collapsible/expandable section below the Graph Name field (inside the form Card) labeled **"Post-Import Query (OpenCypher)"**.
- Collapsed by default; shows a chevron toggle and a one-line summary ("Run a query after import completes").
- When expanded, reveals:
  - A `<textarea>` (monospace, 4 rows) for an OpenCypher query.
  - Helper text: "This query runs against the graph after a successful import. Use it for index creation, property computation, or sanity checks."
- The query value is persisted with the projection (saved on `ensureSession()`).
- After import completes, if a post-import query was provided:
  - Show a new progress step "Running post-import query..." in the status card.
  - On success: show result summary (row count or "OK") inline.
  - On failure: show the error but do NOT fail the overall projection — the graph is already imported. Display a "Retry" button.

### API Changes

#### Schema (`routers/schemas.py`)
- Add `post_import_query: Optional[str] = None` to `ProjectionCreate`, `ProjectionUpdate`, and `ProjectionResponse`.

#### Database (`services/db.py`)
- Add column `post_import_query TEXT` to the `projections` table (migration-safe: use `ALTER TABLE` if table exists, or add to CREATE).

#### Projection Store (`services/projection_store.py`)
- Include `post_import_query` in the `Projection` dataclass and in create/update/serialization logic.

#### Pipeline (`services/pipeline.py`)
- After step 3 (import complete), if `projection.post_import_query` is non-empty:
  - Add step: `_update(projection.id, step="post_import_query", label="Running post-import query", progress=95)`
  - Execute the OpenCypher query via `neptune-data:executeOpenCypherQuery` against the graph.
  - On success: proceed to `complete`.
  - On failure: log the error, store it in a new field `post_import_error`, but still mark projection as `complete` (the graph import itself succeeded).

#### New Endpoint (`routers/projection.py`)
- `POST /api/v0/projection/{projection_id}/run-query` — manually (re-)run the post-import query against the projection's graph. Returns the query result or error. This supports the "Retry" button and ad-hoc re-execution.

### Frontend API Client (`api/index.ts`)
- Add `runQuery: (id: string) => request<QueryResult>(...)` to the `projection` object.
- Add `QueryResult` type: `{ success: boolean; row_count?: number; error?: string; results?: any[] }`.

## Data Flow

```
User fills form → ensureSession() saves post_import_query to backend
    → Execute clicked → pipeline runs → import succeeds
    → pipeline checks post_import_query → executes OpenCypher
    → status polling shows "Running post-import query"
    → complete (with optional post_import_error)
```

## TODO Tasks

### Backend
- [ ] Add `post_import_query` column to `projections` table in `services/db.py`
- [ ] Add `post_import_query` field to `Projection` dataclass in `services/projection_store.py`
- [ ] Add `post_import_query` to `ProjectionCreate`, `ProjectionUpdate`, `ProjectionResponse` schemas
- [ ] Implement OpenCypher execution helper (call `execute_open_cypher_query` on the neptune-graph-data client)
- [ ] Add post-import query step to `run_pipeline()` in `services/pipeline.py`
- [ ] Add `POST /projection/{id}/run-query` endpoint to `routers/projection.py`
- [ ] Add tests for the new endpoint and pipeline step

### Frontend
- [ ] Add `post_import_query` to form state in `Import.tsx`
- [ ] Add collapsible "Post-Import Query" section to the form Card
- [ ] Include `post_import_query` in `ensureSession()` payload
- [ ] Load `post_import_query` in `loadSession()` from existing projections
- [ ] Update status card to show post-import query progress step
- [ ] Show query result/error after completion with a Retry button
- [ ] Add `runQuery` method and `QueryResult` type to `api/index.ts`
- [ ] Add `post_import_query` field to the `Projection` TypeScript interface

### Testing
- [ ] Unit test: pipeline completes successfully with no post-import query (no regression)
- [ ] Unit test: pipeline runs post-import query on success, stores result
- [ ] Unit test: post-import query failure does not fail the projection
- [ ] Unit test: `POST /projection/{id}/run-query` returns results or error
- [ ] Integration test: end-to-end with mock Neptune data client
