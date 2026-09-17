# Spec: AI-Assisted Graph Import ("Generate with AI")

Status: **Draft / Prototype**
Owner: _TBD_
Related code: `proxy/ui-src/src/pages/Import.tsx`, `proxy/src/nx_neptune_proxy/routers/projection.py`, `proxy/src/nx_neptune_proxy/services/`
Prior art: `~/git/amazon-neptune-samples/neptune-prototyping-with-agents` (Strands + Bedrock agent pipeline — see its `PROJECT_OVERVIEW.md`)

---

## 1. Purpose

Let a user fill out the **Import** dialog (which projects Athena/data-lake tables into a Neptune Analytics graph) using natural language instead of hand-writing SQL and openCypher. A **"Generate with AI"** button opens an in-page chat assistant that, backed by Amazon Bedrock and a small pipeline of Strands agents, will:

1. Discover the available Athena databases/tables and their columns/types (see §9.6; an explicit **ontology** artifact is a Future Enhancement — §7).
2. Turn a plain-English request into the concrete form fields — Catalog, Database, S3 Staging Bucket, Graph Name, and the **Node Queries** / **Edge Queries** (SQL).
3. Optionally propose **post-import openCypher queries/mutations** to run against the finished graph (a new section added to the Import dialog).

The goal is a **demo-quality prototype** that proves the end-to-end flow on a real Athena catalog + Neptune Analytics graph, not a production feature.

### Non-goals (prototype)
- No multi-turn memory persisted across page reloads (in-memory conversation only).
- No fine-tuning; use off-the-shelf Bedrock Claude models.
- No guarantee of query optimality — correctness and "it runs" over performance.
- No auth/roles beyond the proxy's existing per-run bearer token.

---

## 2. Use Cases

- **UC-1 — Zero-to-graph from a question.** "Get the PageRank of all malware in my database." The assistant inspects the catalog, proposes node/edge SQL, fills the dialog, and offers follow-up openCypher queries to run after import (a `pageRank.mutate` to persist scores, then a read for the top malware).
- **UC-2 — Field autofill.** User has picked a database but doesn't know the schema; asks "which tables have relationships?" and the assistant sets the edge queries.
- **UC-3 — Guided refinement.** Assistant asks probing questions ("Do you want `groups` as nodes too?") or runs a sample `SELECT ... LIMIT 5` to preview data before committing to a mapping.
- **UC-4 — Post-import analytics.** After the graph is built, user asks "find the shortest path between two techniques" and the assistant emits one or more openCypher queries into the new query/mutation section.
- **UC-5 — Manual override.** Everything the assistant fills remains editable; the user can accept, tweak, or discard any suggestion.

---

## 3. Dependencies

### Existing (already in the proxy)
- FastAPI backend + React/Vite SPA (`Import.tsx`) with the auto-save projection model (`projection_service`, `query_store`).
- `ClientFactory` (from `nx-neptune`) for Athena / S3 / Neptune Analytics clients; region from `AWS_DEFAULT_REGION`.
- Existing metadata endpoints: `/metadata/athena/{catalogs,databases,tables,columns}`, `/metadata/s3/buckets`.
- Per-run bearer token auth on all `/api/*` routes.

### New
- **Amazon Bedrock** access (Claude Sonnet 4.5 or configurable) — `bedrock:InvokeModel*`.
- **Strands Agents** (`strands-agents`, `strands-agents-tools`) added to `pyproject.toml`.
- IAM additions: `bedrock:InvokeModel`, `bedrock:InvokeModelWithResponseStream`; Athena `SHOW CREATE TABLE` / sample `SELECT` already covered by existing Athena perms.
- (Post-import queries) Neptune Analytics `ExecuteQuery` permission for the openCypher run/mutate feature.

---

## 4. High-Level Design

### 4.1 UI

**"Generate" button** (Requirement 1)
- Placed in the Import page header (near the projection selector) and/or at the top of the config `Card`.
- Style: dark purple background (`bg-purple-800` / `#5b21b6`-ish), white text "Generate", with a **sparkle** icon prefix (`Sparkles` from `lucide-react`).

**Chat panel** (Requirement 2)
- Clicking **Generate** expands a chat-bot-style panel **at the top, above the "Project" text box** (collapsible; not a modal overlay so the form stays visible).
- Contains: message transcript, a text input + send button, a small "Bedrock model" selector (see 4.3), and a status/thinking indicator.

**Assistant → form binding** (Requirements 3 & 5)
- Assistant responses include a **structured proposal** (JSON) that maps onto the existing React state: `catalog`, `database`, `bucket`, `graphName`, `nodeQueries[]`, `edgeQueries[]`, and the new `graphQueries[]`.
- Proposals are applied through the same setters the form uses, so existing auto-save (`ensureProjection`, `scheduleSave`) persists them. Each proposed field shows an "AI-suggested" affordance the user can accept/edit.

**New post-import query/mutation section** (Requirement 4)
- A new `Card` rendered **after "Edge Queries"** titled e.g. **"Graph Queries (openCypher)"**.
- Same **Add / Trash** repeat-row UX as Node/Edge Queries (`addNodeQuery` pattern), each row a textarea for one openCypher statement; runs in sequence.
- Runs only against a **completed** graph (guarded when `status !== "complete"`); supports both read queries and mutations.

### 4.2 Backend — new endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v0/assistant/session` | Start an assistant session (returns id; holds ontology + history in memory). |
| `POST` | `/api/v0/assistant/message` | Send a user turn; returns assistant text + structured field proposal + any probing question. |
| `GET`  | `/api/v0/assistant/models` | List configured/allowed Bedrock model IDs. |
| `POST` | `/api/v0/projection/{id}/graph-queries` (+ `GET`, `PUT`) | CRUD for the new openCypher graph-query list (mirrors `/queries`). |
| `POST` | `/api/v0/projection/{id}/graph-queries/run` | Execute the saved openCypher queries in sequence against the projection's graph; return rows / mutation counts. |

Notes:
- Reuse `query_store` patterns; add a `graph_queries` table alongside `node_queries` / `edge_queries`.
- The run endpoint uses `ClientFactory().neptune()` (Neptune Analytics `ExecuteQuery`); there is **no** existing openCypher execution path in the proxy today — this is net-new (`graph.py` only does lifecycle actions).

### 4.3 Bedrock configuration
- New settings (env): `BEDROCK_MODEL` (default `us.anthropic.claude-sonnet-4-5-...`), `BEDROCK_REGION` (defaults to `AWS_REGION`).
- Exposed read-only via `/metadata/config` and selectable in the chat panel's model dropdown.

### 4.4 Agent pipeline (Strands + Bedrock)

> **Superseded by §9.** The linear Ontology-Builder → SQL-Mapper → Query-Planner pipeline below was designed for the Import-embedded chat. §9 replaces it with a **supervisor (agents-as-tools)** topology behind the standalone drawer (§8), drops the Ontology Builder (→ Future Enhancement, §7), and adds a **Schema Discovery** agent built on the proxy's existing Athena metadata endpoints plus a **Navigation** agent. `BaseAgent` (4.4.2), the JSON-contract discipline, and the worked example (4.4.6) still apply. Kept here for history.

Modeled on `neptune-prototyping-with-agents`: each stage is a Strands `Agent` with a system prompt and a Bedrock model, wrapped by a shared base class; **structured JSON is the contract between stages**, with a JSON-extraction fallback for when the model wraps output in prose/markdown.

#### 4.4.1 Orchestration

A per-session **Orchestrator** (created by `POST /assistant/session`, driven by `POST /assistant/message`) owns an in-memory session holding the ontology and conversation history. Each user turn flows:

```
user turn ──▶ Orchestrator
                 │  (ontology cached on session? if not, run Agent A once)
                 ├─▶ Agent A  Ontology Builder      ── ontology JSON ─┐
                 │                                                    │ (cached on session)
                 ├─▶ Agent B  SQL Property Mapper    ── node/edge SQL ─┤
                 ├─▶ Agent C  Graph Query Planner    ── openCypher[] ──┤
                 │                                                    ▼
                 └─◀ assembled proposal + assistant text + optional clarifying question
                        │
                        ▼
             structured proposal ──▶ UI applies to form setters (catalog, database,
                                     bucket, graphName, nodeQueries[], edgeQueries[],
                                     graphQueries[]) via existing auto-save
```

- **Agent A runs at most once per session** (ontology is cached); B and C run each relevant turn against the cached ontology + the new request.
- Any agent may short-circuit the turn by returning a **clarifying question** instead of a final proposal; the Orchestrator relays it to the chat and waits for the next user turn (drives the probing loop, e.g. "Include `groups` as nodes too?").
- The Orchestrator merges A/B/C outputs into one `AssistantProposal` (see §5) and only includes the fields an agent actually produced, so a follow-up turn can update just the graph queries without clobbering the SQL.

#### 4.4.2 Shared base (`BaseAgent`)

Ported/trimmed from the sample's `agents/baseclass.py`. Wraps a Strands `Agent` and provides:
- **Retry** on EventLoop / EventStream / Bedrock throttling exceptions (exponential backoff).
- **Usage/metrics** collection (tokens, latency) surfaced for the future cost/token meter.
- **`_execute_agent` hook** as the single overridable call site; **`extract_json`** to recover a JSON object from a model reply that added stray text.
- A configured Bedrock model id (default from `BEDROCK_MODEL`, overridable per request via the chat panel's model selector).

#### 4.4.3 The agents

For the concrete pattern of defining an agent (subclass `BaseAgent`, supply a system prompt + model, optionally inject tools) and chaining it into the pipeline, follow the reference implementation in [`aws-samples/amazon-neptune-samples/neptune-prototyping-with-agents`](https://github.com/aws-samples/amazon-neptune-samples/tree/master/neptune-prototyping-with-agents/) — see its `agents/` directory (`baseclass.py` plus one module per agent) and `full_end_to_end_workflow.py` for how agents are constructed and sequenced. Each agent below mirrors that structure.

| Agent | Input | Output (JSON contract) | Tools |
|---|---|---|---|
| **A — Ontology Builder** | chosen catalog/database (or discovers via `/metadata`) | ontology: tables → columns/types + inferred relationships (`relationships.source_ref → *.id`) | Athena `SHOW CREATE TABLE`, sample `SELECT ... LIMIT n` |
| **B — SQL Property Mapper** | ontology + user request | `{ nodeQueries: [...], edgeQueries: [...] }` as Athena SQL with bulk-load aliases `~id`/`~label`/`~from`/`~to` | Athena sample `SELECT ... LIMIT n` (optional, to confirm columns) |
| **C — Graph Query Planner** | ontology + user request | `{ graphQueries: [...] }` openCypher statements (or `none`) | none (pure LLM); openCypher validated at run time by the Phase 1 endpoint |

**Agent A — Ontology Builder.** Runs one or more Athena `SHOW CREATE TABLE` calls and emits a lightweight ontology (JSON-LD, or `.owl`/`.rdf`) of tables, columns, types, and inferred relationships. Cached on the session and reused by B and C.

**Agent B — SQL Property Mapper.** Emits Node Queries and Edge Queries as Athena SQL using the Neptune bulk-load column aliases (`~id`, `~label`, `~from`, `~to`) — the same convention the current `QueryBuilder`/`Import` placeholders use. Typically one node query per entity table and edge queries split by relationship type.

**Agent C — Graph Query Planner.** Decides whether a follow-up openCypher query/mutation is requested and emits one or more statements for the new Graph Queries section (e.g. a `pageRank.mutate` followed by a ranked read — see the worked example below).

#### 4.4.4 Tool layer

- Agents reach Athena via **direct-call tools** (`SHOW CREATE TABLE`, `SELECT ... LIMIT n`) built on the proxy's existing `ClientFactory().athena()` — no new metadata path required.
- Following the sample, tools can alternatively be exposed through an **MCP client** (`list_tools_sync()`); for this prototype direct-call tools are sufficient since the proxy already wraps the AWS clients.
- Agents run **sequentially** per turn (unlike the sample's fan-out data generation) — the latency is dominated by A's `SHOW CREATE TABLE` calls, which are cached, so no thread pool is needed for the prototype.

#### 4.4.5 Error handling & guardrails
- Throttling/transient Bedrock errors retried in `BaseAgent`; invalid-JSON replies recovered via `extract_json`, else the turn returns a plain clarifying message rather than a broken proposal.
- Generated SQL/openCypher is treated as **untrusted**: it is surfaced in the editable form for the user to review, and openCypher only executes through the Phase 1 run endpoint (guarded to `status == complete`, with explicit confirmation required for mutations — see Phase 4).

#### Worked example
Request: *"get the page rank from all malware in my database"* with the given `SHOW CREATE TABLE` results (`malware`, `groups`, `campaigns`, `mitigations`, `relationships`, `techniques`, `tools`).

- **Agent A** infers: entity tables (`malware`, `campaigns`, `mitigations`, `tools`, …) keyed by `id`; `relationships(source_ref, target_ref, relationship_type)` is an edge table linking any two entities.
- **Agent B** proposes multiple node and edge queries — one node query per entity table, and the `relationships` table split into one edge query per `relationship_type`, e.g.:
  - Node: `SELECT id AS "~id", 'Malware' AS "~label", name, attack_id, platforms FROM malware`
  - Node: `SELECT id AS "~id", 'Campaign' AS "~label", name, attack_id, first_seen FROM campaigns`
  - Node: `SELECT id AS "~id", 'Mitigation' AS "~label", name, attack_id, description FROM mitigations`
  - Node: `SELECT id AS "~id", 'Tool' AS "~label", name, attack_id, platforms FROM tools`
  - Edge: `SELECT id AS "~id", source_ref AS "~from", target_ref AS "~to", 'uses' AS "~label" FROM relationships WHERE relationship_type = 'uses'`
  - Edge: `SELECT id AS "~id", source_ref AS "~from", target_ref AS "~to", 'mitigates' AS "~label" FROM relationships WHERE relationship_type = 'mitigates'`
  - Edge: `SELECT id AS "~id", source_ref AS "~from", target_ref AS "~to", 'attributed-to' AS "~label" FROM relationships WHERE relationship_type = 'attributed-to'`

  (`groups` and `techniques` are omitted here; the assistant offers to add them.)
- **Agent C** proposes two post-import openCypher statements — first a `pageRank.mutate` that writes a `pagerank` property onto every node, then a read that ranks malware by it:
  ```cypher
  CALL neptune.algo.pageRank.mutate({
    writeProperty: "pagerank"
  })
  YIELD success
  RETURN success
  ```
  ```cypher
  MATCH (n)
  WHERE 'Malware' IN labels(n)
  RETURN n.name AS malware, n.pagerank AS pagerank
  ORDER BY pagerank DESC
  LIMIT 10
  ```
  (See the [Neptune Analytics `pageRank.mutate` docs](https://docs.aws.amazon.com/neptune-analytics/latest/userguide/page-rank-mutate.html). The `mutate` variant persists scores as a node property so subsequent queries can read and rank on them, rather than recomputing per query.)

---

## 5. Data Model Changes
- New SQLite table `graph_queries` (`id`, `projection_id`, `cypher`, `position`) — mirrors `node_queries`/`edge_queries` in `query_store.py`.
- New response/request schemas in `routers/schemas.py`: `GraphQueryInput`, `GraphQueriesPayload`, `GraphQueriesResponse`, assistant message/proposal schemas.
- Front-end `api/index.ts`: add `graphQueries` types + `assistant` client group.

---

## 6. Prototype TODO List

### Phase 0 — Scaffolding
- [ ] Add `strands-agents`, `strands-agents-tools` to `proxy/pyproject.toml`; pin versions.
- [ ] Add Bedrock settings (`BEDROCK_MODEL`, `BEDROCK_REGION`) to `config.py` + surface in `/metadata/config`.
- [ ] Add IAM policy snippet (Bedrock + Neptune `ExecuteQuery`) to `README.md`.

### Phase 1 — Post-import Graph Queries (Requirement 4) — *do first; no AI needed*
- [ ] Create `graph_queries` table + migration in `services/db.py`.
- [ ] Extend `query_store.py` with `list/save_graph_queries`.
- [ ] Add schemas: `GraphQueryInput`, `GraphQueriesPayload`, `GraphQueriesResponse`.
- [ ] Add `GET/PUT /projection/{id}/graph-queries` and `POST /projection/{id}/graph-queries/run` (openCypher via `ClientFactory().neptune()`), guarded to `status == complete`.
- [ ] UI: new "Graph Queries (openCypher)" `Card` after Edge Queries with Add/Trash rows (clone node-query pattern in `Import.tsx`).
- [ ] UI: "Run Queries" action + results/mutation-count display; disable until graph complete.

### Phase 2 — Assistant backend (agents)

> **Revised by §9.11** — the Agent A/B/C list below is superseded by the supervisor (agents-as-tools) roster: Schema Discovery, SQL Mapping, Query Planner, Navigation, Page-Action, and a Supervisor. Use §9.11's checklist. Original bullets kept for history.

- [ ] `BaseAgent` wrapper (retry + Strands metrics), ported/trimmed from the sample.
- [ ] ~~Agent A — Ontology Builder~~ → replaced by **Schema Discovery agent** (Athena metadata API, §9.6).
- [ ] Agent B — SQL Property Mapper (→ **SQL Mapping agent**, §9.5).
- [ ] Agent C — Graph Query Planner (→ **Query Planner agent**, §9.5).
- [ ] Orchestrator: session store (in-memory), probing-question loop, structured proposal assembly (→ **Supervisor**, §9.2/9.9).
- [ ] `POST /assistant/session`, `POST /assistant/message`, `GET /assistant/models`.

### Phase 3 — Assistant UI (Requirements 1, 2, 3, 5)
- [ ] "Generate" button — dark purple, white text, `Sparkles` prefix — in Import header/config card.
- [ ] Collapsible chat panel rendered **above the Project field**.
- [ ] Wire assistant proposals to form setters (`catalog`, `database`, `bucket`, `graphName`, node/edge/graph queries) via existing auto-save.
- [ ] "AI-suggested" accept/edit affordance per field; model-selector dropdown in the panel.

### Phase 4 — Demo hardening
- [ ] End-to-end demo against the MITRE ATT&CK-style catalog (malware/groups/relationships/techniques) → PageRank.
- [ ] Basic error handling (Bedrock throttling retry, invalid-JSON recovery like the sample's `extract_json`).
- [ ] Prompt-injection guardrails: treat DB content/user text as untrusted; validate generated SQL/openCypher before run; require explicit confirm for mutations.
- [ ] Short demo script + screenshots in `docs/`.

---

## 6a. Requirement → Phase Traceability

| # | Requirement (summary) | Phase(s) | Key TODO items |
|---|---|---|---|
| 1 | "Generate with AI" button — dark purple, white "Generate" text, sparkle prefix | Phase 3 | Generate button in Import header/config card |
| 2 | Clicking Generate expands a chat panel above the "Project" field | Phase 3 | Collapsible chat panel above Project field |
| 3 | Chat defines dialog fields (Catalog, Database, S3 Staging Bucket, Graph Name, Node Queries, Edge Queries) | Phase 2 + Phase 3 | Agents A & B; wire proposals to form setters |
| 4 | New post-import openCypher query/mutation section after Edge Queries (Add for multiple, run in sequence) | Phase 1 | `graph_queries` table/store/schemas, CRUD + run endpoints, UI card + Run action |
| 5 | Chat can also define one or more openCypher queries in the dialog | Phase 2 + Phase 3 | Agent C (Graph Query Planner); proposals populate Graph Queries card |

Cross-cutting (all requirements): Phase 0 scaffolding (Strands/Bedrock deps, config, IAM) and Phase 4 demo hardening (guardrails, error handling, end-to-end demo).

## 7. Future Enhancements (post-prototype)

### UI Updates
- Minimize/collapse sections while chatting, so users can inspect the details only if they want to.
- Ontology preview/section area — view the source catalog rendered as an ontology.
- Cost/token meter in the chat panel (reuse Strands metrics).

### AI-Driven Features
- Streaming assistant responses (token-by-token) instead of request/response.
- "Explain this suggestion" — agent rationale surfaced inline.
- Persist assistant sessions (SQLite) and allow resuming a conversation.
- Data-preview-driven type inference and cardinality hints in the ontology.
- Query optimization pass on generated openCypher (borrow the sample's Explain + Tuning agents).

### Ontology (deferred from the prototype)
- **Ontology-driven import.** Re-introduce an Ontology Builder agent that emits a lightweight ontology (JSON-LD / `.owl`/`.rdf`) of the source catalog and use it as an explicit import parameter, instead of the prototype's direct schema-discovery → mapping flow (§9). Dropped from the prototype because it added a modeling artifact without improving the demo path.
- Save generated ontology as a reusable artifact attached to the project or persistent storage.
- Import/Read external ontology context for Catalogs.
- Multi-database / cross-catalog ontologies and joins.

### Graph Enhancements
- OpenCypher validation — add a validation option, or fold openCypher checks into the existing "Validate Query" button.
- Auto-suggest graph memory (m-NCU) sizing from estimated node/edge counts.
- RDF/SPARQL target support (sample currently property-graph only).

### Security & Guardrails
- Guardrail policy config (allow/deny mutation keywords, per-environment).

---

## 8. Revision: Standalone, Cross-Page Assistant

Status: **Design agreed** (mock-only scope). Supersedes the "chat panel embedded in Import" model in §4.1 — the assistant becomes a global, page-aware drawer rather than Import-local UI.

### 8.1 Motivation
Today the assistant lives entirely inside `Import.tsx` as a visual-only mock (canned responses, local React state, direct `setCatalog`/`setNodeQueries` calls). Three new requirements:

1. **Separate the assistant from the AI pages** — run it in its own frame, but still let it update the **current** page (e.g. Import's SQL queries, database name, graph name).
2. **Cross-page navigation** — the assistant can suggest jumping to another page and raise an inline button to do it. Three jumps: **New Import**, **New Project**, **Graphs in the Project**.
3. **Page-aware suggested actions** — the assistant surfaces the current page's actions as inline chat buttons: Import → **Execute**, **Validate Query**, **Preview Schema**; Projections/Graphs → **Stop Instance**, **Delete Instance**.

### 8.2 Design decisions

| # | Decision | Choice |
|---|---|---|
| 1 | Assistant ↔ active-page coupling | **PageBridge React Context.** Global `AssistantProvider`; each page registers `{ fields, setters, actions, jumpContext }` on mount and unregisters on unmount. Type-safe, single source of truth. |
| 2 | "Separate frame" form factor | **Global docked right-side drawer**, mounted in `App` (in-window, same React tree — no iframe/postMessage). Persists across route changes. |
| 3 | Scope of this work | **Mock only.** Replies stay canned; **jump + action buttons are real** (real `navigate` + real PageBridge action calls). No Bedrock, no new `/assistant` endpoints (those remain Phase 2 of §6). |
| 4 | Toggle placement | **Global** — `✨ AI Assistant` button in the Sidebar footer, openable on every page. Optional contextual `Generate` on Import that just opens the drawer. |
| 5 | Stop/Delete Instance target | Registered via PageBridge on **both** the Projections list (`/projections?project=X`) and Graphs (`/graphs`) pages; assistant shows the current page's set. Per-graph; if a project has >1 graph, an in-chat **graph picker** precedes the action. `confirm()` required (reuse existing wording). |
| 6 | How buttons are generated | **Dynamic from PageBridge** registered actions, honoring each action's `enabled`/disabled state. No duplicated per-page knowledge in the assistant. |
| 7 | Jump target resolution | New Import → `/import?project=…&t=Date.now()`; New Project → `/projects`; **Graphs in Project → always asks "which project?"** (fetches `projectApi.list()`) then `/projections?project=X`. New Import + New Project always available. |
| 8 | Button placement in drawer | **Inline in the assistant message** that proposes them (like today's "applied" chips), tying intent to the conversation. |

Notes / to nail down at implementation:
- **"Preview Schema"** label → confirm it maps to `handlePreview` (data preview) vs `handleValidate`.
- The assistant only writes to the **currently mounted** page's bridge — no deferred writes to unmounted pages.
- `/graphs` is a **global** list (no project filter exists); the per-project graph list is `/projections?project=X`. That is the target for "Graphs in the Project."

### 8.3 TODO List

**Group 1 — Assistant shell + context (foundation)**
- [ ] `AssistantProvider` + `useAssistant()`: holds `open`, chat transcript, `bedrockModel`, `thinking`, and the active PageBridge registration.
- [ ] `usePageBridge(reg)` hook: registers `{ page, fields, setters, actions: {name → {run, enabled, label}}, jumps }`; clears on unmount.
- [ ] `AssistantDrawer` component (right-docked, collapsible/expandable) — port the panel markup from `Import.tsx` (header, model select, transcript, thinking indicator, input).
- [ ] Mount `<AssistantProvider>` + `<AssistantDrawer/>` in `App.tsx`, wrapping `<Routes>` so it survives navigation.
- [ ] Global `✨ AI Assistant` toggle in the `Sidebar` footer.

**Group 2 — De-couple the current Import assistant**
- [ ] Remove assistant state/JSX/`sendChat` from `Import.tsx`; move the canned `sendChat` into the provider/mock module.
- [ ] Rework the canned MITRE fill to call `bridge.setters.*` (runs only when Import is the active page).
- [ ] Optional contextual `Generate` button in the Import header that opens the global drawer.

**Group 3 — PageBridge registrations per page**
- [ ] `Import.tsx`: register fields (`catalog, database, bucket, graphName, nodeQueries, edgeQueries, graphQueries`), setters, and actions `execute → handleExecute`, `validateQuery → handleValidateQuery`, `preview → handlePreview`; wire each action's `enabled`.
- [ ] `Graphs.tsx`: register per-graph `stop`/`delete` from `graphActions` (respect `getActions` availability + transient states).
- [ ] `Projections.tsx`: register the project's graphs + `stop`/`delete` per graph (same picker path).
- [ ] Every page: expose `jumpContext` = current `project` id from URL params (or null).

**Group 4 — Jump actions (inline buttons)** ✅
- [x] Helper to emit inline jump buttons: New Import, New Project, Graphs in Project (always renders an in-chat project picker → `/projections?project=X`). Implemented as `JumpAction` type + `runJump` in `assistant/context.tsx`; `graphs-in-project` fetches `projectApi.list()` and appends an assistant message with `open-projections` buttons per project.
- [x] Canned intent matching ("create a project", "new graph", "show my graphs") → attach relevant jump button(s) to the reply. Implemented via `detectJumps()` in `assistant/mock.ts`; jumps render as inline buttons under the message in `AssistantDrawer.tsx`.

**Group 5 — Suggested page actions (inline buttons)** ✅
- [x] Render inline action buttons from `bridge.actions`, disabled per `enabled`. `ChatAction` descriptors + `detectActions()` (mock) match intent; rendered in `AssistantDrawer.tsx` (disabled honored).
- [x] Destructive (`stop`/`delete`): `confirm()` before `graphActions.perform`; in-chat graph picker first when >1 graph. `detectActions` emits one `graph-action` button per eligible graph (the picker); `runChatAction` delegates to the page's `runGraphAction`, which already runs `confirm()` + refresh.
- [x] Surface action result/error back into the transcript. `runChatAction` posts a "Ran …" / "Requested …" / "That action failed: …" assistant note, and guards against acting on an unmounted page.

**Group 6 — Cleanup** ✅
- [x] Move shared chat types (message shape, `applied` chips) into the assistant module; drop now-unused `lucide` imports from `Import.tsx`. Chat types (`ChatMessage`, `JumpAction`, `ChatAction`) live in `assistant/context.tsx` with no duplicates elsewhere; Import.tsx's remaining lucide icons are all still in use (no dead imports).
- [ ] Manual pass: open drawer → navigate Import→Projects→Graphs; confirm transcript persists and buttons reflect the active page. _(User-run verification.)_

---

## 9. Revision: Strands agent backend behind the standalone assistant

Status: **Design agreed.** Supersedes §4.4 (the Import-embedded Ontology → SQL-Mapper → Query-Planner pipeline). Builds directly on §8: the assistant is already a global, page-aware drawer that executes real jumps and page/graph actions but returns **canned** text (`cannedRespond` in `assistant/mock.ts`). This section replaces that mock with a **Strands supervisor agent** on Bedrock behind `POST /assistant/message`, while keeping the exact client-render contract §8 already ships (assistant text + `applied` field proposal + inline `jumps[]` + inline `actions[]`).

Prior art remains `neptune-prototyping-with-agents` — but this design uses its **agents-as-tools** supervisor pattern (one entry agent whose tools are other agents), not the sample's linear fan-out.

### 9.1 What changes from §4.4

| §4.4 (superseded) | §9 (this design) |
|---|---|
| Chat embedded in `Import.tsx` | Global drawer (§8), works on any page |
| Linear A → B → C pipeline | **Supervisor** routes each turn to specialist agents exposed as tools |
| **Agent A — Ontology Builder** emits an ontology artifact via `SHOW CREATE TABLE` | **Schema Discovery agent** reads columns/types via the proxy's existing Athena **metadata API**; no ontology artifact (→ Future Enhancement, §7) |
| Navigation not modeled (jumps were client-only heuristics) | Dedicated **Navigation agent** returns the same `JumpAction` descriptors the client already executes |
| — | Dedicated **Page-Action agent** surfaces the current page's registered actions as `ChatAction` descriptors |

### 9.2 Topology — supervisor (agents-as-tools)

```
client turn ──▶ POST /assistant/message
  { text, sessionId, pageContext }        pageContext = snapshot of the active PageBridge (§9.4)
        │
        ▼
   Supervisor Agent (Strands, Bedrock)      one system prompt; decides which tool(s) to call
        ├─▶ tool navigate()            ─▶ Navigation agent      ─▶ JumpAction[]
        ├─▶ tool generate_import()     ─▶ Schema Discovery ─▶ SQL Mapping ─▶ Query Planner
        │                                                     ─▶ field proposal (catalog…graphQueries)
        └─▶ tool suggest_page_actions()─▶ Page-Action agent    ─▶ ChatAction[]
        │
        ▼
   AssistantReply { text, proposal?, jumps?, actions?, question? }   (§9.8)
```

- Each specialist is a `BaseAgent` (§4.4.2) wrapped as a Strands `@tool` and registered on the supervisor via `Agent(tools=[...])`, exactly the sample's agents-as-tools construction.
- The supervisor calls **only the tools a turn needs** — a pure navigation request ("show the TPCH project") never runs discovery; an import request never fabricates page actions.
- `generate_import` is a **single** supervisor tool that internally runs Discovery → Mapping → Planner in sequence (chosen over three separate tools: smaller supervisor surface, and the mapping/planner agents always get discovery output as context).

### 9.3 Authority model — suggest only

Agents have **no execution authority**. They never call form setters, never `navigate`, never run a graph action. They return *descriptors*; the human clicks. This keeps §8's `runJump` / `runChatAction` as the sole execution path — real navigation and real bridge calls (with `confirm()` on destructive graph actions) happen client-side, unchanged. The field proposal is applied to the form only when the user accepts it (existing "AI-suggested" affordance).

### 9.4 Page-context snapshot (request payload)

The client sends, with each turn, a snapshot derived from the active `PageBridge`:

```jsonc
{ "page": "import",
  "projectId": "proj-123",
  "actions":      [ { "key": "execute", "label": "Execute", "enabled": false } ],
  "graphTargets": [ { "id": "g-1", "name": "malware-graph", "actions": ["stop","delete"] } ] }
```

The Navigation and Page-Action agents may reference **only** keys/targets present in this snapshot (preserves §8 decision 6: buttons are dynamic-from-bridge, the assistant never invents an action a page didn't register). Table names, graph names, and any values in the snapshot are treated as **untrusted** (prompt-injection surface).

### 9.5 The agents

| Agent | Role | Input | Output (JSON contract) | Tools |
|---|---|---|---|---|
| **Supervisor** | Route the turn, assemble the reply | user text + page snapshot + history | `AssistantReply` (§9.8) | the four tools below |
| **Navigation** | Choose cross-page jump(s) | user text + `projectId` from snapshot | `JumpAction[]` — `new-import` / `new-project` / `open-projections` | `projectApi.list` (resolve a project named in the text) |
| **Schema Discovery** | Read the selected DB's schema | catalog + database + user text | `{ tables: [ { name, columns:[{name,type}], sampleRows? } ] }` | Athena **metadata** (`list_table_metadata`, `get_table_metadata`); optional guarded `sample_table` (§9.6) |
| **SQL Mapping** | Schema → node/edge SQL | discovery output + user text | `{ nodeQueries:[…], edgeQueries:[…] }` with `~id`/`~label`/`~from`/`~to` aliases | none (pure LLM over discovery context) |
| **Query Planner** | Post-import openCypher (Req 5) | discovery output + user text | `{ graphQueries:[…] }` or `none` | none (openCypher validated at run time by the Phase 1 endpoint) |
| **Page-Action** | Surface the current page's actions | user text + page snapshot | `ChatAction[]` — `page-action` / `graph-action` | none (constrained to snapshot keys) |

`generate_import` = Discovery → SQL Mapping → Query Planner, merged into one field proposal (`catalog`, `database`, `bucket`, `graphName`, `nodeQueries[]`, `edgeQueries[]`, `graphQueries[]`). Any stage may short-circuit the turn with a clarifying question (e.g. "Include `groups` as nodes too?").

### 9.6 Schema discovery mechanism

- **Reuse the proxy's existing Athena metadata path.** `/metadata/athena/{tables,columns}` is already backed by `ClientFactory().athena()` — `list_table_metadata(catalog, db)` for the table list and `get_table_metadata(catalog, db, table)` for columns + types. These are Athena **metadata-API** calls (Glue-backed): they return columns/types structurally with **no query cost, no wait, and no DDL parsing** — so the agent does **not** issue `DESCRIBE TABLE` / `SHOW CREATE TABLE`. The Discovery agent's tools wrap these two calls.
- **Scope: the selected catalog + database only.** The agent lists that database's tables, then fetches columns only for the tables relevant to the request — not the whole account/all catalogs (bounds latency and keeps unrelated schema out of the model context; matches the Import form's single-database model).
- **Optional, guarded sampling.** A `sample_table` tool runs `SELECT * FROM <table> LIMIT 10` through the existing Athena **query** path, invoked **only** when columns/types are ambiguous (e.g. disambiguating id / label / relationship columns). Capped rows, a few tables at most. Sampled rows are untrusted context (injection guardrails) used purely to inform the mapping — never executed, never echoed as instructions.

### 9.7 Navigation agent

A dedicated agent so navigation intent is resolved independently of import generation (the supervisor can answer "take me to a new import" without touching Athena). It emits the same `JumpAction` descriptors the client already executes in `runJump`: `new-import`, `new-project`, `open-projections`. A project named in the request ("show the TPCH project") is resolved by name against `projectApi.list()`, falling back to the snapshot's current `projectId`.

### 9.8 Reply contract ↔ existing client

`AssistantReply` maps 1:1 onto the `ChatMessage` §8 already renders — so the **only** client change is swapping the mock call for the API call; rendering, jump execution, and action execution are untouched:

| `AssistantReply` field | Rendered by (existing) |
|---|---|
| `text` | message body |
| `proposal` (field map) | applied via bridge setters + `applied` chips |
| `jumps: JumpAction[]` | inline jump buttons → `runJump` |
| `actions: ChatAction[]` | inline action buttons → `runChatAction` (disabled honored, `confirm()` on destructive) |
| `question` | plain clarifying turn (no proposal) |

Client wiring: `assistant/context.tsx` `sendChat` replaces `await cannedRespond(text, bridge)` with `await assistantApi.message({ text, sessionId, pageContext })`, where `pageContext` is serialized from `bridgeRef.current`. `assistant/mock.ts` becomes an offline fallback.

### 9.9 Orchestration & session

- Supervisor and specialists are built on `BaseAgent` (§4.4.2: retry on Bedrock throttling, `extract_json` recovery, token/latency metrics). Model id from `BEDROCK_MODEL`, overridable per request via the drawer's model selector (already wired in `AssistantDrawer.tsx`).
- Per-session in-memory store keyed by `sessionId`: a **discovery cache** (per catalog+database) plus conversation history. Discovery re-runs when the target database changes; Mapping/Planner run each relevant turn against the cached schema + the new request.
- Agents run sequentially within a turn (latency dominated by discovery, which is cached) — no thread pool needed for the prototype.

### 9.10 Guardrails

- **Suggest-only (§9.3) is the primary guardrail**: no agent can mutate the form, navigate, or run a destructive action — only propose.
- Discovery/sample data and all generated SQL/openCypher are **untrusted**: surfaced in the editable form, executed only through the Phase 1 run endpoints (guarded to `status == complete`, explicit `confirm()` for mutations).
- Navigation and Page-Action agents are constrained to the page snapshot's registered keys/targets — they cannot surface an action the current page didn't register.

### 9.11 TODO (revises Phase 2 of §6)

Replaces the Agent-A/B/C bullets under **Phase 2 — Assistant backend**:

- [ ] `BaseAgent` wrapper (retry + `extract_json` + Strands metrics), ported/trimmed from the sample.
- [ ] **Schema Discovery agent** + tools over existing `ClientFactory().athena()` metadata (`list_table_metadata`, `get_table_metadata`); optional guarded `sample_table` (`LIMIT 10`).
- [ ] **SQL Mapping agent** (discovery context + request → node/edge SQL with `~id/~label/~from/~to`).
- [ ] **Query Planner agent** (discovery context + request → openCypher list, or "none").
- [ ] **Navigation agent** (request + `projectId` → `JumpAction[]`; resolve named project via `projectApi.list`).
- [ ] **Page-Action agent** (request + page snapshot → `ChatAction[]`, constrained to snapshot keys).
- [ ] **Supervisor agent** registering the four tools (`navigate`, `generate_import`, `suggest_page_actions`); `generate_import` chains Discovery → Mapping → Planner.
- [ ] Session store (in-memory): discovery cache per catalog+database + history; clarifying-question loop; `AssistantReply` assembly.
- [ ] `POST /assistant/session`, `POST /assistant/message` (accepts `pageContext`), `GET /assistant/models`.
- [ ] Client: `sendChat` calls `assistantApi.message(...)` with a serialized `pageContext`; keep `mock.ts` as offline fallback.
