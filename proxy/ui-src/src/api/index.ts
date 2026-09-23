const BASE = "/api/v0";

// The proxy delivers this run's token via the launch URL's query string
// (e.g. http://127.0.0.1:8080/?token=...). We read it once at module load,
// strip it from the address bar (so it doesn't linger in history/referer),
// and cache it in sessionStorage so a full page reload in the same tab keeps
// working without reopening the launch URL.
//
// sessionStorage (not localStorage) scopes the token to this tab session: it
// survives reloads but is cleared when the tab closes and is not shared with
// other tabs. No cookie is set. The token is never sent cross-origin.
const TOKEN_KEY = "nx-neptune-proxy-token";
const PROXY_TOKEN = (() => {
  const params = new URLSearchParams(window.location.search);
  const urlToken = params.get("token");
  if (urlToken) {
    // Remove ?token=... from the URL without reloading the page.
    params.delete("token");
    const query = params.toString();
    const newUrl =
      window.location.pathname +
      (query ? `?${query}` : "") +
      window.location.hash;
    window.history.replaceState(window.history.state, "", newUrl);
    try {
      sessionStorage.setItem(TOKEN_KEY, urlToken);
    } catch {
      // sessionStorage unavailable (e.g. private mode); fall back to memory.
    }
    return urlToken;
  }
  // No token in the URL (e.g. a page reload) — reuse the cached one.
  try {
    return sessionStorage.getItem(TOKEN_KEY);
  } catch {
    return null;
  }
})();

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  headers.set("X-Requested-With", "nx-neptune");
  if (PROXY_TOKEN) {
    headers.set("Authorization", `Bearer ${PROXY_TOKEN}`);
  }
  const res = await fetch(`${BASE}${path}`, { ...init, headers });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.message || `Request failed: ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

// --- Metadata ---

export const metadata = {
  config: () => request<{ region: string; graph_prefix: string; config_bucket: string }>("/metadata/config"),
  catalogs: () => request<{ catalogs: { name: string; status: string }[] }>("/metadata/athena/catalogs"),
  databases: (catalog: string) => request<{ databases: string[] }>(`/metadata/athena/databases?catalog=${encodeURIComponent(catalog)}`),
  tables: (database: string, catalog: string) => request<{ tables: string[] }>(`/metadata/athena/tables?database=${encodeURIComponent(database)}&catalog=${encodeURIComponent(catalog)}`),
  columns: (database: string, table: string, catalog: string) => request<{ columns: { name: string; type: string }[] }>(`/metadata/athena/columns?database=${encodeURIComponent(database)}&table=${encodeURIComponent(table)}&catalog=${encodeURIComponent(catalog)}`),
  buckets: () => request<{ buckets: string[] }>("/metadata/s3/buckets"),
  graphs: () => request<{ graphs: { id: string; name: string; status: string }[] }>("/metadata/neptune/graph-analytics"),
  deleteGraph: (id: string) => request<{ id: string; status: string }>(`/metadata/neptune/graph-analytics/${id}`, { method: "DELETE" }),
  graphSummary: (id: string) => request<{ numNodes: number; numEdges: number; nodeLabels: string[]; edgeLabels: string[] }>(`/metadata/neptune/graph-analytics/${id}/summary`),
};

// --- Graph Actions ---

export interface Inflight {
  action: string;
  error: string | null;
}

export const graphActions = {
  getActions: (graphId: string) => request<{ graph_id: string; status: string; actions: string[]; inflight: Inflight | null }>(`/graphs/${graphId}/actions`),
  perform: (graphId: string, action: string) => request<{ graph_id: string; action: string; status: string }>(`/graphs/${graphId}/${action}`, { method: "POST" }),
  getInflight: (graphId: string) => request<{ graph_id: string; inflight: Inflight | null }>(`/graphs/${graphId}/inflight`),
  dismissInflight: (graphId: string) => request<{ graph_id: string; cleared: boolean }>(`/graphs/${graphId}/inflight`, { method: "DELETE" }),
};;

// --- Projection ---

export interface Projection {
  id: string;
  status: string;
  catalog: string;
  database?: string;
  graph_name?: string;
  graph_id?: string;
  graph_endpoint?: string;
  graph_memory_gb: number;
  s3_staging_bucket?: string;
  project_id: string;
  step?: string;
  step_label?: string;
  progress: number;
  error?: string;
  created_at: string;
}

export interface ProjectionStatus {
  id: string;
  status: string;
  step?: string;
  step_label?: string;
  progress: number;
  error?: string;
  graph_endpoint?: string;
}

export const projection = {
  list: () => request<Projection[]>("/projection"),
  get: (id: string) => request<Projection>(`/projection/${id}`),
  create: (data: Record<string, unknown>) => request<Projection>("/projection", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) }),
  update: (id: string, data: Record<string, unknown>) => request<Projection>(`/projection/${id}`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) }),
  status: (id: string) => request<ProjectionStatus>(`/projection/${id}/status`),
  validate: (id: string) => request<{ valid: boolean; checks: { check: string; passed: boolean; message?: string }[] }>(`/projection/${id}/validate`, { method: "POST" }),
  validateQuery: (id: string) => request<{ valid: boolean; checks: { check: string; passed: boolean; message?: string }[] }>(`/projection/${id}/validate-query`, { method: "POST" }),
  preview: (id: string, limit = 10) => request<{ error?: string; results: { columns: string[]; rows: string[][] }[] }>(`/projection/${id}/preview?limit=${limit}`, { method: "POST" }),
  execute: (id: string) => request<{ message: string }>(`/projection/${id}/execute`, { method: "POST" }),
  runQuery: (id: string, queries: string[]) => request<{ error?: string; results: unknown[] }>(`/projection/${id}/run-query`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ queries }) }),
  // Validate openCypher syntax via Neptune Analytics EXPLAIN (read-only). One
  // verdict per non-blank query, in order.
  explainQuery: (id: string, queries: string[]) => request<{ results: { valid: boolean; error?: string }[] }>(`/projection/${id}/explain-query`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ queries }) }),
  getQueries: (id: string) => request<QueriesResponse>(`/projection/${id}/queries`),
  saveQueries: (id: string, data: QueriesPayload) => request<QueriesResponse>(`/projection/${id}/queries`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) }),
  // Persist only the openCypher graph queries (leaves node/edge queries intact).
  // Query text only — results are never persisted.
  saveGraphQueries: (id: string, graph_queries: GraphQueryInput[]) => request<GraphQueriesResponse>(`/projection/${id}/graph-queries`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ graph_queries }) }),
  delete: (id: string) => request<{ id: string; status: string }>(`/projection/${id}`, { method: "DELETE" }),
  deleteGraph: (id: string) => request<{ id: string; status: string }>(`/projection/${id}/delete-graph`, { method: "POST" }),
};

// --- Multi-Query ---

export interface NodeQueryInput {
  id?: string;
  sql: string;
}

export interface EdgeQueryInput {
  id?: string;
  sql: string;
}

export interface GraphQueryInput {
  id?: string;
  cypher: string;
}

export interface NodeQueryResponse {
  id: string;
  sql: string;
  position: number;
}

export interface EdgeQueryResponse {
  id: string;
  sql: string;
  position: number;
}

export interface GraphQueryResponse {
  id: string;
  cypher: string;
  position: number;
}

export interface QueriesPayload {
  node_queries: NodeQueryInput[];
  edge_queries: EdgeQueryInput[];
  // Post-import openCypher graph queries (text only; results are not persisted).
  graph_queries?: GraphQueryInput[];
}

export interface QueriesResponse {
  node_queries: NodeQueryResponse[];
  edge_queries: EdgeQueryResponse[];
  graph_queries: GraphQueryResponse[];
}

export interface GraphQueriesResponse {
  graph_queries: GraphQueryResponse[];
}

// --- Project ---

export interface Project {
  id: string;
  name: string;
  status: string;
  created_at: string;
}

// --- AI Assistant (agent backend, spec §9) ---
//
// Wire contracts are snake_case (matching the proxy API); the client maps the
// reply into its camelCase JumpAction/ChatAction shapes in assistant/remote.ts.

export interface AssistantSqlQuery {
  sql: string;
}

export interface AssistantCypherQuery {
  cypher: string;
}

export interface AssistantProposal {
  catalog?: string | null;
  database?: string | null;
  bucket?: string | null;
  graph_name?: string | null;
  node_queries?: AssistantSqlQuery[] | null;
  edge_queries?: AssistantSqlQuery[] | null;
  graph_queries?: AssistantCypherQuery[] | null;
}

export interface AssistantJump {
  kind: "new-import" | "new-project" | "open-projections";
  label: string;
  project_id?: string | null;
}

export interface AssistantAction {
  kind: "page-action" | "graph-action" | "run-query";
  page: string;
  label: string;
  action_key?: string | null;
  enabled?: boolean | null;
  destructive?: boolean | null;
  graph_id?: string | null;
  graph_action?: string | null;
  // For run-query actions: the openCypher to execute against the page's graph.
  query?: string | null;
}

export interface AssistantReply {
  text: string;
  proposal?: AssistantProposal | null;
  jumps?: AssistantJump[];
  actions?: AssistantAction[];
  question?: string | null;
}

export interface AssistantPageContext {
  page: string;
  project_id?: string | null;
  // Import page's selected Athena catalog/database, so the agent can generate an
  // import without re-asking for what the form already shows.
  catalog?: string | null;
  database?: string | null;
  // Import page state so the agent knows a projection/graph already exists: the
  // loaded projection id, its import status, the created graph id, and the
  // node/edge queries that define the current graph model.
  projection_id?: string | null;
  graph_status?: string | null;
  graph_id?: string | null;
  node_queries?: AssistantSqlQuery[];
  edge_queries?: AssistantSqlQuery[];
  // Graph Queries (openCypher) currently on the page, plus whether the page can
  // run them — set on the Details page so the agent may offer "Run Query" buttons.
  graph_queries?: AssistantCypherQuery[];
  can_run_queries?: boolean | null;
  actions?: { key: string; label: string; enabled?: boolean }[];
  graph_targets?: { id: string; name: string; actions: string[] }[];
}

export interface AssistantMessagePayload {
  text: string;
  session_id?: string | null;
  page_context?: AssistantPageContext | null;
  model?: string | null;
}

export const assistantApi = {
  session: () => request<{ session_id: string }>("/assistant/session", { method: "POST" }),
  message: (payload: AssistantMessagePayload) =>
    request<AssistantReply>("/assistant/message", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    }),
  models: () => request<{ models: string[]; default: string }>("/assistant/models"),
};

export const projectApi = {
  list: () => request<Project[]>("/project"),
  create: (name: string) => request<Project>("/project", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name }) }),
  delete: (id: string) => request<{ id: string; status: string }>(`/project/${id}`, { method: "DELETE" }),
  export: (id: string) => request<unknown>(`/project/${id}/export`),
  importProject: (data: unknown) => request<{ imported: { id: string; name: string } }>("/project/import", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) }),
  exportToS3: (id: string) => request<{ filename: string; key: string }>(`/project/${id}/export/s3`, { method: "POST" }),
  listS3Exports: () => request<{ files: { key: string; filename: string; last_modified: string }[] }>("/project/import/s3/list"),
  importFromS3: (key: string) => request<{ imported: { id: string; name: string } }>("/project/import/s3", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key }) }),
};
