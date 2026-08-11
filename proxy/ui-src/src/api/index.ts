const BASE = "/api/v0";

// The proxy delivers this run's token via the launch URL's query string
// (e.g. http://127.0.0.1:8080/?token=...). We read it once at module load,
// strip it from the address bar (so it doesn't linger in history/referer),
// and keep it in memory only — no cookie, no localStorage/sessionStorage.
//
// Consequences (intentional): in-app navigation keeps the token (the JS
// runtime and this module variable persist); a full page reload clears it,
// after which the operator must reopen the launch URL.
const PROXY_TOKEN = (() => {
  const params = new URLSearchParams(window.location.search);
  const token = params.get("token");
  if (token) {
    // Remove ?token=... from the URL without reloading the page.
    params.delete("token");
    const query = params.toString();
    const newUrl =
      window.location.pathname +
      (query ? `?${query}` : "") +
      window.location.hash;
    window.history.replaceState(window.history.state, "", newUrl);
  }
  return token;
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
  sql_query?: string;
  node_query?: string;
  edge_query?: string;
  graph_name?: string;
  graph_id?: string;
  graph_endpoint?: string;
  graph_memory_gb: number;
  s3_staging_bucket?: string;
  project_id?: string;
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
  delete: (id: string) => request<{ id: string; status: string }>(`/projection/${id}`, { method: "DELETE" }),
  deleteGraph: (id: string) => request<{ id: string; status: string }>(`/projection/${id}/delete-graph`, { method: "POST" }),
};

// --- Project ---

export interface Project {
  id: string;
  name: string;
  status: string;
  created_at: string;
}

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
