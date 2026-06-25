const BASE = "/api/v0";

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.message || `Request failed: ${res.status}`);
  }
  return res.json();
}

// --- Metadata ---

export const metadata = {
  config: () => request<{ region: string }>("/metadata/config"),
  catalogs: () => request<{ catalogs: { name: string; status: string }[] }>("/metadata/athena/catalogs"),
  databases: (catalog: string) => request<{ databases: string[] }>(`/metadata/athena/databases?catalog=${encodeURIComponent(catalog)}`),
  tables: (database: string, catalog: string) => request<{ tables: string[] }>(`/metadata/athena/tables?database=${encodeURIComponent(database)}&catalog=${encodeURIComponent(catalog)}`),
  columns: (database: string, table: string, catalog: string) => request<{ columns: { name: string; type: string }[] }>(`/metadata/athena/columns?database=${encodeURIComponent(database)}&table=${encodeURIComponent(table)}&catalog=${encodeURIComponent(catalog)}`),
  buckets: () => request<{ buckets: string[] }>("/metadata/s3/buckets"),
  graphs: () => request<{ graphs: { id: string; name: string; status: string }[] }>("/metadata/neptune/graph-analytics"),
  deleteGraph: (id: string) => request<{ id: string; status: string }>(`/metadata/neptune/graph-analytics/${id}`, { method: "DELETE" }),
  graphSummary: (id: string) => request<{ numNodes: number; numEdges: number; nodeLabels: string[]; edgeLabels: string[] }>(`/metadata/neptune/graph-analytics/${id}/summary`),
};

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
  workspace_id?: string;
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
};

// --- Workspace ---

export interface Workspace {
  id: string;
  name: string;
  created_at: string;
}

export const workspaceApi = {
  list: () => request<Workspace[]>("/workspace"),
  create: (name: string) => request<Workspace>("/workspace", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name }) }),
  delete: (id: string) => fetch(`${BASE}/workspace/${id}`, { method: "DELETE" }).then(r => { if (!r.ok) throw new Error("Delete failed"); }),
};
