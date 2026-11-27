import type { Page, Route } from "@playwright/test";

// Fake proxy API for browser tests. Nothing here calls AWS.
// Every request the UI makes under /api/v0 is answered from `state`. A request
// with no handler is recorded in `unmocked` and answered with 501, and the
// `api` fixture fails the test, so a new API call in the UI forces a mock update.

export interface MockProject {
  id: string;
  name: string;
  status: string;
  created_at: string;
}

export interface MockProjection {
  id: string;
  status: string;
  catalog: string;
  database?: string;
  graph_name?: string;
  graph_id?: string;
  graph_memory_gb: number;
  s3_staging_bucket?: string;
  project_id: string;
  progress: number;
  created_at: string;
  [key: string]: unknown;
}

export interface MockGraph {
  id: string;
  name: string;
  status: string;
}

export interface MockStatus {
  id: string;
  status: string;
  step?: string;
  step_label?: string;
  progress: number;
  error?: string;
  graph_endpoint?: string;
}

export interface RecordedCall {
  method: string;
  path: string; // relative to /api/v0, without the query string
  body: any;
}

export interface MockState {
  config: { region: string; graph_prefix: string };
  catalogs: { name: string; status: string }[];
  databases: string[];
  buckets: string[];
  projects: MockProject[];
  projections: MockProjection[];
  graphs: MockGraph[];
  // Responses for GET /projection/:id/status, in order. The last one repeats.
  statusSequence: MockStatus[];
  // Set to make POST /projection/:id/execute fail with this message.
  executeError?: string;
}

export const CREATED_AT = "2026-01-15T10:00:00Z";

export function defaultState(): MockState {
  return {
    config: { region: "us-west-2", graph_prefix: "nxp-" },
    catalogs: [{ name: "AwsDataCatalog", status: "ACTIVE" }],
    databases: ["finance", "sales"],
    buckets: ["e2e-staging"],
    projects: [{ id: "p1", name: "Alpha", status: "active", created_at: CREATED_AT }],
    projections: [
      {
        id: "s1111111-0000-0000-0000-000000000000",
        status: "complete",
        catalog: "AwsDataCatalog",
        database: "finance",
        graph_name: "fraud-demo",
        graph_id: "g-abc123",
        graph_memory_gb: 16,
        s3_staging_bucket: "s3://e2e-staging/",
        project_id: "p1",
        progress: 100,
        created_at: CREATED_AT,
      },
    ],
    graphs: [{ id: "g-abc123", name: "nxp-fraud-demo", status: "AVAILABLE" }],
    statusSequence: [
      { id: "new-1", status: "importing", step: "load", step_label: "Loading data", progress: 40 },
      {
        id: "new-1",
        status: "complete",
        progress: 100,
        graph_endpoint: "g-new123.us-west-2.neptune-graph.amazonaws.com",
      },
    ],
  };
}

export interface MockApi {
  state: MockState;
  calls: RecordedCall[];
  unmocked: string[];
  /** Calls matching a method and a path (string or regex). */
  find(method: string, path: string | RegExp): RecordedCall[];
}

const json = (route: Route, body: unknown, status = 200) =>
  route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });

export async function createMockApi(page: Page, overrides: Partial<MockState> = {}): Promise<MockApi> {
  const state: MockState = { ...defaultState(), ...overrides };
  const calls: RecordedCall[] = [];
  const unmocked: string[] = [];
  let statusCalls = 0;
  let nextProjectionId = 1;

  const api: MockApi = {
    state,
    calls,
    unmocked,
    find: (method, path) =>
      calls.filter(
        (c) => c.method === method && (typeof path === "string" ? c.path === path : path.test(c.path)),
      ),
  };

  await page.route("**/api/v0/**", async (route) => {
    const req = route.request();
    const method = req.method();
    const url = new URL(req.url());
    const path = url.pathname.replace(/^\/api\/v0/, "");
    let body: any = undefined;
    const raw = req.postData();
    if (raw) {
      try {
        body = JSON.parse(raw);
      } catch {
        body = raw;
      }
    }
    calls.push({ method, path, body });

    let m: RegExpMatchArray | null;

    // --- metadata ---
    if (method === "GET" && path === "/metadata/config") return json(route, state.config);
    if (method === "GET" && path === "/metadata/athena/catalogs") return json(route, { catalogs: state.catalogs });
    if (method === "GET" && path === "/metadata/athena/databases") return json(route, { databases: state.databases });
    if (method === "GET" && path === "/metadata/s3/buckets") return json(route, { buckets: state.buckets });
    if (method === "GET" && path === "/metadata/neptune/graph-analytics") return json(route, { graphs: state.graphs });
    if (method === "GET" && /^\/metadata\/neptune\/graph-analytics\/[^/]+\/summary$/.test(path)) {
      return json(route, { numNodes: 1200, numEdges: 3400, nodeLabels: ["account"], edgeLabels: ["transfer"] });
    }

    // --- graph actions ---
    if (method === "GET" && (m = path.match(/^\/graphs\/([^/]+)\/actions$/))) {
      return json(route, { graph_id: m[1], status: "AVAILABLE", actions: ["stop", "delete"], inflight: null });
    }

    // --- projects ---
    if (method === "GET" && path === "/project") return json(route, state.projects);
    if (method === "POST" && path === "/project") {
      const project: MockProject = {
        id: `p${state.projects.length + 1}`,
        name: body.name,
        status: "active",
        created_at: CREATED_AT,
      };
      state.projects.push(project);
      return json(route, project);
    }

    // --- projections ---
    if (method === "GET" && path === "/projection") return json(route, state.projections);
    if (method === "POST" && path === "/projection") {
      const p: MockProjection = {
        status: "draft",
        progress: 0,
        created_at: CREATED_AT,
        catalog: "AwsDataCatalog",
        graph_memory_gb: 16,
        project_id: "",
        ...body,
        id: `new-${nextProjectionId++}`,
      };
      state.projections.push(p);
      return json(route, p);
    }
    if ((m = path.match(/^\/projection\/([^/]+)$/))) {
      const found = state.projections.find((p) => p.id === m![1]);
      if (method === "GET" && found) return json(route, found);
      if (method === "PUT" && found) return json(route, Object.assign(found, body));
    }
    if ((m = path.match(/^\/projection\/([^/]+)\/queries$/))) {
      if (method === "GET") return json(route, { node_queries: [], edge_queries: [] });
      if (method === "PUT") {
        return json(route, {
          node_queries: (body.node_queries ?? []).map((q: any, i: number) => ({ id: `nq${i}`, sql: q.sql, position: i })),
          edge_queries: (body.edge_queries ?? []).map((q: any, i: number) => ({ id: `eq${i}`, sql: q.sql, position: i })),
        });
      }
    }
    if (method === "POST" && /^\/projection\/[^/]+\/execute$/.test(path)) {
      if (state.executeError) return json(route, { message: state.executeError }, 500);
      return json(route, { message: "started" });
    }
    if (method === "GET" && /^\/projection\/[^/]+\/status$/.test(path)) {
      const i = Math.min(statusCalls++, state.statusSequence.length - 1);
      return json(route, state.statusSequence[i]);
    }

    unmocked.push(`${method} ${path}`);
    return json(route, { message: `Unmocked API call: ${method} ${path}` }, 501);
  });

  return api;
}
