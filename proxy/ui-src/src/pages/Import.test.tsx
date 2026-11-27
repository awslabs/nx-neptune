import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

/**
 * Covers the wiring, not the wording.
 *
 * `util/blocked.test.ts` already pins what each reason says. What it cannot
 * catch is a button being handed the *other* button's reason — which is exactly
 * the bug here: Preview showed Execute's "an import is already running" while
 * staying enabled, because both read one shared variable.
 */

const api = {
  config: vi.fn(),
  catalogs: vi.fn(),
  databases: vi.fn(),
  buckets: vi.fn(),
  list: vi.fn(),
  get: vi.fn(),
  getQueries: vi.fn(),
  saveQueries: vi.fn(),
  update: vi.fn(),
  status: vi.fn(),
  projects: vi.fn(),
  execute: vi.fn(),
};

vi.mock("react-router", () => ({
  useSearchParams: () => [new URLSearchParams("?projection=p-1"), vi.fn()],
}));

vi.mock("../api", () => ({
  metadata: {
    config: () => api.config(),
    catalogs: () => api.catalogs(),
    databases: (...a: unknown[]) => api.databases(...a),
    buckets: () => api.buckets(),
  },
  projection: {
    list: () => api.list(),
    get: (...a: unknown[]) => api.get(...a),
    getQueries: (...a: unknown[]) => api.getQueries(...a),
    saveQueries: (...a: unknown[]) => api.saveQueries(...a),
    update: (...a: unknown[]) => api.update(...a),
    status: (...a: unknown[]) => api.status(...a),
    create: vi.fn(),
    execute: (...a: unknown[]) => api.execute(...a),
    preview: vi.fn(),
    translate: vi.fn(),
    validate: vi.fn(),
    validateQuery: vi.fn(),
  },
  projectApi: { list: () => api.projects() },
}));

const PROJECTION = {
  id: "p-1",
  project_id: "proj-1",
  graph_name: "demo",
  catalog: "AwsDataCatalog",
  database: "db",
  s3_staging_bucket: "s3://bucket/staging/",
  graph_memory_gb: 16,
  status: "draft",
  progress: 0,
  property_graph: null,
};

async function renderImport() {
  vi.resetModules();
  const { Import } = await import("./Import");
  return render(<Import />);
}

describe("Import action reasons", () => {
  beforeEach(() => {
    Object.values(api).forEach((m) => m.mockReset());
    api.config.mockResolvedValue({ graph_prefix: "nxp-", region: "us-west-2" });
    api.catalogs.mockResolvedValue({ catalogs: ["AwsDataCatalog"] });
    api.databases.mockResolvedValue({ databases: ["db"] });
    api.buckets.mockResolvedValue({ buckets: ["bucket"] });
    api.projects.mockResolvedValue([{ id: "proj-1", name: "P", status: "active" }]);
    api.list.mockResolvedValue([PROJECTION]);
    api.get.mockResolvedValue(PROJECTION);
    api.saveQueries.mockResolvedValue({});
    api.update.mockResolvedValue(PROJECTION);
    api.status.mockResolvedValue({ ...PROJECTION, status: "draft" });
  });

  it("tells the user to add a query when there are none", async () => {
    api.getQueries.mockResolvedValue({ node_queries: [], edge_queries: [] });

    await renderImport();

    const execute = await waitFor(() =>
      screen.getByRole("button", { name: /Execute/ }),
    );
    expect(execute).toBeDisabled();
    expect(execute).toHaveAttribute(
      "title",
      expect.stringContaining("node or edge query"),
    );
  });

  it("gives Preview and Execute the same reason when neither can run", async () => {
    api.getQueries.mockResolvedValue({ node_queries: [], edge_queries: [] });

    await renderImport();

    const preview = await waitFor(() =>
      screen.getByRole("button", { name: /Preview/ }),
    );
    const execute = screen.getByRole("button", { name: /Execute/ });

    expect(preview.getAttribute("title")).toBe(execute.getAttribute("title"));
    expect(preview).toBeDisabled();
  });

  it("does not tell Preview an import blocks it", async () => {
    // The regression: with queries present and nothing else pending, Preview is
    // enabled, so its title must not claim it is blocked. Handing it Execute's
    // reason produced an enabled button advertising a block.
    api.getQueries.mockResolvedValue({
      node_queries: [{ id: "n1", sql: "SELECT 1", position: 0 }],
      edge_queries: [],
    });

    await renderImport();

    const preview = await waitFor(() =>
      screen.getByRole("button", { name: /Preview/ }),
    );
    expect(preview).toBeEnabled();
    expect(preview.getAttribute("title")).not.toMatch(/already running/);
    expect(preview.getAttribute("title")).toMatch(/Preview the first rows/);
  });

  // NOT COVERED: the wiring under a live polling state. `blocked.test.ts` pins
  // that Execute's reason includes the running-import case and Preview's does
  // not, and the tests above pin the wiring when nothing is polling — but with
  // `polling` false both reasons are identical, so swapping the two variables
  // at the call site is invisible to every test here. An attempt to drive
  // Execute and assert mid-import did not reach a polling state in jsdom and
  // was removed rather than left passing vacuously.
});
