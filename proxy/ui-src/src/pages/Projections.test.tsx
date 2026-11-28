import { act, render, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({
  projection: { list: vi.fn(), getQueries: vi.fn() },
  metadata: { config: vi.fn(), graphs: vi.fn(), graphSummary: vi.fn() },
  projectApi: { list: vi.fn() },
  graphActions: { getActions: vi.fn(), perform: vi.fn(), dismissInflight: vi.fn() },
}));

import { graphActions, metadata, projection, projectApi, type Projection } from "../api";
import { Projections } from "./Projections";

function makeProjection(id: string, graphId: string): Projection {
  return {
    id,
    status: "complete",
    catalog: "AwsDataCatalog",
    graph_id: graphId,
    graph_name: `nxp-${id}`,
    graph_memory_gb: 8,
    project_id: "p-1",
    progress: 100,
    created_at: "2026-01-01T00:00:00Z",
  };
}

async function renderAndSettle() {
  render(
    <MemoryRouter>
      <Projections />
    </MemoryRouter>,
  );
  // Wait for load() to reach the per-graph actions, then let the summary step finish.
  await waitFor(() => expect(graphActions.getActions).toHaveBeenCalled());
  await act(async () => {
    await new Promise((resolve) => setTimeout(resolve, 0));
  });
}

// A summary call goes to the graph itself and wakes a graph session (billable),
// so load() must only ask for counts of graphs that are not sessions.
describe("Projections summaries", () => {
  beforeEach(() => {
    vi.mocked(metadata.config).mockResolvedValue({
      region: "us-west-2",
      graph_prefix: "nxp-",
      session_memory_sizes: [4, 8],
      memory_sizes: [4, 8, 16],
    });
    vi.mocked(metadata.graphSummary).mockResolvedValue({
      numNodes: 1,
      numEdges: 2,
      nodeLabels: [],
      edgeLabels: [],
    });
    vi.mocked(projectApi.list).mockResolvedValue([]);
    vi.mocked(graphActions.getActions).mockResolvedValue({
      graph_id: "g-1",
      status: "AVAILABLE",
      actions: [],
      inflight: null,
    });
  });

  it.each([
    { graph_type: "provisioned", calls: 1 },
    { graph_type: "session", calls: 0 },
    { graph_type: "unknown", calls: 0 },
  ] as const)("$graph_type graph: $calls summary call(s)", async ({ graph_type, calls }) => {
    vi.mocked(projection.list).mockResolvedValue([makeProjection("a", "g-1")]);
    vi.mocked(metadata.graphs).mockResolvedValue({
      graphs: [{ id: "g-1", name: "nxp-a", status: "AVAILABLE", graph_type }],
    });

    await renderAndSettle();

    expect(metadata.graphSummary).toHaveBeenCalledTimes(calls);
    if (calls) expect(metadata.graphSummary).toHaveBeenCalledWith("g-1");
  });

  it("only skips the session graph when both kinds are listed", async () => {
    vi.mocked(projection.list).mockResolvedValue([
      makeProjection("tiny", "g-tiny"),
      makeProjection("big", "g-big"),
    ]);
    vi.mocked(metadata.graphs).mockResolvedValue({
      graphs: [
        { id: "g-tiny", name: "nxp-tiny", status: "AVAILABLE", graph_type: "session" },
        { id: "g-big", name: "nxp-big", status: "AVAILABLE", graph_type: "provisioned" },
      ],
    });

    await renderAndSettle();

    expect(metadata.graphSummary).toHaveBeenCalledTimes(1);
    expect(metadata.graphSummary).toHaveBeenCalledWith("g-big");
  });
});
