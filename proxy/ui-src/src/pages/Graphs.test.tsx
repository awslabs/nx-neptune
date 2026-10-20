import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../api", () => ({
  metadata: { config: vi.fn(), graphs: vi.fn(), graphSummary: vi.fn() },
  graphActions: { getActions: vi.fn(), perform: vi.fn(), dismissInflight: vi.fn() },
}));

import { graphActions, metadata } from "../api";
import { Graphs } from "./Graphs";

// A summary call goes to the graph itself and wakes a graph session (billable),
// so the page must only ask for counts of graphs that are not sessions.
describe("Graphs summaries", () => {
  beforeEach(() => {
    vi.mocked(metadata.config).mockResolvedValue({
      region: "us-west-2",
      graph_prefix: "nxp-",
      session_memory_sizes: [4, 8],
      memory_sizes: [4, 8, 16],
    });
    vi.mocked(metadata.graphSummary).mockResolvedValue({
      numNodes: 1234,
      numEdges: 5678,
      nodeLabels: [],
      edgeLabels: [],
    });
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
    vi.mocked(metadata.graphs).mockResolvedValue({
      graphs: [{ id: "g-1", name: "nxp-demo", status: "AVAILABLE", graph_type }],
    });

    render(<Graphs />);
    await screen.findByText("nxp-demo");

    expect(metadata.graphSummary).toHaveBeenCalledTimes(calls);
    if (calls) {
      expect(metadata.graphSummary).toHaveBeenCalledWith("g-1");
      expect(await screen.findByText("1,234")).toBeInTheDocument();
    } else {
      // No counts are shown for a graph whose summary was skipped.
      expect(screen.queryByText("1,234")).not.toBeInTheDocument();
      expect(screen.getAllByText("—")).toHaveLength(2);
    }
  });

  it("only skips the session graph when both kinds are listed", async () => {
    vi.mocked(metadata.graphs).mockResolvedValue({
      graphs: [
        { id: "g-tiny", name: "nxp-tiny", status: "AVAILABLE", graph_type: "session" },
        { id: "g-big", name: "nxp-big", status: "AVAILABLE", graph_type: "provisioned" },
      ],
    });

    render(<Graphs />);
    await screen.findByText("nxp-tiny");

    expect(metadata.graphSummary).toHaveBeenCalledTimes(1);
    expect(metadata.graphSummary).toHaveBeenCalledWith("g-big");
  });
});
