import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";

/**
 * The Graphs page's own share of the "silent disabled controls / blanking
 * table" work, which the first pass covered only on Projections and the poll.
 *
 * 1. Its four action buttons were `title="Stop graph"` and friends — static
 *    labels, so a greyed-out Delete explained nothing.
 * 2. `performAction` refreshed without `background`, so clicking Stop blanked
 *    the table the user was looking at. That is the commonest trigger, not the
 *    poll the first pass fixed.
 * 3. `load` had no `try/finally`, so a failed first request left "Loading..."
 *    up forever with nothing to retry.
 */

const graphs = vi.fn();
const config = vi.fn();
const graphSummary = vi.fn();
const getActions = vi.fn();
const perform = vi.fn();

vi.mock("react-router", () => ({
  NavLink: ({ children }: { children?: unknown }) => <>{children}</>,
  useNavigate: () => vi.fn(),
  useSearchParams: () => [new URLSearchParams(), vi.fn()],
}));

vi.mock("../api", () => ({
  metadata: {
    graphs: () => graphs(),
    graphSummary: (id: string) => graphSummary(id),
    config: () => config(),
  },
  graphActions: {
    getActions: (id: string) => getActions(id),
    perform: (id: string, action: string) => perform(id, action),
    dismissInflight: vi.fn(() => Promise.resolve()),
  },
}));

async function renderGraphs() {
  vi.resetModules();
  const { Graphs } = await import("./Graphs");
  return render(<Graphs />);
}

const graph = (status: string) => ({
  graphs: [
    { id: "g-1", name: "nxp-demo", status, provisionedMemory: 16, endpoint: "e" },
  ],
});

beforeEach(() => {
  // Stop and Delete ask for confirmation, and jsdom has no `confirm`.
  vi.stubGlobal("confirm", () => true);
  graphs.mockReset().mockResolvedValue(graph("AVAILABLE"));
  config.mockReset().mockResolvedValue({ region: "us-west-2", graph_prefix: "nxp-" });
  graphSummary.mockReset().mockResolvedValue({ numNodes: 1, numEdges: 2 });
  getActions.mockReset().mockResolvedValue({ actions: ["stop"], inflight: null });
  perform.mockReset().mockResolvedValue({});
});

describe("Graphs action reasons", () => {
  it("explains a disabled action instead of showing a static label", async () => {
    // CREATING: everything is blocked until it settles, and the button should
    // say so rather than reading "Delete graph" while greyed out.
    graphs.mockResolvedValue(graph("CREATING"));
    await renderGraphs();

    const del = await screen.findByLabelText("Delete graph");
    expect(del).toBeDisabled();
    expect(del.getAttribute("title")).toMatch(/wait for it to settle/);
  });

  it("does not claim an action is refused while the action list is loading", async () => {
    // `getActions` never resolves, so `available` is unknown rather than empty.
    // Reporting "cannot be stopped" here asserted a refusal nobody made.
    getActions.mockReturnValue(new Promise(() => {}));
    await renderGraphs();

    const stop = await screen.findByLabelText("Stop graph");
    expect(stop.getAttribute("title")).toMatch(/Checking/);
    expect(stop.getAttribute("title")).not.toMatch(/cannot be stopped/);
  });

  it("enables an action the service reports as available", async () => {
    await renderGraphs();

    const stop = await screen.findByLabelText("Stop graph");
    await waitFor(() => expect(stop).not.toBeDisabled());
    expect(stop.getAttribute("title")).toBe("Stop graph");
  });
});

describe("Graphs table stability", () => {
  it("keeps the table up while refreshing after an action", async () => {
    await renderGraphs();
    const stop = await screen.findByLabelText("Stop graph");
    await waitFor(() => expect(stop).not.toBeDisabled());

    await act(async () => {
      stop.click();
    });

    // The row must still be there: the refresh after the click used to set the
    // spinner and replace the whole table.
    expect(screen.getByText("nxp-demo")).toBeTruthy();
    expect(screen.queryByText("Loading...")).toBeNull();
    expect(perform).toHaveBeenCalledWith("g-1", "stop");
  });
});

describe("Graphs load failure", () => {
  it("surfaces an error with a retry instead of a permanent spinner", async () => {
    graphs.mockRejectedValue(new Error("Request failed: 503"));
    await renderGraphs();

    // Previously `setLoading(false)` was never reached, so this stayed on
    // "Loading..." with no error and nothing to click.
    await waitFor(() => expect(screen.getByRole("alert")).toBeTruthy());
    expect(screen.getByText("Request failed: 503")).toBeTruthy();
    expect(screen.queryByText("Loading...")).toBeNull();
    expect(screen.getByLabelText("Retry")).toBeTruthy();
  });

  it("recovers when a retry succeeds", async () => {
    graphs.mockRejectedValueOnce(new Error("Request failed: 503"));
    await renderGraphs();
    await waitFor(() => expect(screen.getByRole("alert")).toBeTruthy());

    graphs.mockResolvedValue(graph("AVAILABLE"));
    await act(async () => {
      screen.getByLabelText("Retry").click();
    });

    await waitFor(() => expect(screen.getByText("nxp-demo")).toBeTruthy());
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
