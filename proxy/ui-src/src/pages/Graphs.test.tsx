import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";

const graphs = vi.fn();
const config = vi.fn();
const graphSummary = vi.fn();
const getActions = vi.fn();

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
    perform: vi.fn(),
    dismissInflight: vi.fn(() => Promise.resolve()),
  },
}));

async function renderGraphs() {
  vi.resetModules();
  const { Graphs } = await import("./Graphs");
  return render(<Graphs />);
}

const STOPPING = {
  graphs: [
    {
      id: "g-1",
      name: "nxp-demo",
      status: "STOPPING",
      provisionedMemory: 16,
      endpoint: "e",
    },
  ],
};

describe("Graphs polling", () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    graphs.mockReset();
    config.mockReset().mockResolvedValue({ region: "us-west-2", graph_prefix: "nxp-" });
    graphSummary.mockReset().mockResolvedValue({ numNodes: 0, numEdges: 0 });
    getActions.mockReset().mockResolvedValue({ actions: ["start"], inflight: null });
    graphs.mockResolvedValue(STOPPING);
  });

  it("does not replace the table with a loading state on a background poll", async () => {
    // The render swaps the whole table — Stop and Start included — for
    // "Loading...". Polling every 5s while a graph transitions therefore made
    // the table blink and pulled controls out from under the cursor, which is
    // what shortening the interval exposed.
    //
    // The assertion has to land WHILE the poll is in flight: if it waits for
    // the request to resolve, `loading` is already back to false and the test
    // passes against the broken version too.
    let releasePoll!: (v: unknown) => void;
    const pending = new Promise((res) => {
      releasePoll = res;
    });

    // Hold open every call after mount, rather than relying on how many times
    // mounting happens to call this.
    let mounted = false;
    graphs.mockImplementation(() => {
      if (!mounted) return Promise.resolve(STOPPING);
      return pending;
    });

    await renderGraphs();
    await waitFor(() => expect(screen.getByText("nxp-demo")).toBeInTheDocument());
    const atMount = graphs.mock.calls.length;
    mounted = true;

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    await waitFor(() =>
      expect(graphs.mock.calls.length).toBeGreaterThan(atMount),
    );

    // The poll is still outstanding here.
    expect(screen.queryByText("Loading...")).not.toBeInTheDocument();
    expect(screen.getByText("nxp-demo")).toBeInTheDocument();

    releasePoll(STOPPING);
  });

  it("polls while a graph is transitioning", async () => {
    await renderGraphs();
    await waitFor(() => expect(screen.getByText("nxp-demo")).toBeInTheDocument());

    const before = graphs.mock.calls.length;
    await vi.advanceTimersByTimeAsync(5000);

    await waitFor(() =>
      expect(graphs.mock.calls.length).toBeGreaterThan(before),
    );
  });

  it("does not poll when nothing is transitioning", async () => {
    graphs.mockResolvedValue({
      graphs: [{ ...STOPPING.graphs[0], status: "AVAILABLE" }],
    });

    await renderGraphs();
    await waitFor(() => expect(screen.getByText("nxp-demo")).toBeInTheDocument());

    const settled = graphs.mock.calls.length;
    await vi.advanceTimersByTimeAsync(15000);

    expect(graphs.mock.calls.length).toBe(settled);
  });
});
