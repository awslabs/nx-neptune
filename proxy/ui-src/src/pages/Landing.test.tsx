import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";

const navigate = vi.fn();
const list = vi.fn();

vi.mock("react-router", () => ({
  useNavigate: () => navigate,
}));

vi.mock("../api", () => ({
  projectApi: {
    list: () => list(),
  },
}));

async function renderLanding() {
  vi.resetModules();
  const { Landing } = await import("./Landing");
  return render(<Landing />);
}

/** A promise the test resolves by hand, so the pending state can be asserted. */
function deferred<T>() {
  let resolve!: (v: T) => void;
  let reject!: (e: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("Landing", () => {
  beforeEach(() => {
    navigate.mockReset();
    list.mockReset();
  });

  it("shows a loading state while it decides where to go", async () => {
    // It used to render null here, so a new user's first impression of the
    // product was a blank page for the length of an API round trip.
    const d = deferred<unknown[]>();
    list.mockReturnValue(d.promise);

    await renderLanding();

    expect(screen.getByLabelText("Loading")).toBeInTheDocument();
    expect(screen.getByLabelText("Loading")).toHaveAttribute("aria-busy", "true");

    d.resolve([]);
  });

  it("sends a visitor with no projects to the projects page", async () => {
    list.mockResolvedValue([]);

    await renderLanding();
    await vi.waitFor(() =>
      expect(navigate).toHaveBeenCalledWith("/projects", { replace: true }),
    );
  });

  it("sends a visitor with projects to the projections page", async () => {
    list.mockResolvedValue([{ id: "p1", name: "P", status: "active" }]);

    await renderLanding();
    await vi.waitFor(() =>
      expect(navigate).toHaveBeenCalledWith("/projections", { replace: true }),
    );
  });

  it("ignores projects that are being deleted when deciding", async () => {
    // A workspace whose only project is mid-deletion is effectively empty.
    list.mockResolvedValue([{ id: "p1", name: "P", status: "deleting" }]);

    await renderLanding();
    await vi.waitFor(() =>
      expect(navigate).toHaveBeenCalledWith("/projects", { replace: true }),
    );
  });

  it("surfaces a failure instead of sitting on the loading state", async () => {
    // Before, the call had no catch at all: a failed request left the blank
    // page up permanently, indistinguishable from a slow network.
    list.mockRejectedValue(new Error("Request failed: 503"));

    await renderLanding();

    await vi.waitFor(() =>
      expect(screen.getByText("Request failed: 503")).toBeInTheDocument(),
    );
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    expect(screen.queryByLabelText("Loading")).not.toBeInTheDocument();
    expect(navigate).not.toHaveBeenCalled();
  });
});
