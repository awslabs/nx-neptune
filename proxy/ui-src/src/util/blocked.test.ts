import { describe, expect, it } from "vitest";

import {
  TRANSIENT_POLL_MS,
  executeBlockedReason,
  graphActionBlockedReason,
  isTransientStatus,
  previewBlockedReason,
} from "./blocked";

describe("executeBlockedReason", () => {
  const ready = { polling: false, loading: false, hasQueries: true };

  it("returns null when nothing is in the way", () => {
    expect(executeBlockedReason(ready)).toBeNull();
  });

  it("names a running import", () => {
    expect(executeBlockedReason({ ...ready, polling: true })).toMatch(
      /already running/,
    );
  });

  it("names a pending operation", () => {
    expect(executeBlockedReason({ ...ready, loading: true })).toMatch(
      /current operation/,
    );
  });

  it("names the missing queries", () => {
    expect(executeBlockedReason({ ...ready, hasQueries: false })).toMatch(
      /node or edge query/,
    );
  });

  it("reports the first condition a user would hit, not the last", () => {
    expect(
      executeBlockedReason({ polling: true, loading: true, hasQueries: false }),
    ).toMatch(/already running/);
  });

});

describe("previewBlockedReason", () => {
  it("does not treat a running import as a block", () => {
    // Preview starts no import, so it stays available during one. Sharing
    // Execute's reason here claimed a block that was not there while the button
    // remained clickable.
    expect(previewBlockedReason({ loading: false, hasQueries: true })).toBeNull();
  });

  it("still names a pending operation", () => {
    expect(previewBlockedReason({ loading: true, hasQueries: true })).toMatch(
      /current operation/,
    );
  });

  it("still names the missing queries", () => {
    expect(previewBlockedReason({ loading: false, hasQueries: false })).toMatch(
      /node or edge query/,
    );
  });

  it("differs from Execute only on the polling case", () => {
    // The two share their ordering; polling is the single difference, and
    // pinning that keeps them from drifting apart in either direction.
    for (const loading of [true, false]) {
      for (const hasQueries of [true, false]) {
        const base = { loading, hasQueries };
        expect(previewBlockedReason(base)).toBe(
          executeBlockedReason({ ...base, polling: false }),
        );
      }
    }
    expect(
      executeBlockedReason({ polling: true, loading: false, hasQueries: true }),
    ).not.toBeNull();
    expect(previewBlockedReason({ loading: false, hasQueries: true })).toBeNull();
  });

});

describe("graphActionBlockedReason", () => {
  const available = {
    graphId: "g-1",
    graphStatus: "AVAILABLE",
    available: ["stop"],
    isTransient: false,
  };

  it("allows exploring an available graph", () => {
    expect(
      graphActionBlockedReason({ ...available, action: "explore" }),
    ).toBeNull();
  });

  it("tells a user to run the import when exploring with no graph", () => {
    expect(
      graphActionBlockedReason({
        ...available,
        action: "explore",
        graphId: null,
      }),
    ).toMatch(/run the import first/);
  });

  it("names the status when a graph is not available to explore", () => {
    expect(
      graphActionBlockedReason({
        ...available,
        action: "explore",
        graphStatus: "STOPPED",
      }),
    ).toMatch(/stopped/);
  });

  it("tells a user to wait while a graph is transitioning", () => {
    expect(
      graphActionBlockedReason({
        ...available,
        action: "stop",
        graphStatus: "STOPPING",
        isTransient: true,
      }),
    ).toMatch(/wait for it to settle/);
  });

  it("allows an action the service reports as available", () => {
    expect(graphActionBlockedReason({ ...available, action: "stop" })).toBeNull();
  });

  it("refuses an action the service does not report", () => {
    expect(
      graphActionBlockedReason({ ...available, action: "start" }),
    ).toMatch(/cannot be started/);
  });

  it("blocks deleting a graph while an import is running", () => {
    // This is the one that matters most: deleting mid-import is the case the
    // backend guards with a 409, so the button should say so rather than fail.
    expect(
      graphActionBlockedReason({
        ...available,
        action: "delete-graph",
        projectionStatus: "importing",
      }),
    ).toMatch(/import is running/);
  });

  it("prefers the import message over the transient one when both apply", () => {
    expect(
      graphActionBlockedReason({
        ...available,
        action: "delete-graph",
        projectionStatus: "executing",
        isTransient: true,
      }),
    ).toMatch(/import is running/);
  });

  it("reports a missing graph before anything else", () => {
    expect(
      graphActionBlockedReason({
        ...available,
        action: "delete-graph",
        graphId: null,
        projectionStatus: "importing",
        isTransient: true,
      }),
    ).toMatch(/No graph attached/);
  });

  it("does not render 'undefined' when the status is unknown", () => {
    const reason = graphActionBlockedReason({
      ...available,
      action: "stop",
      graphStatus: null,
      isTransient: true,
    });
    expect(reason).not.toMatch(/undefined|null/);
    expect(reason).toMatch(/changing/);
  });
});

describe("TRANSIENT_POLL_MS", () => {
  it("is short enough that a state change is visible", () => {
    // A long interval makes clicking Stop look like it did nothing, which
    // invites a second click. This pins the number against an accidental revert.
    expect(TRANSIENT_POLL_MS).toBeLessThanOrEqual(5000);
    expect(TRANSIENT_POLL_MS).toBeGreaterThan(0);
  });
});

describe("an unknown action list", () => {
  const base = {
    graphId: "g-1",
    graphStatus: "AVAILABLE",
    isTransient: false,
  };

  it("says it is still checking rather than claiming a refusal", () => {
    // `[]` means "the service says no"; `null` means "we have not asked yet, or
    // the request failed". Conflating them told the user an AVAILABLE graph
    // could not be stopped — on every page load, and forever after a failure.
    for (const action of ["stop", "start"] as const) {
      const reason = graphActionBlockedReason({ ...base, action, available: null });
      expect(reason).toMatch(/Checking/);
      expect(reason).not.toMatch(/cannot be/);
    }
  });

  it("still reports a genuine refusal when the list is known and empty", () => {
    expect(
      graphActionBlockedReason({ ...base, action: "stop", available: [] }),
    ).toMatch(/cannot be stopped/);
  });

  it("does not mask a reason that outranks it", () => {
    // Transient and no-graph come first: an unknown action list must not hide
    // the more specific thing that is actually in the way.
    expect(
      graphActionBlockedReason({
        ...base,
        action: "stop",
        available: null,
        isTransient: true,
        graphStatus: "STOPPING",
      }),
    ).toMatch(/wait for it to settle/);
    expect(
      graphActionBlockedReason({ ...base, action: "stop", available: null, graphId: null }),
    ).toMatch(/No graph/);
  });
});

describe("the delete action", () => {
  const base = { graphId: "g-1", graphStatus: "AVAILABLE", isTransient: false };

  it("is gated by the action list, like stop and start", () => {
    // The Graphs page's Delete goes through the graph action API, so the
    // service's list is authoritative for it.
    expect(
      graphActionBlockedReason({ ...base, action: "delete", available: ["delete"] }),
    ).toBeNull();
    expect(
      graphActionBlockedReason({ ...base, action: "delete", available: [] }),
    ).toMatch(/cannot be deleted/);
  });

  it("is not confused with the projection-side delete-graph", () => {
    // `delete-graph` is the projection teardown and goes through a different
    // API, so the graph action list says nothing about it. Gating it on
    // `available` would have disabled it permanently.
    expect(
      graphActionBlockedReason({ ...base, action: "delete-graph", available: [] }),
    ).toBeNull();
  });

  it("still refuses either delete while an import is running", () => {
    for (const action of ["delete", "delete-graph"] as const) {
      expect(
        graphActionBlockedReason({
          ...base,
          action,
          available: ["delete"],
          projectionStatus: "importing",
        }),
      ).toMatch(/import is running/);
    }
  });
});

describe("isTransientStatus", () => {
  it("recognises the mid-transition states", () => {
    for (const s of ["STOPPING", "STARTING", "DELETING", "CREATING"]) {
      expect(isTransientStatus(s)).toBe(true);
    }
  });

  it("treats a settled or absent status as not transient", () => {
    for (const s of ["AVAILABLE", "STOPPED", undefined, null, ""]) {
      expect(isTransientStatus(s)).toBe(false);
    }
  });
});
