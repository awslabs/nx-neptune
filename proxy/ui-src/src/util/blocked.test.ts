import { describe, expect, it } from "vitest";

import {
  TRANSIENT_POLL_MS,
  executeBlockedReason,
  graphActionBlockedReason,
  isExecuteBlocked,
  isPreviewBlocked,
  previewBlockedReason,
} from "./blocked";

describe("executeBlockedReason", () => {
  const ready = { polling: false, loading: false, hasQueries: true };

  it("returns null when nothing is in the way", () => {
    expect(executeBlockedReason(ready)).toBeNull();
    expect(isExecuteBlocked(ready)).toBe(false);
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

  it("agrees with isExecuteBlocked on every combination", () => {
    for (const polling of [true, false]) {
      for (const loading of [true, false]) {
        for (const hasQueries of [true, false]) {
          const state = { polling, loading, hasQueries };
          expect(isExecuteBlocked(state)).toBe(
            executeBlockedReason(state) !== null,
          );
        }
      }
    }
  });
});

describe("previewBlockedReason", () => {
  it("does not treat a running import as a block", () => {
    // Preview starts no import, so it stays available during one. Sharing
    // Execute's reason here claimed a block that was not there while the button
    // remained clickable.
    expect(previewBlockedReason({ loading: false, hasQueries: true })).toBeNull();
    expect(isPreviewBlocked({ loading: false, hasQueries: true })).toBe(false);
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

  it("agrees with isPreviewBlocked on every combination", () => {
    for (const loading of [true, false]) {
      for (const hasQueries of [true, false]) {
        const state = { loading, hasQueries };
        expect(isPreviewBlocked(state)).toBe(
          previewBlockedReason(state) !== null,
        );
      }
    }
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
    // Was 30s, which is long enough that clicking Stop looks like it did
    // nothing. This pins the number against an accidental revert.
    expect(TRANSIENT_POLL_MS).toBeLessThanOrEqual(5000);
    expect(TRANSIENT_POLL_MS).toBeGreaterThan(0);
  });
});
