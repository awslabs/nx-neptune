/**
 * Why an action is unavailable, as a sentence a user can act on.
 *
 * A disabled control that gives no reason makes the user guess which of several
 * conditions is unmet. These helpers live outside their components so the
 * wording is testable on its own, and so the ordering stays explicit: the
 * reason reported is the first unmet condition in the order the user meets them.
 */

/** How often to re-poll while a graph is mid-transition, in milliseconds.
 *
 * This was 30s, which is long enough that clicking Stop looks like it did
 * nothing and invites a second click. Polling only runs while something is
 * actually transitioning, so a short interval costs little.
 */
export const TRANSIENT_POLL_MS = 5000;

/**
 * Why Execute is unavailable.
 *
 * Execute and Preview are gated differently and must not share a reason: a
 * running import blocks another Execute but not a Preview, so reporting
 * Execute's reason on Preview claims a block that is not there.
 */
export function executeBlockedReason(state: {
  polling: boolean;
  loading: boolean;
  hasQueries: boolean;
}): string | null {
  if (state.polling) return "An import is already running for this projection.";
  return previewBlockedReason(state);
}

/** Why Preview is unavailable. Deliberately ignores `polling`. */
export function previewBlockedReason(state: {
  loading: boolean;
  hasQueries: boolean;
}): string | null {
  if (state.loading) return "Waiting for the current operation to finish.";
  if (!state.hasQueries) return "Add at least one node or edge query first.";
  return null;
}

/** Whether `executeBlockedReason`'s inputs mean Execute is disabled at all. */
export function isExecuteBlocked(state: {
  polling: boolean;
  loading: boolean;
  hasQueries: boolean;
}): boolean {
  return executeBlockedReason(state) !== null;
}

/** Whether `previewBlockedReason`'s inputs mean Preview is disabled at all. */
export function isPreviewBlocked(state: {
  loading: boolean;
  hasQueries: boolean;
}): boolean {
  return previewBlockedReason(state) !== null;
}

export function graphActionBlockedReason(state: {
  action: "explore" | "stop" | "start" | "delete-graph";
  graphId?: string | null;
  graphStatus?: string | null;
  available: string[];
  isTransient: boolean;
  projectionStatus?: string | null;
}): string | null {
  const status = state.graphStatus?.toLowerCase() || "unavailable";

  if (!state.graphId) {
    return state.action === "explore"
      ? "No graph yet — run the import first"
      : "No graph attached to this projection";
  }

  if (
    state.action === "delete-graph" &&
    (state.projectionStatus === "importing" || state.projectionStatus === "executing")
  ) {
    return "An import is running; wait for it to finish";
  }

  if (state.isTransient) {
    return `Graph is ${state.graphStatus?.toLowerCase() || "changing"}; wait for it to settle`;
  }

  if (state.action === "explore") {
    return state.graphStatus !== "AVAILABLE"
      ? `Graph is ${status}; it must be available to explore`
      : null;
  }

  if (state.action === "stop" || state.action === "start") {
    return state.available.includes(state.action)
      ? null
      : `Graph cannot be ${state.action === "stop" ? "stopped" : "started"} in its current state`;
  }

  return null;
}
