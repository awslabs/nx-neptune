/**
 * Why an action is unavailable, as a sentence a user can act on.
 *
 * A disabled control that gives no reason makes the user guess which of several
 * conditions is unmet. These helpers live outside their components so the
 * wording is testable on its own, and so the ordering stays explicit: the
 * reason reported is the first unmet condition in the order the user meets them.
 *
 * **These functions are the only source of truth for whether a control is
 * disabled.** Callers spell `disabled={reason !== null}` rather than repeating
 * the conditions, because a hand-written `disabled` beside a derived reason is
 * two implementations of one rule and they drift: you get an enabled button
 * carrying a blocking explanation, or a disabled one with no explanation at all,
 * which is the bug this module exists to prevent.
 */

/** How often to re-poll while a graph is mid-transition, in milliseconds.
 *
 * Polling only runs while something is actually transitioning, so a short
 * interval costs little — and at half a minute, clicking Stop looks like it did
 * nothing and invites a second click.
 */
export const TRANSIENT_POLL_MS = 5000;

/** Graph states that are mid-transition: no action can be taken until they settle. */
export const TRANSIENT_STATUSES = ["STOPPING", "STARTING", "DELETING", "CREATING"];

/** Whether a graph status is mid-transition. One definition, not one per page. */
export function isTransientStatus(status?: string | null): boolean {
  return !!status && TRANSIENT_STATUSES.includes(status);
}

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

export type GraphAction = "explore" | "stop" | "start" | "delete" | "delete-graph";

/**
 * Why a graph action is unavailable.
 *
 * `available` is the action list the API reported for this graph, or `null` when
 * it has not been fetched yet or the fetch failed. The distinction matters:
 * an empty list means "the service says you cannot", while `null` means "we do
 * not know". Reporting the former for the latter told the user an AVAILABLE
 * graph could not be stopped, on every page load and permanently after a failed
 * request.
 */
export function graphActionBlockedReason(state: {
  action: GraphAction;
  graphId?: string | null;
  graphStatus?: string | null;
  available: string[] | null;
  isTransient: boolean;
  projectionStatus?: string | null;
}): string | null {
  // Two fallbacks on purpose, because they answer different questions. For a
  // graph mid-transition the unknown state is still "changing"; for one that
  // merely is not AVAILABLE, "unavailable" is the accurate word. Collapsing
  // them tells a CREATING graph with no status yet that it is unavailable.
  const status = state.graphStatus?.toLowerCase() || "unavailable";
  const transientStatus = state.graphStatus?.toLowerCase() || "changing";

  if (!state.graphId) {
    return state.action === "explore"
      ? "No graph yet — run the import first"
      : "No graph attached to this projection";
  }

  const isDelete = state.action === "delete" || state.action === "delete-graph";
  if (
    isDelete &&
    (state.projectionStatus === "importing" || state.projectionStatus === "executing")
  ) {
    return "An import is running; wait for it to finish";
  }

  if (state.isTransient) {
    return `Graph is ${transientStatus}; wait for it to settle`;
  }

  if (state.action === "explore") {
    return state.graphStatus !== "AVAILABLE"
      ? `Graph is ${status}; it must be available to explore`
      : null;
  }

  // `delete-graph` is the projection-side teardown: it goes through the
  // projection API, not the graph action list, so `available` says nothing
  // about it. Everything that gates it has already been checked above.
  if (state.action === "delete-graph") return null;

  // Not known yet: say so rather than asserting the service refused.
  if (state.available === null) {
    return "Checking what this graph allows...";
  }

  if (state.available.includes(state.action)) return null;

  switch (state.action) {
    case "stop":
      return "Graph cannot be stopped in its current state";
    case "start":
      return "Graph cannot be started in its current state";
    default:
      return "Graph cannot be deleted in its current state";
  }
}
