import type { PageBridge, ChatMessage, JumpAction, ChatAction } from "./context";
import {
  assistantApi,
  type AssistantReply,
  type AssistantPageContext,
  type AssistantProposal,
} from "../api";

// Client bridge for the live agent backend (spec §9). It serializes the active
// page into the snake_case PageContext the agents consume, calls
// POST /assistant/message, and maps the reply into the client's ChatMessage —
// applying any import proposal through the page bridge setters. The assistant
// only *proposes*: jumps/actions become inline buttons the user clicks (§9.3).

// Build the snake_case PageContext (spec §9.4) from the active bridge. Only the
// keys/targets the page registered are sent, so the Page-Action agent cannot
// reference an action the page does not currently offer.
export function serializePageContext(
  bridge: PageBridge | null,
): AssistantPageContext | undefined {
  if (!bridge) return undefined;
  const actions = bridge.actions
    ? Object.entries(bridge.actions).map(([key, spec]) => ({
        key,
        label: spec.label,
        enabled: spec.enabled !== false,
      }))
    : [];
  const graph_targets = (bridge.graphTargets ?? []).map((g) => ({
    id: g.id,
    name: g.name,
    actions: g.actions,
  }));
  // Surface the Import form's current catalog/database so the agent can generate
  // an import without re-asking. Only forward non-empty string values.
  const asField = (v: unknown): string | null =>
    typeof v === "string" && v.trim() ? v : null;
  return {
    page: bridge.page,
    project_id: bridge.jumpContext?.projectId ?? null,
    catalog: asField(bridge.fields?.catalog),
    database: asField(bridge.fields?.database),
    actions,
    graph_targets,
  };
}

// Map server jumps (snake_case) → the client JumpAction union. open-projections
// needs a resolved project id; drop it if the agent didn't supply one.
function mapJumps(reply: AssistantReply): JumpAction[] {
  return (reply.jumps ?? []).flatMap((j): JumpAction[] => {
    switch (j.kind) {
      case "new-import":
        return [{ kind: "new-import", label: j.label, projectId: j.project_id ?? null }];
      case "new-project":
        return [{ kind: "new-project", label: j.label }];
      case "open-projections":
        return j.project_id
          ? [{ kind: "open-projections", label: j.label, projectId: j.project_id }]
          : [];
      default:
        return [];
    }
  });
}

// Map server actions (snake_case) → the client ChatAction union. Drop any
// malformed descriptor (missing key/target) rather than render a dead button.
function mapActions(reply: AssistantReply): ChatAction[] {
  return (reply.actions ?? []).flatMap((a): ChatAction[] => {
    if (a.kind === "page-action") {
      if (!a.action_key) return [];
      return [
        {
          kind: "page-action",
          page: a.page,
          actionKey: a.action_key,
          label: a.label,
          enabled: a.enabled ?? undefined,
          destructive: a.destructive ?? undefined,
        },
      ];
    }
    if (a.kind === "graph-action") {
      if (!a.graph_id || !a.graph_action) return [];
      return [
        {
          kind: "graph-action",
          page: a.page,
          graphId: a.graph_id,
          graphAction: a.graph_action,
          label: a.label,
          destructive: a.destructive ?? undefined,
        },
      ];
    }
    return [];
  });
}

// Apply an import proposal to the Import form via the bridge setters (spec §9.5),
// mirroring the mock's importDemo. Only fields the agent produced are set, so a
// follow-up turn updating just the graph queries won't clobber the SQL. Returns
// the human labels of the fields it filled (for the message's "applied" chips).
function applyProposal(proposal: AssistantProposal, bridge: PageBridge | null): string[] {
  if (!bridge || bridge.page !== "import" || !bridge.setters) return [];
  const s = bridge.setters;
  const applied: string[] = [];
  if (proposal.catalog != null) {
    s.catalog?.(proposal.catalog);
    applied.push("Catalog");
  }
  if (proposal.database != null) {
    const db = proposal.database;
    // Ensure the option exists so the <select> shows it as selected (the
    // dropdown only renders databases fetched from the API).
    s.databases?.((prev: string[]) => (prev.includes(db) ? prev : [...prev, db]));
    s.database?.(db);
    applied.push("Database");
  }
  if (proposal.bucket != null) {
    s.bucket?.(proposal.bucket);
    applied.push("S3 Staging Bucket");
  }
  if (proposal.graph_name != null) {
    s.graphName?.(proposal.graph_name);
    applied.push("Graph Name");
  }
  if (proposal.node_queries) {
    s.nodeQueries?.(proposal.node_queries.map((q) => ({ sql: q.sql })));
    applied.push("Node Queries");
  }
  if (proposal.edge_queries) {
    s.edgeQueries?.(proposal.edge_queries.map((q) => ({ sql: q.sql })));
    applied.push("Edge Queries");
  }
  if (proposal.graph_queries) {
    s.graphQueries?.(proposal.graph_queries.map((q) => ({ cypher: q.cypher })));
    applied.push("Graph Queries");
  }

  // Persist the applied fields as a projection so the assistant's work survives
  // (and so Validate/Execute has a real projection id). Pass the proposal values
  // directly — the setters above are async, so page state is still stale here.
  // Fire-and-forget: creation happens in the background like the page's own
  // auto-save, and errors surface through the page's normal error handling.
  if (applied.length) {
    void bridge.persistImport?.({
      catalog: proposal.catalog ?? undefined,
      database: proposal.database ?? undefined,
      bucket: proposal.bucket ?? undefined,
      graphName: proposal.graph_name ?? undefined,
      nodeQueries: proposal.node_queries?.map((q) => ({ sql: q.sql })),
      edgeQueries: proposal.edge_queries?.map((q) => ({ sql: q.sql })),
    });
  }
  return applied;
}

// Turn a server AssistantReply into a client ChatMessage: apply any import
// proposal to the active page, map jumps/actions, and fold a clarifying
// question (§9.8) into the reply text.
export function mapReply(reply: AssistantReply, bridge: PageBridge | null): ChatMessage {
  const applied = reply.proposal ? applyProposal(reply.proposal, bridge) : [];
  const jumps = mapJumps(reply);
  const actions = mapActions(reply);
  const text = reply.question ? `${reply.text}\n\n${reply.question}` : reply.text;
  return {
    role: "assistant",
    text,
    ...(applied.length && { applied }),
    ...(jumps.length && { jumps }),
    ...(actions.length && { actions }),
  };
}

// Run one assistant turn against the backend and map the reply. Throws on
// network / server error so the caller can fall back to the offline mock.
export async function remoteRespond(
  text: string,
  bridge: PageBridge | null,
  sessionId: string | null,
  model: string,
): Promise<ChatMessage> {
  const reply = await assistantApi.message({
    text,
    session_id: sessionId,
    page_context: serializePageContext(bridge) ?? null,
    model,
  });
  return mapReply(reply, bridge);
}
