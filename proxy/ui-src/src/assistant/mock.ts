import type { PageBridge, ChatMessage, JumpAction, ChatAction } from "./context";
import { projectApi } from "../api";

// Canned, deterministic assistant behaviour for the prototype (spec §8.2
// decision 3: replies stay canned in this phase). All page mutations go through
// the active page's PageBridge setters — the assistant never touches page state
// directly.
export async function cannedRespond(text: string, bridge: PageBridge | null): Promise<ChatMessage> {
  const base: ChatMessage =
    bridge?.page === "import" && bridge.setters
      ? importDemo(bridge)
      : {
          role: "assistant",
          text: `Got it. I'll help with that on ${
            bridge?.page ? `the ${bridge.page} page` : "this page"
          }.`,
        };

  const jumps = await detectJumps(text, bridge);
  const actions = detectActions(text, bridge);
  return { ...base, ...(jumps.length && { jumps }), ...(actions.length && { actions }) };
}

// Canned intent matching → inline page/graph action buttons (spec §8.2
// decisions 5/6, Group 5). Page-level actions come from bridge.actions; per-graph
// Stop/Delete come from bridge.graphTargets (>1 graph ⇒ one button per graph,
// i.e. the in-chat picker). Only actions the page currently offers are shown.
function detectActions(text: string, bridge: PageBridge | null): ChatAction[] {
  if (!bridge) return [];
  const t = text.toLowerCase();
  const actions: ChatAction[] = [];

  // Page-level singleton actions (Import: execute / validateQuery / preview).
  const pageActionIntent: Record<string, RegExp> = {
    execute: /\b(execute|run|import|ingest)\b/,
    validateQuery: /\b(validate|verify|check|lint)\b/,
    preview: /\b(preview|schema|sample|inspect)\b/,
  };
  if (bridge.actions) {
    for (const [key, spec] of Object.entries(bridge.actions)) {
      const re = pageActionIntent[key];
      if (re && re.test(t)) {
        actions.push({
          kind: "page-action",
          page: bridge.page,
          actionKey: key,
          label: spec.label,
          enabled: spec.enabled,
          destructive: spec.destructive,
        });
      }
    }
  }

  // Per-graph Stop / Delete (Graphs, Projections). One button per eligible graph.
  if (bridge.graphTargets && bridge.runGraphAction) {
    const wanted: string[] = [];
    if (/\bstop\b/.test(t)) wanted.push("stop");
    if (/\b(delete|remove|destroy|drop)\b/.test(t)) wanted.push("delete");
    for (const ga of wanted) {
      for (const g of bridge.graphTargets) {
        if (!g.actions.includes(ga)) continue;
        actions.push({
          kind: "graph-action",
          page: bridge.page,
          graphId: g.id,
          graphAction: ga,
          label: `${ga === "stop" ? "Stop" : "Delete"} ${g.name}`,
          destructive: true,
        });
      }
    }
  }

  return actions;
}

// Canned intent matching → inline jump buttons (spec §8.2 decision 7, Group 4).
// New Import / New Project are offered on create intent. A "show <name> project"
// (or "show my graphs") intent resolves to a single suggested project — matched
// by name against projectApi.list(), falling back to the first project.
async function detectJumps(text: string, bridge: PageBridge | null): Promise<JumpAction[]> {
  const t = text.toLowerCase();
  const projectId = bridge?.jumpContext?.projectId ?? null;
  const jumps: JumpAction[] = [];

  if (/\b(new|create|add|another|start)\b[\s\S]*\bproject\b/.test(t)) {
    jumps.push({ kind: "new-project", label: "New Project" });
  }
  if (/\b(new|create|add|another|start|run|do)\b[\s\S]*\b(import|graph|projection)\b/.test(t)) {
    jumps.push({ kind: "new-import", label: "New Import", projectId });
  }

  // "show/view/open ... project|graph(s)" → suggest a single project to open.
  const viewIntent =
    /\b(show|view|see|open|list|display|go\s*to)\b/.test(t) && /\b(project|graphs?)\b/.test(t);
  if (viewIntent) {
    const projects = await projectApi.list().catch(() => []);
    if (projects.length) {
      const match = projects.find((p) => p.name && t.includes(p.name.toLowerCase())) ?? projects[0];
      jumps.push({ kind: "open-projections", label: match.name, projectId: match.id });
    }
  }

  return jumps;
}

// Worked example (MITRE ATT&CK → PageRank), applied to the Import form via the
// bridge setters. Uses functional updaters so we never read stale field values.
function importDemo(bridge: PageBridge): ChatMessage {
  const s = bridge.setters!;
  s.catalog?.("AwsDataCatalog");
  // Ensure the option exists so the <select> shows it as selected (the dropdown
  // only renders databases fetched from the API).
  s.databases?.((prev: string[]) =>
    prev.includes("mitre_attack") ? prev : [...prev, "mitre_attack"],
  );
  s.database?.("mitre_attack");
  s.bucket?.((prev: string) => prev || "s3://my-neptune-staging/");
  s.graphName?.((prev: string) => prev || "malware-threat-graph");
  const nodeQueries = [
    { sql: `SELECT id AS "~id", 'Malware' AS "~label", name, attack_id, platforms FROM malware` },
    { sql: `SELECT id AS "~id", 'Campaign' AS "~label", name, attack_id, first_seen FROM campaigns` },
    { sql: `SELECT id AS "~id", 'Mitigation' AS "~label", name, attack_id, description FROM mitigations` },
    { sql: `SELECT id AS "~id", 'Tool' AS "~label", name, attack_id, platforms FROM tools` },
  ];
  s.nodeQueries?.(nodeQueries);
  const edgeQueries = [
    { sql: `SELECT id AS "~id", source_ref AS "~from", target_ref AS "~to", 'uses' AS "~label"\nFROM relationships WHERE relationship_type = 'uses'` },
    { sql: `SELECT id AS "~id", source_ref AS "~from", target_ref AS "~to", 'mitigates' AS "~label"\nFROM relationships WHERE relationship_type = 'mitigates'` },
    { sql: `SELECT id AS "~id", source_ref AS "~from", target_ref AS "~to", 'attributed-to' AS "~label"\nFROM relationships WHERE relationship_type = 'attributed-to'` },
  ];
  s.edgeQueries?.(edgeQueries);
  const graphQueries = [
    { cypher: `CALL neptune.algo.pageRank.mutate({\n  writeProperty: "pagerank"\n})\nYIELD success\nRETURN success` },
    { cypher: `MATCH (n)\nWHERE 'Malware' IN labels(n)\nRETURN n.name AS malware, n.pagerank AS pagerank\nORDER BY pagerank DESC\nLIMIT 10` },
  ];
  s.graphQueries?.(graphQueries);
  // Mirror the remote path: persist the demo as a projection so it behaves like
  // a real assistant fill (creating the projection and saving its queries).
  void bridge.persistImport?.({
    catalog: "AwsDataCatalog",
    database: "mitre_attack",
    bucket: "s3://my-neptune-staging/",
    graphName: "malware-threat-graph",
    nodeQueries,
    edgeQueries,
  });
  return {
    role: "assistant",
    text:
      "I inspected your catalog with SHOW CREATE TABLE. I mapped `malware`, `campaigns`, " +
      "`mitigations`, and `tools` as node tables, and split `relationships(source_ref, " +
      "target_ref, relationship_type)` into edge queries per relationship type (uses / " +
      "mitigates / attributed-to). I've left out `groups` and `techniques` for now. After " +
      "import, the first openCypher query runs pageRank.mutate to write a `pagerank` " +
      "property onto every node, and the second reads that property to return the top 10 " +
      "malware by PageRank. Want me to add `groups` and `techniques` as nodes too?",
    applied: ["Catalog", "Database", "S3 Staging Bucket", "Graph Name", "Node Queries", "Edge Queries", "Graph Queries"],
  };
}
