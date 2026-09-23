import { useEffect, useState, useCallback, useRef } from "react";
import { useSearchParams, useNavigate, useLocation } from "react-router";
import {
  metadata,
  projection,
  projectApi,
  graphActions,
  type Projection,
  type Inflight,
} from "../api";
import { Button, Card, RefreshButton } from "../components/ui";
import { usePageBridge } from "../assistant/context";
import {
  Play,
  Plus,
  Trash2,
  Square,
  ExternalLink,
  AlertTriangle,
  ChevronDown,
  ChevronRight,
} from "lucide-react";

// Read-only view of a completed projection. Config is immutable (the graph
// cannot be re-modeled after import); only the openCypher Graph Queries are
// interactive, plus the graph lifecycle controls. Import owns everything that
// is still in flight — this page only ever renders a `complete` projection (a
// reverse guard bounces anything else back to /import).

type GraphQuery = { id: string; cypher: string };
// Per-query run state: the last result (or error) and whether its collapsible
// results panel is expanded.
type RunState = { running: boolean; result?: unknown; error?: string; open: boolean };

const newId = () =>
  typeof crypto !== "undefined" && crypto.randomUUID ? crypto.randomUUID() : `q${Math.random()}`;

export function Details() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();
  const location = useLocation();
  const projectionId = searchParams.get("projection");

  const [current, setCurrent] = useState<Projection | null>(null);
  const [projectName, setProjectName] = useState<string | null>(null);
  const [region, setRegion] = useState("");
  const [graphStatus, setGraphStatus] = useState<string>("");
  const [actions, setActions] = useState<string[]>([]);
  const [inflight, setInflight] = useState<Inflight | null>(null);
  const [error, setError] = useState<string | null>(null);

  // --- Post-import openCypher graph queries (local state, not persisted) ---
  // Seed from navigation state so queries typed on the Import page carry over
  // when the user clicks "View Details" on completion. Blank rows are dropped;
  // fall back to a single empty row (e.g. the guard redirect, or a direct link).
  const [graphQueries, setGraphQueries] = useState<GraphQuery[]>(() => {
    const incoming = (location.state as { graphQueries?: { cypher: string }[] } | null)?.graphQueries;
    const filtered = incoming?.filter((q) => q.cypher.trim());
    const seed = filtered && filtered.length ? filtered : [{ cypher: "" }];
    return seed.map((q) => ({ id: newId(), cypher: q.cypher }));
  });
  const [runStates, setRunStates] = useState<Record<string, RunState>>({});

  const loadGraphState = useCallback((graphId: string) => {
    graphActions
      .getActions(graphId)
      .then((r) => {
        setGraphStatus(r.status);
        setActions(r.actions);
        setInflight(r.inflight);
      })
      .catch(() => {});
  }, []);

  const loadProjection = useCallback(
    (p: Projection) => {
      // Reverse guard: /details only renders completed projections. Anything
      // still in flight (or failed/archived) belongs on the Import page.
      if (p.status !== "complete") {
        navigate(`/import?projection=${p.id}`, { replace: true });
        return;
      }
      setCurrent(p);
      if (p.project_id) {
        projectApi
          .list()
          .then((list) => setProjectName(list.find((w) => w.id === p.project_id)?.name ?? null))
          .catch(() => {});
      }
      if (p.graph_id) loadGraphState(p.graph_id);

      // Load persisted graph queries. If any are stored, they win over the
      // navigation-state seed. If none are stored yet but queries carried over
      // from the Import page, persist them now so they survive a reload.
      projection
        .getQueries(p.id)
        .then((res) => {
          if (res.graph_queries.length > 0) {
            setGraphQueries(res.graph_queries.map((q) => ({ id: newId(), cypher: q.cypher })));
          } else {
            const carried = (
              (location.state as { graphQueries?: { cypher: string }[] } | null)?.graphQueries ?? []
            ).filter((q) => q.cypher.trim());
            if (carried.length) {
              projection
                .saveGraphQueries(p.id, carried.map((q) => ({ cypher: q.cypher })))
                .catch(() => {});
            }
          }
        })
        .catch(() => {});
    },
    [navigate, loadGraphState, location.state],
  );

  useEffect(() => {
    metadata.config().then((c) => setRegion(c.region)).catch(() => {});
    if (projectionId) projection.get(projectionId).then(loadProjection).catch((e) => setError(e.message));
  }, [projectionId]);

  // --- Graph query editing ---
  // Graph queries persist their openCypher text only (never results) via the
  // graph-only endpoint, so editing here doesn't touch the immutable node/edge
  // queries that defined the import.
  const persistTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const persistGraphQueries = useCallback(
    (list: GraphQuery[], debounce = false) => {
      if (!current) return;
      const save = () =>
        projection
          .saveGraphQueries(current.id, list.map((q) => ({ cypher: q.cypher })))
          .catch(() => {});
      if (persistTimer.current) clearTimeout(persistTimer.current);
      if (debounce) persistTimer.current = setTimeout(save, 1000);
      else save();
    },
    [current],
  );

  function updateGraphQuery(id: string, cypher: string) {
    setGraphQueries((prev) => {
      const updated = prev.map((q) => (q.id === id ? { ...q, cypher } : q));
      persistGraphQueries(updated, true);
      return updated;
    });
  }
  function addGraphQuery() {
    setGraphQueries((prev) => [...prev, { id: newId(), cypher: "" }]);
  }
  function removeGraphQuery(id: string) {
    setGraphQueries((prev) => {
      const updated = prev.filter((q) => q.id !== id);
      persistGraphQueries(updated);
      return updated;
    });
    setRunStates((prev) => {
      const { [id]: _drop, ...rest } = prev;
      return rest;
    });
  }
  function toggleResult(id: string) {
    setRunStates((prev) => ({ ...prev, [id]: { ...prev[id], open: !prev[id]?.open } }));
  }

  // Execute a single openCypher statement against the live graph. Shared by the
  // per-query Run button and the assistant's inline run-query action.
  const executeQuery = useCallback(
    async (cypher: string): Promise<{ result?: unknown; error?: string }> => {
      if (!current) return { error: "No projection loaded." };
      const res = await projection.runQuery(current.id, [cypher]);
      return { result: res.results, error: res.error };
    },
    [current],
  );

  async function runOne(id: string) {
    const q = graphQueries.find((g) => g.id === id);
    if (!q || !q.cypher.trim()) return;
    setRunStates((prev) => ({ ...prev, [id]: { ...prev[id], running: true, open: true } }));
    try {
      const { result, error } = await executeQuery(q.cypher);
      setRunStates((prev) => ({ ...prev, [id]: { running: false, result, error, open: true } }));
    } catch (e: any) {
      setRunStates((prev) => ({ ...prev, [id]: { running: false, error: e.message, open: true } }));
    }
  }

  // Run a query on behalf of the assistant (inline "Run Query" chat button).
  // Reflects the result on the matching row when the query is one already listed,
  // and always returns it so the assistant can show it in the transcript.
  const runGraphQuery = useCallback(
    async (cypher: string): Promise<{ result?: unknown; error?: string }> => {
      const outcome = await executeQuery(cypher);
      setGraphQueries((prev) => {
        const match = prev.find((g) => g.cypher.trim() === cypher.trim());
        if (match) {
          setRunStates((rs) => ({
            ...rs,
            [match.id]: { running: false, result: outcome.result, error: outcome.error, open: true },
          }));
        }
        return prev;
      });
      return outcome;
    },
    [executeQuery],
  );

  async function performGraphAction(action: string) {
    if (!current?.graph_id) return;
    const name = current.graph_name || current.graph_id;
    if (action === "stop" && !confirm(`Stop graph ${name}? It will become unavailable until restarted.`)) return;
    if (action === "delete" && !confirm(`Delete graph ${name}? This cannot be undone.`)) return;
    try {
      if (action === "delete") {
        // Delete archives the projection — the details page is no longer valid,
        // so return to the projections list.
        await projection.deleteGraph(current.id);
        window.dispatchEvent(new Event("projects-changed"));
        navigate("/projections");
        return;
      }
      await graphActions.perform(current.graph_id, action);
      loadGraphState(current.graph_id);
    } catch (e: any) {
      setError(e.message || `Failed to ${action}`);
    }
  }

  function openGraphExplorer() {
    if (!current?.graph_id) return;
    const graphDbUrl = `https://${current.graph_id}.${region}.neptune-graph.amazonaws.com`;
    const params = new URLSearchParams({
      graphDbUrl,
      queryEngine: "openCypher",
      awsRegion: region,
      serviceType: "neptune-graph",
      name: current.graph_name || current.graph_id || "",
    } as Record<string, string>);
    const geBase = (import.meta as any).env?.VITE_GRAPH_EXPLORER_URL || "https://localhost/explorer";
    window.open(`${geBase}/#/connect?${params}`, "_blank");
  }

  // Full assistant bridge: expose the completed graph as an action target, let
  // the assistant propose openCypher via the graphQueries setter, and run a
  // specific query via runGraphQuery (its presence tells the backend this page
  // can run queries, so it may offer inline "Run Query" buttons). There are no
  // config setters — the page is immutable.
  usePageBridge({
    page: "details",
    fields: {
      graphQueries: graphQueries.map((q) => ({ cypher: q.cypher })),
      projectionId: current?.id ?? null,
      graphStatus: graphStatus || null,
      graphId: current?.graph_id ?? null,
    },
    setters: {
      // The assistant proposes bare {cypher}; normalize to keyed rows and
      // persist (text only), so applied queries survive a reload.
      graphQueries: (qs: { cypher: string }[]) => {
        const list = qs.map((q) => ({ id: newId(), cypher: q.cypher }));
        setGraphQueries(list);
        setRunStates({});
        persistGraphQueries(list);
      },
    },
    graphTargets:
      current?.graph_id
        ? [{ id: current.graph_id, name: current.graph_name || current.graph_id, status: graphStatus, actions }]
        : [],
    runGraphAction: (_id, action) => performGraphAction(action),
    runGraphQuery,
    jumpContext: { projectId: current?.project_id ?? null },
  });

  const graphStatusStyle = (status: string) => {
    switch (status) {
      case "AVAILABLE": return "bg-green-100 text-green-700";
      case "STOPPED": return "bg-yellow-100 text-yellow-700";
      case "STOPPING": case "DELETING": return "bg-red-100 text-red-700";
      case "CREATING": case "STARTING": return "bg-blue-100 text-blue-700";
      default: return "bg-gray-100 text-gray-700";
    }
  };

  if (!current) {
    return (
      <div className="mx-auto max-w-3xl space-y-6">
        <h1 className="text-lg font-semibold">Details</h1>
        {error ? (
          <div className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-700">{error}</div>
        ) : (
          <p className="text-sm text-gray-500">Loading…</p>
        )}
      </div>
    );
  }

  const isTransient = ["STOPPING", "STARTING", "DELETING", "CREATING"].includes(graphStatus);

  const field = (label: string, value?: string | null) => (
    <div>
      <span className="text-gray-500">{label}:</span> {value || "—"}
    </div>
  );

  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <div className="flex items-center justify-between pr-32">
        <h1 className="text-lg font-semibold">{current.graph_name || current.id.slice(0, 8)} — Details</h1>
        <RefreshButton onClick={() => current.graph_id && loadGraphState(current.graph_id)} />
      </div>

      {/* Immutable configuration */}
      <Card>
        <div className="space-y-2 text-sm">
          {field("Project", projectName)}
          {field("Catalog", current.catalog)}
          {field("Database", current.database)}
          {field("S3 Staging Bucket", current.s3_staging_bucket)}
          {field("Graph Name", current.graph_name)}
          <div>
            <span className="text-gray-500">Memory:</span> {current.graph_memory_gb} GB
          </div>
          {field("Graph ID", current.graph_id)}
          {graphStatus && (
            <div>
              <span className="text-gray-500">Graph Status:</span>{" "}
              <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${graphStatusStyle(graphStatus)}`}>
                {graphStatus.toLowerCase()}
              </span>
            </div>
          )}
          {current.graph_endpoint && field("Endpoint", current.graph_endpoint)}
        </div>
      </Card>

      {/* Graph lifecycle controls */}
      {current.graph_id && (
        <Card>
          <div className="flex flex-wrap gap-2">
            <Button variant="ghost" onClick={openGraphExplorer}>
              <ExternalLink className="h-3 w-3" /> Open in Graph Explorer
            </Button>
            <Button
              variant="secondary"
              disabled={!actions.includes("stop") || isTransient}
              onClick={() => performGraphAction("stop")}
            >
              <Square className="h-3 w-3" /> Stop
            </Button>
            <Button
              variant="secondary"
              disabled={!actions.includes("start") || isTransient}
              onClick={() => performGraphAction("start")}
            >
              <Play className="h-3 w-3" /> Start
            </Button>
            <Button
              variant="ghost"
              className="text-red-600 hover:text-red-700"
              disabled={isTransient}
              onClick={() => performGraphAction("delete")}
            >
              <Trash2 className="h-3 w-3" /> Delete
            </Button>
          </div>
          {inflight?.error && (
            <div className="mt-3 flex items-start gap-2 rounded border border-red-200 bg-red-50 p-2 text-xs text-red-700">
              <AlertTriangle className="mt-0.5 h-3 w-3 flex-shrink-0" />
              <span>{inflight.error}</span>
            </div>
          )}
        </Card>
      )}

      {/* Graph Queries (openCypher) — each runs independently */}
      <Card>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold">Graph Queries (openCypher)</h2>
              <p className="text-xs text-gray-500">Run each query against the graph independently.</p>
            </div>
            <button onClick={addGraphQuery} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800">
              <Plus className="h-3 w-3" /> Add
            </button>
          </div>
          <div className="max-h-[32rem] space-y-3 overflow-y-auto pr-1">
            {graphQueries.map((gq, i) => {
              const rs = runStates[gq.id];
              const hasResult = rs && (rs.result !== undefined || rs.error);
              return (
                <div key={gq.id} className="rounded-md border border-gray-200 overflow-hidden">
                  <div className="flex items-center justify-between bg-gray-50 px-3 py-1.5 border-b border-gray-200">
                    <span className="text-xs font-medium text-gray-700">Query {i + 1}</span>
                    <div className="flex items-center gap-3">
                      <button
                        onClick={() => runOne(gq.id)}
                        disabled={rs?.running || !gq.cypher.trim()}
                        className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 disabled:opacity-40 disabled:hover:text-blue-600"
                      >
                        <Play className="h-3 w-3" /> {rs?.running ? "Running..." : "Run Query"}
                      </button>
                      {graphQueries.length > 1 && (
                        <button onClick={() => removeGraphQuery(gq.id)} className="text-gray-400 hover:text-red-600">
                          <Trash2 className="h-3 w-3" />
                        </button>
                      )}
                    </div>
                  </div>
                  <textarea
                    className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-y"
                    rows={3}
                    placeholder="MATCH (n) RETURN n LIMIT 10"
                    value={gq.cypher}
                    onChange={(e) => updateGraphQuery(gq.id, e.target.value)}
                  />
                  {hasResult && (
                    <div className="border-t border-gray-200">
                      <button
                        onClick={() => toggleResult(gq.id)}
                        className="flex w-full items-center gap-1 bg-gray-50 px-3 py-1.5 text-xs font-medium text-gray-600 hover:bg-gray-100"
                      >
                        {rs!.open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                        {rs!.error ? "Error" : "Results"}
                      </button>
                      {rs!.open &&
                        (rs!.error ? (
                          <div className="bg-red-50 px-3 py-2 text-xs text-red-700">{rs!.error}</div>
                        ) : (
                          <pre className="max-h-72 overflow-auto bg-gray-900 px-3 py-2 text-xs text-gray-100">
                            {JSON.stringify(rs!.result, null, 2)}
                          </pre>
                        ))}
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </Card>

      {error && (
        <div className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-700">{error}</div>
      )}
    </div>
  );
}
