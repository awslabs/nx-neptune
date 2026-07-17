import { useEffect, useState } from "react";
import { useSearchParams, NavLink } from "react-router";
import { projection, metadata, projectApi, graphActions, type Projection, type Project, type Inflight, type TimingRecord } from "../api";
import { Card, Button, RefreshButton } from "../components/ui";
import { X, ExternalLink, Trash2, Square, Play, AlertTriangle, ChevronRight, ChevronDown, Clock } from "lucide-react";
import { useNavigate } from "react-router";

const PHASE_LABELS: Record<string, string> = {
  graph_creation: "Create graph",
  graph_reset: "Reset graph",
  athena_export: "Execute Athena statements",
  graph_import: "Import graph data",
  post_import_query: "Post-import query",
};

function formatDuration(seconds: number): string {
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)} s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  return `${m}m ${s}s`;
}

function TimingsList({ timings }: { timings: TimingRecord[] }) {
  const total = timings.reduce((sum, t) => sum + t.seconds, 0);
  return (
    <>
      <ul className="space-y-1">
        {timings.map((t, i) => (
          <li key={i} className="flex items-baseline justify-between gap-3 text-xs">
            <span className="text-gray-600">
              {i + 1}. {PHASE_LABELS[t.phase] || t.phase}
            </span>
            <span className="whitespace-nowrap font-mono text-gray-800">{formatDuration(t.seconds)}</span>
          </li>
        ))}
      </ul>
      <div className="mt-2 flex items-baseline justify-between gap-3 border-t border-gray-100 pt-1 text-xs font-medium">
        <span className="text-gray-700">Total</span>
        <span className="whitespace-nowrap font-mono text-gray-900">{formatDuration(total)}</span>
      </div>
    </>
  );
}

function TimingsPopover({ timings, anchor }: { timings: TimingRecord[]; anchor: DOMRect }) {
  // Rendered with position: fixed so an ancestor's overflow-hidden (the table
  // Card) can't clip it. Anchored just below the status badge.
  return (
    <div
      className="pointer-events-none fixed z-50 w-72 rounded-md border border-gray-200 bg-white p-3 text-left shadow-lg"
      style={{ top: anchor.bottom + 4, left: anchor.left }}
    >
      <p className="mb-2 flex items-center gap-1 text-xs font-semibold text-gray-700">
        <Clock className="h-3 w-3" /> Timing breakdown
      </p>
      <TimingsList timings={timings} />
    </div>
  );
}

export function Sessions() {
  const [searchParams] = useSearchParams();
  const [sessions, setSessions] = useState<Projection[]>([]);
  const [selected, setSelected] = useState<Projection | null>(null);
  const [region, setRegion] = useState("");
  const [projects, setProjects] = useState<Map<string, Project>>(new Map());
  const [summaries, setSummaries] = useState<Map<string, { numNodes: number; numEdges: number }>>(new Map());
  const [graphStatuses, setGraphStatuses] = useState<Map<string, string>>(new Map());
  const [actionStates, setActionStates] = useState<Record<string, { actions: string[]; inflight: Inflight | null }>>({});
  const [alerts, setAlerts] = useState<{ graphId: string; graphName: string; message: string }[]>([]);
  const [archivedOpen, setArchivedOpen] = useState(false);
  const [timingHover, setTimingHover] = useState<{ id: string; anchor: DOMRect } | null>(null);
  const navigate = useNavigate();
  const filterProjectId = searchParams.get("project");

  useEffect(() => {
    load();
    metadata.config().then(c => setRegion(c.region));
    projectApi.list().then(list => setProjects(new Map(list.map(p => [p.id, p]))));
  }, []);

  async function load() {
    const list = await projection.list();
    setSessions(list);
    // Fetch graph statuses and actions
    metadata.graphs().then(({ graphs }) => {
      setGraphStatuses(new Map(graphs.map(g => [g.id, g.status])));
    }).catch(() => {});
    // Fetch actions for sessions with graphs
    for (const s of list) {
      if (s.graph_id) {
        graphActions.getActions(s.graph_id).then(r => {
          setActionStates(prev => ({ ...prev, [s.graph_id!]: { actions: r.actions, inflight: r.inflight } }));
          if (r.inflight?.error) {
            setAlerts(prev => {
              if (prev.some(a => a.graphId === s.graph_id)) return prev;
              return [...prev, { graphId: s.graph_id!, graphName: s.graph_name || s.graph_id!, message: r.inflight!.error! }];
            });
          }
        }).catch(() => {});
      }
    }
    // Fetch graph summaries for completed projections
    const entries: [string, { numNodes: number; numEdges: number }][] = [];
    await Promise.all(
      list.filter(p => p.graph_id && (p.status === "complete" || p.status === "importing")).map(async (p) => {
        try {
          const s = await metadata.graphSummary(p.graph_id!);
          entries.push([p.id, { numNodes: s.numNodes, numEdges: s.numEdges }]);
        } catch {}
      })
    );
    setSummaries(new Map(entries));
  }

  const allFiltered = filterProjectId
    ? sessions.filter(s => s.project_id === filterProjectId)
    : sessions;
  const active = allFiltered.filter(s => s.status !== "archived");
  const archived = allFiltered.filter(s => s.status === "archived");
  const projectName = filterProjectId ? projects.get(filterProjectId)?.name : null;

  // Fetch graph actions when selecting a session that has a graph
  useEffect(() => {
    if (selected?.graph_id) {
      graphActions.getActions(selected.graph_id).then(r => {
        setActionStates(prev => ({ ...prev, [selected.graph_id!]: { actions: r.actions, inflight: r.inflight } }));
      }).catch(() => {});
    }
  }, [selected?.graph_id, graphStatuses]);

  async function performGraphAction(graphId: string, action: string, graphName: string) {
    if (action === "stop" && !confirm(`Stop graph ${graphName}? It will become unavailable until restarted.`)) return;
    try {
      await graphActions.perform(graphId, action);
      load();
    } catch (e: any) {
      setAlerts(prev => [...prev, { graphId, graphName, message: e.message || `Failed to ${action}` }]);
    }
  }

  async function archiveSession(sessionId: string, name: string) {
    if (!confirm(`Delete graph for "${name}"? The session config will be preserved.`)) return;
    try {
      await projection.deleteGraph(sessionId);
      load();
      window.dispatchEvent(new Event("projects-changed"));
    } catch (e: any) {
      setAlerts(prev => [...prev, { graphId: sessionId, graphName: name, message: e.message || "Failed to delete graph" }]);
    }
  }

  async function purgeSession(sessionId: string, name: string) {
    if (!confirm(`Permanently delete projection job "${name}"? This cannot be undone.`)) return;
    try {
      await projection.delete(sessionId);
      if (selected?.id === sessionId) setSelected(null);
      load();
      window.dispatchEvent(new Event("projects-changed"));
    } catch (e: any) {
      setAlerts(prev => [...prev, { graphId: sessionId, graphName: name, message: e.message || "Failed to delete" }]);
    }
  }

  function dismissAlert(graphId: string) {
    setAlerts(prev => prev.filter(a => a.graphId !== graphId));
    graphActions.dismissInflight(graphId).catch(() => {});
  }

  // Poll every 30s when any graph is in a transient state
  const hasTransient = [...graphStatuses.values()].some(s =>
    ["STOPPING", "STARTING", "DELETING", "CREATING"].includes(s)
  );

  useEffect(() => {
    if (!hasTransient) return;
    const interval = setInterval(() => load(), 30000);
    return () => clearInterval(interval);
  }, [hasTransient]);

  const statusStyle = (status: string) => {
    switch (status) {
      case "complete": return "bg-green-100 text-green-700";
      case "failed": return "bg-red-100 text-red-700";
      case "executing": case "deleting": return "bg-blue-100 text-blue-700";
      case "archived": return "bg-gray-100 text-gray-600";
      default: return "bg-gray-100 text-gray-700";
    }
  };

  const graphStatusStyle = (status: string) => {
    switch (status) {
      case "AVAILABLE": return "bg-green-100 text-green-700";
      case "STOPPED": return "bg-yellow-100 text-yellow-700";
      case "STOPPING": case "DELETING": return "bg-red-100 text-red-700";
      case "CREATING": case "STARTING": return "bg-blue-100 text-blue-700";
      default: return "bg-gray-100 text-gray-700";
    }
  };

  return (
    <div className="flex gap-4">
      <div className="flex-1 space-y-4">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold">{projectName ? `${projectName} — Sessions` : "Sessions"}</h1>
          <div className="flex items-center gap-2">
            <NavLink
              to={filterProjectId ? `/import?project=${filterProjectId}&t=${Date.now()}` : "/import"}
              className="inline-flex items-center gap-1 rounded-md border border-gray-300 bg-white px-3 py-1.5 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50"
            >+ New</NavLink>
            <RefreshButton onClick={load} />
          </div>
        </div>

      {/* Error Alerts */}
      {alerts.map((alert) => (
        <div key={alert.graphId} className="flex items-start gap-3 rounded-lg border border-red-200 bg-red-50 p-3">
          <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0 text-red-600" />
          <div className="flex-1 text-sm">
            <p className="font-medium text-red-800">Action failed on {alert.graphName}</p>
            <p className="text-red-700">{alert.message}</p>
          </div>
          <button onClick={() => dismissAlert(alert.graphId)} className="text-red-400 hover:text-red-600">
            <X className="h-4 w-4" />
          </button>
        </div>
      ))}

        {/* Active Sessions */}
        <Card className="overflow-hidden p-0">
          <table className="w-full text-left text-sm">
            <thead className="border-b bg-gray-50">
              <tr>
                <th className="px-4 py-3 font-medium">Name</th>
                <th className="px-4 py-3 font-medium">Project</th>
                <th className="px-4 py-3 font-medium">Import Status</th>
                <th className="px-4 py-3 font-medium">Progress</th>
                <th className="px-4 py-3 font-medium">Created</th>
                <th className="px-4 py-3 font-medium">Graph Status</th>
                <th className="px-4 py-3 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {active.length === 0 ? (
                <tr><td colSpan={7} className="px-4 py-6 text-center text-sm text-gray-500">No active sessions</td></tr>
              ) : active.map((s) => {
                const graphStatus = s.graph_id ? graphStatuses.get(s.graph_id) : undefined;
                const state = s.graph_id ? actionStates[s.graph_id] : undefined;
                const actions = state?.actions || [];
                const isTransient = graphStatus ? ["STOPPING", "STARTING", "DELETING", "CREATING"].includes(graphStatus) : false;

                return (
                <tr
                  key={s.id}
                  className={`cursor-pointer border-b last:border-0 hover:bg-gray-50 ${selected?.id === s.id ? "bg-blue-50" : ""}`}
                  onClick={() => setSelected(s)}
                  onDoubleClick={() => navigate(`/import?session=${s.id}`)}
                >
                  <td className="px-4 py-3 font-medium">{s.graph_name || s.id.slice(0, 8)}</td>
                  <td className="px-4 py-3 text-gray-600">{s.project_id ? projects.get(s.project_id)?.name || "—" : "—"}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusStyle(s.status)}`}>{s.status}</span>
                  </td>
                  <td className="px-4 py-3">{Math.round(s.progress)}%</td>
                  <td className="px-4 py-3 text-gray-500">{new Date(s.created_at).toLocaleString()}</td>
                  <td className="px-4 py-3">
                    <div className="inline-block">
                      {graphStatus ? (
                        <span
                          className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium ${graphStatusStyle(graphStatus)}`}
                          onMouseEnter={(e) => {
                            if (s.timings && s.timings.length > 0)
                              setTimingHover({ id: s.id, anchor: e.currentTarget.getBoundingClientRect() });
                          }}
                          onMouseLeave={() => setTimingHover((h) => (h?.id === s.id ? null : h))}
                        >
                          {graphStatus}
                          {s.timings && s.timings.length > 0 && <Clock className="h-3 w-3 opacity-60" />}
                        </span>
                      ) : <span className="text-gray-400">—</span>}
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex gap-1">
                      {/* Graph Explorer */}
                      <button
                        className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-blue-600 disabled:opacity-30 disabled:hover:bg-transparent"
                        disabled={!s.graph_id || graphStatus !== "AVAILABLE"}
                        title="Open in Graph Explorer"
                        onClick={(e) => {
                          e.stopPropagation();
                          const graphDbUrl = `https://${s.graph_id}.${region}.neptune-graph.amazonaws.com`;
                          const params = new URLSearchParams({
                            graphDbUrl,
                            queryEngine: "openCypher",
                            awsRegion: region,
                            serviceType: "neptune-graph",
                            name: s.graph_name || s.graph_id || "",
                          });
                          const geBase = (import.meta as any).env?.VITE_GRAPH_EXPLORER_URL || "https://localhost/explorer";
                          window.open(`${geBase}/#/connect?${params}`, "_blank");
                        }}
                      >
                        <ExternalLink className="h-4 w-4" />
                      </button>

                      {/* Stop */}
                      <button
                        className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-amber-600 disabled:opacity-30 disabled:hover:bg-transparent"
                        disabled={!actions.includes("stop") || isTransient}
                        title="Stop graph"
                        onClick={(e) => { e.stopPropagation(); performGraphAction(s.graph_id!, "stop", s.graph_name || s.graph_id!); }}
                      >
                        <Square className="h-4 w-4" />
                      </button>

                      {/* Start */}
                      <button
                        className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-green-600 disabled:opacity-30 disabled:hover:bg-transparent"
                        disabled={!actions.includes("start") || isTransient}
                        title="Start graph"
                        onClick={(e) => { e.stopPropagation(); performGraphAction(s.graph_id!, "start", s.graph_name || s.graph_id!); }}
                      >
                        <Play className="h-4 w-4" />
                      </button>

                      {/* Archive (delete graph, keep session) */}
                      <button
                        className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-red-600 disabled:opacity-30 disabled:hover:bg-transparent"
                        disabled={!s.graph_id || isTransient}
                        title="Delete graph (archive session)"
                        onClick={(e) => { e.stopPropagation(); archiveSession(s.id, s.graph_name || s.id.slice(0, 8)); }}
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </td>
                </tr>
                );
              })}
            </tbody>
          </table>
        </Card>

        {/* Archived Sessions (collapsible) */}
        <div>
          <button
            className="flex items-center gap-1 text-sm font-medium text-gray-600 hover:text-gray-900"
            onClick={() => setArchivedOpen(!archivedOpen)}
          >
            {archivedOpen ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            Archived ({archived.length})
          </button>

          {archivedOpen && (
            <Card className="mt-2 overflow-hidden p-0">
              <table className="w-full text-left text-sm">
                <thead className="border-b bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 font-medium">Name</th>
                    <th className="px-4 py-3 font-medium">Project</th>
                    <th className="px-4 py-3 font-medium">Import Status</th>
                    <th className="px-4 py-3 font-medium">Progress</th>
                    <th className="px-4 py-3 font-medium">Created</th>
                    <th className="px-4 py-3 font-medium">Graph Status</th>
                    <th className="px-4 py-3 font-medium">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {archived.length === 0 ? (
                    <tr><td colSpan={7} className="px-4 py-6 text-center text-sm text-gray-500">No archived sessions</td></tr>
                  ) : archived.map((s) => (
                    <tr
                      key={s.id}
                      className={`cursor-pointer border-b last:border-0 hover:bg-gray-50 ${selected?.id === s.id ? "bg-blue-50" : ""}`}
                      onClick={() => setSelected(s)}
                      onDoubleClick={() => navigate(`/import?session=${s.id}`)}
                    >
                      <td className="px-4 py-3 font-medium text-gray-500">{s.graph_name || s.id.slice(0, 8)}</td>
                      <td className="px-4 py-3 text-gray-500">{s.project_id ? projects.get(s.project_id)?.name || "—" : "—"}</td>
                      <td className="px-4 py-3">
                        <span className="inline-flex rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600">archived</span>
                      </td>
                      <td className="px-4 py-3 text-gray-400">—</td>
                      <td className="px-4 py-3 text-gray-500">{new Date(s.created_at).toLocaleString()}</td>
                      <td className="px-4 py-3"><span className="text-gray-400">—</span></td>
                      <td className="px-4 py-3">
                        <button
                          className="rounded-md border border-red-200 px-2 py-1 text-xs font-medium text-red-600 hover:bg-red-50 hover:border-red-300"
                          onClick={(e) => { e.stopPropagation(); purgeSession(s.id, s.graph_name || s.id.slice(0, 8)); }}
                        >
                          Delete projection job
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </Card>
          )}
        </div>
      </div>

      {/* Detail Panel */}
      {selected && (
        <Card className="w-80 shrink-0 space-y-3 self-start">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold">{selected.graph_name || selected.id.slice(0, 8)}</h2>
            <button onClick={() => setSelected(null)} className="text-gray-400 hover:text-gray-600"><X className="h-4 w-4" /></button>
          </div>
          <div className="space-y-2 text-sm">
            {selected.project_id && <div><span className="text-gray-500">Project:</span> {projects.get(selected.project_id)?.name || selected.project_id}</div>}
            <div><span className="text-gray-500">Catalog:</span> {selected.catalog || "—"}</div>
            <div><span className="text-gray-500">Database:</span> {selected.database || "—"}</div>
            <div>
              <span className="text-gray-500">Node Query:</span>
              <pre className="mt-1 overflow-auto rounded bg-gray-50 p-2 font-mono text-xs">{selected.node_query || "—"}</pre>
            </div>
            <div>
              <span className="text-gray-500">Edge Query:</span>
              <pre className="mt-1 overflow-auto rounded bg-gray-50 p-2 font-mono text-xs">{selected.edge_query || "—"}</pre>
            </div>
            <div><span className="text-gray-500">S3 Bucket:</span> {selected.s3_staging_bucket || "—"}</div>
            <div><span className="text-gray-500">Graph ID:</span> {selected.graph_id || "—"}</div>
            {selected.graph_id && graphStatuses.get(selected.graph_id) && (
              <div><span className="text-gray-500">Graph Status:</span>{" "}
                <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${graphStatusStyle(graphStatuses.get(selected.graph_id)!)}`}>
                  {graphStatuses.get(selected.graph_id)}
                </span>
              </div>
            )}
            {summaries.get(selected.id) && (
              <>
                <div><span className="text-gray-500">Nodes:</span> {summaries.get(selected.id)!.numNodes.toLocaleString()}</div>
                <div><span className="text-gray-500">Edges:</span> {summaries.get(selected.id)!.numEdges.toLocaleString()}</div>
              </>
            )}
            {selected.timings && selected.timings.length > 0 && (
              <div>
                <span className="flex items-center gap-1 text-gray-500"><Clock className="h-3 w-3" /> Timing breakdown</span>
                <div className="mt-1 rounded bg-gray-50 p-2">
                  <TimingsList timings={selected.timings} />
                </div>
              </div>
            )}
            {selected.error && (
              <div>
                <span className="text-gray-500">Error:</span>
                <p className="mt-1 rounded bg-red-50 p-2 text-xs text-red-700">{selected.error}</p>
              </div>
            )}
          </div>
          <Button variant="secondary" className="w-full" onClick={() => navigate(`/import?session=${selected.id}`)}>
            Open in Import
          </Button>
          {selected.graph_id && selected.status !== "archived" && (
            <>
              <Button variant="ghost" className="w-full" onClick={() => {
                const graphDbUrl = `https://${selected.graph_id}.${region}.neptune-graph.amazonaws.com`;
                const params = new URLSearchParams({
                  graphDbUrl,
                  queryEngine: "openCypher",
                  awsRegion: region,
                  serviceType: "neptune-graph",
                  name: selected.graph_name || selected.graph_id || "",
                } as Record<string, string>);
                const geBase = (import.meta as any).env?.VITE_GRAPH_EXPLORER_URL || "https://localhost/explorer";
                window.open(`${geBase}/#/connect?${params}`, "_blank");
              }}>
                <ExternalLink className="h-3 w-3" /> Open in Graph Explorer
              </Button>
              <div className="flex gap-2">
                <Button
                  variant="secondary"
                  className="flex-1"
                  disabled={!actionStates[selected.graph_id]?.actions.includes("stop")}
                  onClick={() => performGraphAction(selected.graph_id!, "stop", selected.graph_name || selected.graph_id!)}
                >
                  <Square className="h-3 w-3" /> Stop
                </Button>
                <Button
                  variant="secondary"
                  className="flex-1"
                  disabled={!actionStates[selected.graph_id]?.actions.includes("start")}
                  onClick={() => performGraphAction(selected.graph_id!, "start", selected.graph_name || selected.graph_id!)}
                >
                  <Play className="h-3 w-3" /> Start
                </Button>
                <Button
                  variant="ghost"
                  className="flex-1 text-red-600 hover:text-red-700"
                  onClick={() => archiveSession(selected.id, selected.graph_name || selected.id.slice(0, 8))}
                >
                  <Trash2 className="h-3 w-3" /> Delete
                </Button>
              </div>
            </>
          )}
          {selected.status === "archived" && (
            <Button
              variant="ghost"
              className="w-full text-red-600 hover:text-red-700"
              onClick={() => purgeSession(selected.id, selected.graph_name || selected.id.slice(0, 8))}
            >
              Delete projection job
            </Button>
          )}
          {selected.graph_id && actionStates[selected.graph_id]?.inflight?.error && (
            <div className="flex items-start gap-2 rounded border border-red-200 bg-red-50 p-2 text-xs text-red-700">
              <AlertTriangle className="mt-0.5 h-3 w-3 flex-shrink-0" />
              <span>{actionStates[selected.graph_id]!.inflight!.error}</span>
            </div>
          )}
        </Card>
      )}

      {/* Hover popover for timing breakdown, rendered at root so the table's
          overflow-hidden cannot clip it. */}
      {timingHover && (() => {
        const s = sessions.find((x) => x.id === timingHover.id);
        if (!s?.timings || s.timings.length === 0) return null;
        return <TimingsPopover timings={s.timings} anchor={timingHover.anchor} />;
      })()}
    </div>
  );
}
