import { useEffect, useState, useCallback, useRef } from "react";
import { metadata, graphActions, Inflight } from "../api";
import { Card, RefreshButton } from "../components/ui";
import {
  TRANSIENT_POLL_MS,
  graphActionBlockedReason,
  isTransientStatus,
} from "../util/blocked";
import { Trash2, ExternalLink, Square, Play, X, AlertTriangle } from "lucide-react";

interface Graph {
  id: string;
  name: string;
  status: string;
}

interface Summary {
  numNodes: number;
  numEdges: number;
  nodeLabels: string[];
  edgeLabels: string[];
}

interface GraphActionState {
  actions: string[];
  inflight: Inflight | null;
}

export function Graphs() {
  const [graphs, setGraphs] = useState<Graph[]>([]);
  const [loading, setLoading] = useState(true);
  const [summaries, setSummaries] = useState<Record<string, Summary>>({});
  const [region, setRegion] = useState("");
  const [graphPrefix, setGraphPrefix] = useState("nxp-");
  const [actionStates, setActionStates] = useState<Record<string, GraphActionState>>({});
  const [alerts, setAlerts] = useState<{ graphId: string; graphName: string; message: string }[]>([]);
  const [loadError, setLoadError] = useState<string | null>(null);
  const inFlight = useRef(false);

  const loadActions = useCallback(async (graphList: Graph[]) => {
    for (const g of graphList) {
      graphActions.getActions(g.id).then(r => {
        setActionStates(prev => ({ ...prev, [g.id]: { actions: r.actions, inflight: r.inflight } }));
        if (r.inflight?.error) {
          setAlerts(prev => {
            if (prev.some(a => a.graphId === g.id)) return prev;
            return [...prev, { graphId: g.id, graphName: g.name, message: r.inflight!.error! }];
          });
        }
      }).catch(() => {});
    }
  }, []);

  const load = useCallback(async (opts?: { withActions?: boolean; withSummaries?: boolean; background?: boolean }) => {
    // One request at a time. The poll fires every TRANSIENT_POLL_MS, and the
    // AWS list call can take longer than that, so without this an older
    // response can resolve after a newer one and overwrite fresh rows with
    // stale ones — and a user action landing mid-poll races the same way.
    if (inFlight.current) return;
    inFlight.current = true;
    // `background` suppresses the spinner. The render used to swap the whole
    // table — Stop and Start buttons included — for "Loading...", which made
    // the table blink and pulled controls out from under the cursor.
    if (!opts?.background) setLoading(true);
    try {
      const data = await metadata.graphs();
      setGraphs(data.graphs);
      setLoadError(null);

      // Fetch summaries only on initial load or explicit request
      if (opts?.withSummaries) {
        for (const g of data.graphs) {
          if (g.status === "AVAILABLE") {
            metadata.graphSummary(g.id).then(s => setSummaries(prev => ({ ...prev, [g.id]: s }))).catch(() => {});
          }
        }
      }

      if (opts?.withActions) {
        loadActions(data.graphs);
      }
    } catch (e: any) {
      // Without this the spinner stayed up forever on a failed first request,
      // with no error and nothing to retry.
      setLoadError(e?.message || "Could not load graphs");
    } finally {
      setLoading(false);
      inFlight.current = false;
    }
  }, [loadActions]);

  useEffect(() => {
    metadata.config().then(c => { setRegion(c.region); setGraphPrefix(c.graph_prefix); });
    load({ withActions: true, withSummaries: true });
  }, [load]);

  // Re-poll while any graph is in a transient state.
  const hasTransient = graphs.some(g => isTransientStatus(g.status));

  useEffect(() => {
    if (!hasTransient) return;
    const interval = setInterval(
      () => load({ withActions: true, background: true }),
      TRANSIENT_POLL_MS,
    );
    return () => clearInterval(interval);
  }, [hasTransient, load]);

  async function performAction(graphId: string, action: string, graphName: string) {
    if (action === "delete" && !confirm(`Delete graph ${graphName}? This cannot be undone.`)) return;
    if (action === "stop" && !confirm(`Stop graph ${graphName}? It will become unavailable until restarted.`)) return;

    try {
      await graphActions.perform(graphId, action);
      // `background`: the user just clicked a button in this table, so blanking
      // it for "Loading..." takes the row out from under them at the moment
      // they are watching it. This is the most common trigger, not the poll.
      load({ withActions: true, background: true });
    } catch (e: any) {
      const message = e.message || `Failed to ${action}`;
      // Keyed by graphId, and dismissAlert removes every alert for that id, so
      // appending a second entry for one graph gives duplicate React keys and a
      // dismiss that clears more than the user clicked.
      setAlerts(prev =>
        prev.some(a => a.graphId === graphId)
          ? prev.map(a => (a.graphId === graphId ? { ...a, message } : a))
          : [...prev, { graphId, graphName, message }],
      );
    }
  }

  function dismissAlert(graphId: string) {
    setAlerts(prev => prev.filter(a => a.graphId !== graphId));
    graphActions.dismissInflight(graphId).catch(() => {});
  }

  const statusStyle = (status: string) => {
    switch (status) {
      case "AVAILABLE": return "bg-green-100 text-green-700";
      case "CREATING":
      case "STARTING": return "bg-blue-100 text-blue-700";
      case "DELETING":
      case "STOPPING": return "bg-red-100 text-red-700";
      case "STOPPED": return "bg-yellow-100 text-yellow-700";
      default: return "bg-gray-100 text-gray-700";
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold">Neptune Analytics Graphs</h1>
          <p className="text-sm text-gray-500">Showing graphs with <code className="rounded bg-gray-100 px-1">{graphPrefix}</code> prefix</p>
        </div>
        <RefreshButton onClick={() => load({ withActions: true, withSummaries: true })} />
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

      {loadError && graphs.length === 0 ? (
        <div role="alert" className="space-y-2">
          <p className="text-sm text-red-700">{loadError}</p>
          <RefreshButton
            label="Retry"
            onClick={() => load({ withActions: true, withSummaries: true })}
          />
        </div>
      ) : loading && graphs.length === 0 ? (
        // Only when there is nothing to show. Once rows exist, keep them up:
        // replacing a populated table with this text is the blanking bug.
        <p role="status" className="text-sm text-gray-500">Loading...</p>
      ) : graphs.length === 0 ? (
        <p className="text-sm text-gray-500">No graphs found in this region.</p>
      ) : (
        <Card className="overflow-hidden p-0">
          <table className="w-full text-left text-sm">
            <thead className="border-b bg-gray-50">
              <tr>
                <th className="px-4 py-3 font-medium">Name</th>
                <th className="px-4 py-3 font-medium">ID</th>
                <th className="px-4 py-3 font-medium">Nodes</th>
                <th className="px-4 py-3 font-medium">Edges</th>
                <th className="px-4 py-3 font-medium">Status</th>
                <th className="px-4 py-3 font-medium">Actions</th>
              </tr>
            </thead>
            <tbody>
              {graphs.map((g) => {
                const s = summaries[g.id];
                const state = actionStates[g.id];
                // null, not [], while the action list is unfetched or failed:
                // an empty list means "the service refused", not "unknown".
                const actions = state ? state.actions : null;
                const isTransient = isTransientStatus(g.status);
                const reasonFor = (action: "explore" | "stop" | "start" | "delete") =>
                  graphActionBlockedReason({
                    action,
                    graphId: g.id,
                    graphStatus: g.status,
                    available: actions,
                    isTransient,
                  });
                const exploreBlocked = reasonFor("explore");
                const stopBlocked = reasonFor("stop");
                const startBlocked = reasonFor("start");
                const deleteBlocked = reasonFor("delete");

                return (
                  <tr key={g.id} className="border-b last:border-0">
                    <td className="px-4 py-3 font-medium">{g.name}</td>
                    <td className="px-4 py-3 font-mono text-xs text-gray-600">{g.id}</td>
                    <td className="px-4 py-3">{s ? s.numNodes.toLocaleString() : "—"}</td>
                    <td className="px-4 py-3">{s ? s.numEdges.toLocaleString() : "—"}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusStyle(g.status)}`}>
                        {g.status}
                      </span>
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-1">
                        {/* Graph Explorer */}
                        <button
                          className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-blue-600 disabled:opacity-30 disabled:hover:bg-transparent"
                          disabled={exploreBlocked !== null}
                          aria-label="Open in Graph Explorer"
                          title={exploreBlocked || "Open in Graph Explorer"}
                          onClick={() => {
                            const graphDbUrl = `https://${g.id}.${region}.neptune-graph.amazonaws.com`;
                            const params = new URLSearchParams({
                              graphDbUrl,
                              queryEngine: "openCypher",
                              awsRegion: region,
                              serviceType: "neptune-graph",
                              name: g.name,
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
                          disabled={stopBlocked !== null}
                          aria-label="Stop graph"
                          title={stopBlocked || "Stop graph"}
                          onClick={() => performAction(g.id, "stop", g.name)}
                        >
                          <Square className="h-4 w-4" />
                        </button>

                        {/* Start */}
                        <button
                          className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-green-600 disabled:opacity-30 disabled:hover:bg-transparent"
                          disabled={startBlocked !== null}
                          aria-label="Start graph"
                          title={startBlocked || "Start graph"}
                          onClick={() => performAction(g.id, "start", g.name)}
                        >
                          <Play className="h-4 w-4" />
                        </button>

                        {/* Delete */}
                        <button
                          className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-red-600 disabled:opacity-30 disabled:hover:bg-transparent"
                          disabled={deleteBlocked !== null}
                          aria-label="Delete graph"
                          title={deleteBlocked || "Delete graph"}
                          onClick={() => performAction(g.id, "delete", g.name)}
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
      )}
    </div>
  );
}
