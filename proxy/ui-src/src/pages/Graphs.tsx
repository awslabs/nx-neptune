import { useEffect, useState, useCallback } from "react";
import { metadata, graphActions, Inflight } from "../api";
import { Card, RefreshButton } from "../components/ui";
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
  const [actionStates, setActionStates] = useState<Record<string, GraphActionState>>({});
  const [alerts, setAlerts] = useState<{ graphId: string; graphName: string; message: string }[]>([]);

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

  const load = useCallback(async (opts?: { withActions?: boolean; withSummaries?: boolean }) => {
    setLoading(true);
    const data = await metadata.graphs();
    setGraphs(data.graphs);
    setLoading(false);

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
  }, [loadActions]);

  useEffect(() => {
    metadata.config().then(c => setRegion(c.region));
    load({ withActions: true, withSummaries: true });
  }, [load]);

  // Poll: 5s during transient states, 30s otherwise
  const hasTransient = graphs.some(g =>
    ["STOPPING", "STARTING", "DELETING", "CREATING"].includes(g.status)
  );

  useEffect(() => {
    if (!hasTransient) return;
    const interval = setInterval(() => load({ withActions: true }), 5000);
    return () => clearInterval(interval);
  }, [hasTransient, load]);

  async function performAction(graphId: string, action: string, graphName: string) {
    if (action === "delete" && !confirm(`Delete graph ${graphName}? This cannot be undone.`)) return;
    if (action === "stop" && !confirm(`Stop graph ${graphName}? It will become unavailable until restarted.`)) return;

    try {
      await graphActions.perform(graphId, action);
      load({ withActions: true });
    } catch (e: any) {
      setAlerts(prev => [...prev, { graphId, graphName, message: e.message || `Failed to ${action}` }]);
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
          <p className="text-sm text-gray-500">Showing graphs with <code className="rounded bg-gray-100 px-1">nxp-</code> prefix</p>
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

      {loading ? (
        <p className="text-sm text-gray-500">Loading...</p>
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
                const actions = state?.actions || [];
                const isTransient = ["STOPPING", "STARTING", "DELETING", "CREATING"].includes(g.status);

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
                          disabled={g.status !== "AVAILABLE"}
                          title="Open in Graph Explorer"
                          onClick={() => {
                            const graphDbUrl = `https://${g.id}.${region}.neptune-graph.amazonaws.com`;
                            const params = new URLSearchParams({
                              graphDbUrl,
                              queryEngine: "openCypher",
                              awsRegion: region,
                              serviceType: "neptune-graph",
                              name: g.name,
                            });
                            const geBase = (import.meta as any).env?.VITE_GRAPH_EXPLORER_URL || "https://localhost";
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
                          onClick={() => performAction(g.id, "stop", g.name)}
                        >
                          <Square className="h-4 w-4" />
                        </button>

                        {/* Start */}
                        <button
                          className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-green-600 disabled:opacity-30 disabled:hover:bg-transparent"
                          disabled={!actions.includes("start") || isTransient}
                          title="Start graph"
                          onClick={() => performAction(g.id, "start", g.name)}
                        >
                          <Play className="h-4 w-4" />
                        </button>

                        {/* Delete */}
                        <button
                          className="rounded p-1 text-gray-400 hover:bg-gray-100 hover:text-red-600 disabled:opacity-30 disabled:hover:bg-transparent"
                          disabled={!actions.includes("delete") || isTransient}
                          title="Delete graph"
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
