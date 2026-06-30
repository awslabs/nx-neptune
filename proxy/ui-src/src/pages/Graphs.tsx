import { useEffect, useState } from "react";
import { metadata } from "../api";
import { Card, RefreshButton } from "../components/ui";
import { Trash2, ExternalLink } from "lucide-react";

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

export function Graphs() {
  const [graphs, setGraphs] = useState<Graph[]>([]);
  const [loading, setLoading] = useState(true);
  const [summaries, setSummaries] = useState<Record<string, Summary>>({});
  const [region, setRegion] = useState("");

  useEffect(() => {
    metadata.config().then(c => setRegion(c.region));
    load();
  }, []);

  async function load() {
    setLoading(true);
    const data = await metadata.graphs();
    setGraphs(data.graphs);
    setLoading(false);
    // Fetch summaries for available graphs
    for (const g of data.graphs) {
      if (g.status === "AVAILABLE") {
        metadata.graphSummary(g.id).then(s => setSummaries(prev => ({ ...prev, [g.id]: s }))).catch(() => {});
      }
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold">Neptune Analytics Graphs</h1>
          <p className="text-sm text-gray-500">Showing graphs with <code className="rounded bg-gray-100 px-1">nxp-</code> prefix</p>
        </div>
        <RefreshButton onClick={load} />
      </div>

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
                <th className="px-4 py-3 font-medium"></th>
              </tr>
            </thead>
            <tbody>
              {graphs.map((g) => {
                const s = summaries[g.id];
                return (
                  <tr key={g.id} className="border-b last:border-0">
                    <td className="px-4 py-3 font-medium">{g.name}</td>
                    <td className="px-4 py-3 font-mono text-xs text-gray-600">{g.id}</td>
                    <td className="px-4 py-3">{s ? s.numNodes.toLocaleString() : "—"}</td>
                    <td className="px-4 py-3">{s ? s.numEdges.toLocaleString() : "—"}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                        g.status === "AVAILABLE" ? "bg-green-100 text-green-700" :
                        g.status === "CREATING" ? "bg-blue-100 text-blue-700" :
                        g.status === "DELETING" ? "bg-red-100 text-red-700" :
                        "bg-gray-100 text-gray-700"
                      }`}>{g.status}</span>
                    </td>
                    <td className="px-4 py-3 flex gap-2">
                      <button
                        className="text-gray-400 hover:text-blue-600 disabled:opacity-30"
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
                      ><ExternalLink className="h-4 w-4" /></button>
                      <button
                        className="text-gray-400 hover:text-red-600 disabled:opacity-30"
                        disabled={g.status === "DELETING"}
                        onClick={async () => { if (confirm(`Delete graph ${g.name}?`)) { await metadata.deleteGraph(g.id); load(); } }}
                      ><Trash2 className="h-4 w-4" /></button>
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
