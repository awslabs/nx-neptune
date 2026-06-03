import { useEffect, useState } from "react";
import { metadata } from "../api";
import { Card, Button } from "../components/ui";
import { RefreshCw } from "lucide-react";

interface Graph {
  id: string;
  name: string;
  status: string;
}

export function Graphs() {
  const [graphs, setGraphs] = useState<Graph[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => { load(); }, []);

  async function load() {
    setLoading(true);
    const data = await metadata.graphs();
    setGraphs(data.graphs);
    setLoading(false);
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold">Neptune Analytics Graphs</h1>
          <p className="text-sm text-gray-500">Showing graphs with <code className="rounded bg-gray-100 px-1">nxp-</code> prefix</p>
        </div>
        <Button variant="ghost" onClick={load}><RefreshCw className="h-4 w-4" /></Button>
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
                <th className="px-4 py-3 font-medium">Status</th>
              </tr>
            </thead>
            <tbody>
              {graphs.map((g) => (
                <tr key={g.id} className="border-b last:border-0">
                  <td className="px-4 py-3 font-medium">{g.name}</td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-600">{g.id}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                      g.status === "AVAILABLE" ? "bg-green-100 text-green-700" :
                      g.status === "CREATING" ? "bg-blue-100 text-blue-700" :
                      g.status === "DELETING" ? "bg-red-100 text-red-700" :
                      "bg-gray-100 text-gray-700"
                    }`}>{g.status}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </Card>
      )}
    </div>
  );
}
