import { useEffect, useState } from "react";
import { projection, type Projection } from "../api";
import { Card, Button, RefreshButton } from "../components/ui";
import { X, Download } from "lucide-react";
import { useNavigate } from "react-router";

export function Sessions() {
  const [sessions, setSessions] = useState<Projection[]>([]);
  const [selected, setSelected] = useState<Projection | null>(null);
  const [copied, setCopied] = useState(false);
  const navigate = useNavigate();

  useEffect(() => { load(); }, []);

  async function load() {
    const list = await projection.list();
    setSessions(list);
  }

  return (
    <div className="flex gap-4">
      <div className="flex-1 space-y-4">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold">Sessions</h1>
          <RefreshButton onClick={load} />
        </div>

        {sessions.length === 0 ? (
          <p className="text-sm text-gray-500">No sessions yet. Create one from the Import page.</p>
        ) : (
          <Card className="overflow-hidden p-0">
            <table className="w-full text-left text-sm">
              <thead className="border-b bg-gray-50">
                <tr>
                  <th className="px-4 py-3 font-medium">Name</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                  <th className="px-4 py-3 font-medium">Database</th>
                  <th className="px-4 py-3 font-medium">Progress</th>
                  <th className="px-4 py-3 font-medium">Created</th>
                </tr>
              </thead>
              <tbody>
                {sessions.map((s) => (
                  <tr
                    key={s.id}
                    className={`cursor-pointer border-b last:border-0 hover:bg-gray-50 ${selected?.id === s.id ? "bg-blue-50" : ""}`}
                    onClick={() => setSelected(s)}
                    onDoubleClick={() => navigate(`/import?session=${s.id}`)}
                  >
                    <td className="px-4 py-3 font-medium">{s.graph_name || s.id.slice(0, 8)}</td>
                    <td className="px-4 py-3">
                      <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                        s.status === "complete" ? "bg-green-100 text-green-700" :
                        s.status === "failed" ? "bg-red-100 text-red-700" :
                        s.status === "executing" ? "bg-blue-100 text-blue-700" :
                        "bg-gray-100 text-gray-700"
                      }`}>{s.status}</span>
                    </td>
                    <td className="px-4 py-3 text-gray-600">{s.database || "—"}</td>
                    <td className="px-4 py-3">{Math.round(s.progress)}%</td>
                    <td className="px-4 py-3 text-gray-500">{new Date(s.created_at).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card>
        )}
      </div>

      {selected && (
        <Card className="w-80 shrink-0 space-y-3 self-start">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold">{selected.graph_name || selected.id.slice(0, 8)}</h2>
            <button onClick={() => setSelected(null)} className="text-gray-400 hover:text-gray-600"><X className="h-4 w-4" /></button>
          </div>
          <div className="space-y-2 text-sm">
            <div><span className="text-gray-500">Catalog:</span> {selected.catalog || "—"}</div>
            <div><span className="text-gray-500">Database:</span> {selected.database || "—"}</div>
            <div>
              <span className="text-gray-500">Query:</span>
              <pre className="mt-1 overflow-auto rounded bg-gray-50 p-2 font-mono text-xs">{selected.sql_query || "—"}</pre>
            </div>
            <div><span className="text-gray-500">S3 Bucket:</span> {selected.s3_staging_bucket || "—"}</div>
            <div><span className="text-gray-500">Graph ID:</span> {selected.graph_id || "—"}</div>
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
          {selected.graph_id && selected.status === "complete" && (
            <Button variant="ghost" className="w-full" onClick={() => {
              const config = {
                id: selected.graph_id,
                displayLabel: selected.graph_name || selected.graph_id,
                connection: {
                  url: `https://${selected.graph_id}.neptune-graph.amazonaws.com`,
                  queryEngine: "openCypher",
                  proxyConnection: true,
                  graphDbUrl: `https://${selected.graph_id}.neptune-graph.amazonaws.com`,
                  awsAuthEnabled: true,
                  serviceType: "neptune-graph",
                },
                schema: { vertices: [], edges: [] },
              };
              const blob = new Blob([JSON.stringify(config, null, 2)], { type: "application/json" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = `${selected.graph_name || selected.graph_id}.json`;
              a.click();
              URL.revokeObjectURL(url);
            }}>
              <Download className="h-3 w-3" /> Export for Graph Explorer
            </Button>
          )}
        </Card>
      )}
    </div>
  );
}
