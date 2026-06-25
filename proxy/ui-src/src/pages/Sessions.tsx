import { useEffect, useState } from "react";
import { useSearchParams } from "react-router";
import { projection, metadata, workspaceApi, type Projection, type Workspace } from "../api";
import { Card, Button, RefreshButton } from "../components/ui";
import { X, ExternalLink } from "lucide-react";
import { useNavigate } from "react-router";

export function Sessions() {
  const [searchParams] = useSearchParams();
  const [sessions, setSessions] = useState<Projection[]>([]);
  const [selected, setSelected] = useState<Projection | null>(null);
  const [region, setRegion] = useState("");
  const [workspaces, setWorkspaces] = useState<Map<string, Workspace>>(new Map());
  const navigate = useNavigate();
  const filterWorkspaceId = searchParams.get("workspace");

  useEffect(() => {
    load();
    metadata.config().then(c => setRegion(c.region));
    workspaceApi.list().then(list => setWorkspaces(new Map(list.map(ws => [ws.id, ws]))));
  }, []);

  async function load() {
    const list = await projection.list();
    setSessions(list);
  }

  const filtered = filterWorkspaceId
    ? sessions.filter(s => s.workspace_id === filterWorkspaceId)
    : sessions;
  const workspaceName = filterWorkspaceId ? workspaces.get(filterWorkspaceId)?.name : null;

  return (
    <div className="flex gap-4">
      <div className="flex-1 space-y-4">
        <div className="flex items-center justify-between">
          <h1 className="text-lg font-semibold">{workspaceName ? `${workspaceName} — Sessions` : "Sessions"}</h1>
          <RefreshButton onClick={load} />
        </div>

        {filtered.length === 0 ? (
          <p className="text-sm text-gray-500">No sessions yet. Create one from the Import page.</p>
        ) : (
          <Card className="overflow-hidden p-0">
            <table className="w-full text-left text-sm">
              <thead className="border-b bg-gray-50">
                <tr>
                  <th className="px-4 py-3 font-medium">Name</th>
                  <th className="px-4 py-3 font-medium">Workspace</th>
                  <th className="px-4 py-3 font-medium">Status</th>
                  <th className="px-4 py-3 font-medium">Database</th>
                  <th className="px-4 py-3 font-medium">Progress</th>
                  <th className="px-4 py-3 font-medium">Created</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((s) => (
                  <tr
                    key={s.id}
                    className={`cursor-pointer border-b last:border-0 hover:bg-gray-50 ${selected?.id === s.id ? "bg-blue-50" : ""}`}
                    onClick={() => setSelected(s)}
                    onDoubleClick={() => navigate(`/import?session=${s.id}`)}
                  >
                    <td className="px-4 py-3 font-medium">{s.graph_name || s.id.slice(0, 8)}</td>
                    <td className="px-4 py-3 text-gray-600">{s.workspace_id ? workspaces.get(s.workspace_id)?.name || "—" : "—"}</td>
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
            {selected.workspace_id && <div><span className="text-gray-500">Workspace:</span> {workspaces.get(selected.workspace_id)?.name || selected.workspace_id}</div>}
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
              const graphDbUrl = `https://${selected.graph_id}.${region}.neptune-graph.amazonaws.com`;
              const params = new URLSearchParams({
                graphDbUrl,
                queryEngine: "openCypher",
                awsRegion: region,
                serviceType: "neptune-graph",
                name: selected.graph_name || selected.graph_id || "",
              } as Record<string, string>);
              const geBase = (import.meta as any).env?.VITE_GRAPH_EXPLORER_URL || "https://localhost";
              window.open(`${geBase}?${params}`, "_blank");
            }}>
              <ExternalLink className="h-3 w-3" /> Open in Graph Explorer
            </Button>
          )}
        </Card>
      )}
    </div>
  );
}
