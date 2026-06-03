import { useEffect, useState } from "react";
import { projection, type Projection } from "../api";
import { Card, Button } from "../components/ui";
import { RefreshCw } from "lucide-react";
import { useNavigate } from "react-router";

export function Sessions() {
  const [sessions, setSessions] = useState<Projection[]>([]);
  const navigate = useNavigate();

  useEffect(() => { load(); }, []);

  async function load() {
    const list = await projection.list();
    setSessions(list);
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">Sessions</h1>
        <Button variant="ghost" onClick={load}><RefreshCw className="h-4 w-4" /></Button>
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
                  className="cursor-pointer border-b last:border-0 hover:bg-gray-50"
                  onClick={() => navigate(`/import`)}
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
  );
}
