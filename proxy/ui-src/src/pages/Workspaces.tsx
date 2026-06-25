import { useEffect, useState } from "react";
import { workspaceApi, type Workspace } from "../api";
import { Card, Button } from "../components/ui";
import { Trash2 } from "lucide-react";

export function Workspaces() {
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [name, setName] = useState("");

  useEffect(() => { workspaceApi.list().then(setWorkspaces); }, []);

  async function handleCreate() {
    if (!name.trim()) return;
    const ws = await workspaceApi.create(name.trim());
    setWorkspaces(prev => [...prev, ws]);
    setName("");
  }

  async function handleDelete(id: string) {
    await workspaceApi.delete(id);
    setWorkspaces(prev => prev.filter(ws => ws.id !== id));
  }

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <h1 className="text-lg font-semibold">Workspaces</h1>
      <p className="text-sm text-gray-600">
        A workspace is a logical group for your projections. Create a workspace here, then select it on the Import page to associate projections with it.
      </p>

      <Card>
        <h2 className="mb-3 text-sm font-medium">Create Workspace</h2>
        <div className="flex gap-2">
          <input
            className="flex-1 rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            placeholder="Workspace name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleCreate()}
          />
          <Button onClick={handleCreate} disabled={!name.trim()}>Create</Button>
        </div>
      </Card>

      {workspaces.length > 0 && (
        <Card className="overflow-hidden p-0">
          <table className="w-full text-left text-sm">
            <thead className="border-b bg-gray-50">
              <tr>
                <th className="px-4 py-3 font-medium">Name</th>
                <th className="px-4 py-3 font-medium">Created</th>
                <th className="px-4 py-3 font-medium"></th>
              </tr>
            </thead>
            <tbody>
              {workspaces.map((ws) => (
                <tr key={ws.id} className="border-b last:border-0">
                  <td className="px-4 py-3 font-medium">{ws.name}</td>
                  <td className="px-4 py-3 text-gray-500">{new Date(ws.created_at).toLocaleString()}</td>
                  <td className="px-4 py-3 text-right">
                    <button onClick={() => handleDelete(ws.id)} className="text-gray-400 hover:text-red-600">
                      <Trash2 className="h-4 w-4" />
                    </button>
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
