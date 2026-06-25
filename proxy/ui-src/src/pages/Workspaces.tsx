import { useState } from "react";
import { workspaceApi } from "../api";
import { Card, Button } from "../components/ui";

export function Workspaces() {
  const [name, setName] = useState("");

  async function handleCreate() {
    if (!name.trim()) return;
    await workspaceApi.create(name.trim());
    setName("");
    window.dispatchEvent(new Event("workspaces-changed"));
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
    </div>
  );
}
