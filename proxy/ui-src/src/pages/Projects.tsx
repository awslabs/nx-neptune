import { useState } from "react";
import { useNavigate } from "react-router";
import { projectApi } from "../api";
import { Card, Button } from "../components/ui";

export function Projects() {
  const [name, setName] = useState("");
  const navigate = useNavigate();

  async function handleCreate() {
    if (!name.trim()) return;
    const project = await projectApi.create(name.trim());
    setName("");
    window.dispatchEvent(new Event("projects-changed"));
    navigate(`/sessions?project=${project.id}`);
  }

  return (
    <div className="mx-auto max-w-2xl space-y-6">
      <h1 className="text-lg font-semibold">Projects</h1>
      <p className="text-sm text-gray-600">
        A project is a logical group for your projections. Create a project here, then select it on the Import page to associate projections with it.
      </p>

      <Card>
        <h2 className="mb-3 text-sm font-medium">Create Project</h2>
        <div className="flex gap-2">
          <input
            className="flex-1 rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
            placeholder="Project name"
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
