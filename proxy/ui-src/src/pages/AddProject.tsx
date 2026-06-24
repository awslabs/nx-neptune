import { useState } from "react";
import { Card } from "../components/ui";

export function AddProject() {
  const [name, setName] = useState("");

  return (
    <div className="mx-auto max-w-xl space-y-6">
      <div>
        <h1 className="text-lg font-semibold">Add Project</h1>
        <p className="text-sm text-gray-500">Create a new project to organize your graphs</p>
      </div>

      <Card>
        <div className="space-y-4">
          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Project Name</span>
            <input
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              placeholder="my-project"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </label>
        </div>
      </Card>

      <button
        disabled={!name.trim()}
        className="inline-flex items-center rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        Create Project
      </button>
    </div>
  );
}
