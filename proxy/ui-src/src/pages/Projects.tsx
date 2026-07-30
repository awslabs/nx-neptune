import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { projectApi } from "../api";
import { Card, Button } from "../components/ui";

export function Projects() {
  const [name, setName] = useState("");
  const [importStatus, setImportStatus] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();

  async function handleCreate() {
    if (!name.trim()) return;
    const project = await projectApi.create(name.trim());
    setName("");
    window.dispatchEvent(new Event("projects-changed"));
    navigate(`/projections?project=${project.id}`);
  }

  async function handleExportAll() {
    try {
      const data = await projectApi.exportAll();
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "projects-export.json";
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      setImportStatus(`Export failed: ${e instanceof Error ? e.message : "Unknown error"}`);
    }
  }

  function handleImportClick() {
    fileInputRef.current?.click();
  }

  async function handleFileSelected(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;

    try {
      const text = await file.text();
      const data = JSON.parse(text);
      const result = await projectApi.import(data);
      const count = result.imported.length;
      setImportStatus(`Imported ${count} project${count !== 1 ? "s" : ""} successfully.`);
      window.dispatchEvent(new Event("projects-changed"));
    } catch (err) {
      setImportStatus(`Import failed: ${err instanceof Error ? err.message : "Invalid file"}`);
    }

    // Reset input so the same file can be re-selected
    e.target.value = "";
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

      <Card>
        <h2 className="mb-3 text-sm font-medium">Import / Export</h2>
        <div className="flex gap-2">
          <Button onClick={handleExportAll}>Export All</Button>
          <Button onClick={handleImportClick}>Import</Button>
          <input
            ref={fileInputRef}
            type="file"
            accept=".json,application/json"
            className="hidden"
            onChange={handleFileSelected}
            aria-label="Import project file"
          />
        </div>
        {importStatus && (
          <p className="mt-2 text-sm text-gray-600">{importStatus}</p>
        )}
      </Card>
    </div>
  );
}
