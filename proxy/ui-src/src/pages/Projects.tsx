import { useState, useRef, useEffect } from "react";
import { useNavigate } from "react-router";
import { projectApi, metadata } from "../api";
import { Card, Button } from "../components/ui";

export function Projects() {
  const [name, setName] = useState("");
  const [importStatus, setImportStatus] = useState<string | null>(null);
  const [exportBucket, setExportBucket] = useState<string | null>(null);
  const [importDropdownOpen, setImportDropdownOpen] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const importDropdownRef = useRef<HTMLDivElement>(null);
  const navigate = useNavigate();

  useEffect(() => {
    metadata.config().then(c => setExportBucket(c.export_bucket));
  }, []);

  // Close dropdown on click outside
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (importDropdownRef.current && !importDropdownRef.current.contains(e.target as Node)) {
        setImportDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, []);

  async function handleCreate() {
    if (!name.trim()) return;
    const project = await projectApi.create(name.trim());
    setName("");
    window.dispatchEvent(new Event("projects-changed"));
    navigate(`/projections?project=${project.id}`);
  }

  async function handleFileSelected(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;

    try {
      const text = await file.text();
      const data = JSON.parse(text);
      const result = await projectApi.import(data);
      setImportStatus(`Imported project "${result.imported.name}" successfully.`);
      window.dispatchEvent(new Event("projects-changed"));
    } catch (err) {
      setImportStatus(`Import failed: ${err instanceof Error ? err.message : "Invalid file"}`);
    }

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
          <div className="relative" ref={importDropdownRef}>
            <Button onClick={() => setImportDropdownOpen(!importDropdownOpen)}>Import</Button>
            {importDropdownOpen && (
              <div className="absolute right-0 z-10 mt-1 w-36 rounded-md border border-gray-200 bg-white py-1 shadow-lg">
                <button
                  className="block w-full px-3 py-1.5 text-left text-sm text-gray-700 hover:bg-gray-100"
                  onClick={() => {
                    setImportDropdownOpen(false);
                    fileInputRef.current?.click();
                  }}
                >
                  Upload file
                </button>
                {exportBucket && (
                  <button
                    className="block w-full px-3 py-1.5 text-left text-sm text-gray-700 hover:bg-gray-100"
                    onClick={() => {
                      setImportDropdownOpen(false);
                      window.dispatchEvent(new Event("open-s3-import-dialog"));
                    }}
                  >
                    Load from S3
                  </button>
                )}
              </div>
            )}
          </div>
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
