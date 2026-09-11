import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { projectApi } from "../api";
import { X } from "lucide-react";

interface S3File {
  key: string;
  filename: string;
  last_modified: string;
}

export function S3ImportDialog() {
  const [open, setOpen] = useState(false);
  const [files, setFiles] = useState<S3File[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedFile, setSelectedFile] = useState<S3File | null>(null);
  const [importing, setImporting] = useState(false);
  const navigate = useNavigate();

  // Listen for the custom event to open the dialog
  useEffect(() => {
    function handleOpen() {
      setOpen(true);
      setSelectedFile(null);
      setError(null);
      loadFiles();
    }
    window.addEventListener("open-s3-import-dialog", handleOpen);
    return () => window.removeEventListener("open-s3-import-dialog", handleOpen);
  }, []);

  async function loadFiles() {
    setLoading(true);
    setError(null);
    try {
      const result = await projectApi.listS3Exports();
      setFiles(result.files);
    } catch (e: any) {
      setError(e.message || "Failed to list files");
      setFiles([]);
    } finally {
      setLoading(false);
    }
  }

  async function handleImport() {
    if (!selectedFile) return;
    setImporting(true);
    try {
      const result = await projectApi.importFromS3(selectedFile.key);
      window.dispatchEvent(new Event("projects-changed"));
      setOpen(false);
      navigate(`/projections?project=${result.imported.id}`);
    } catch (e: any) {
      setError(e.message || "Import failed");
    } finally {
      setImporting(false);
    }
  }

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50" onClick={() => setOpen(false)}>
      <div className="w-full max-w-md rounded-lg bg-white p-6 shadow-xl" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-semibold">Import from S3</h2>
          <button onClick={() => setOpen(false)} className="text-gray-400 hover:text-gray-600">
            <X className="h-4 w-4" />
          </button>
        </div>

        {loading && <p className="text-sm text-gray-500">Loading...</p>}

        {error && (
          <p className="mb-3 rounded bg-red-50 p-2 text-xs text-red-700">{error}</p>
        )}

        {!loading && files.length === 0 && !error && (
          <div className="py-6">
            <table className="w-full text-sm">
              <thead className="border-b">
                <tr>
                  <th className="pb-2 text-left font-medium text-gray-500">Filename</th>
                  <th className="pb-2 text-left font-medium text-gray-500">Modified</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td colSpan={2} className="py-4 text-center text-gray-400">No exports found</td>
                </tr>
              </tbody>
            </table>
          </div>
        )}

        {!loading && files.length > 0 && (
          <div className="max-h-64 overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="border-b sticky top-0 bg-white">
                <tr>
                  <th className="pb-2 text-left font-medium text-gray-500">Filename</th>
                  <th className="pb-2 text-left font-medium text-gray-500">Modified</th>
                </tr>
              </thead>
              <tbody>
                {files.map((file) => (
                  <tr
                    key={file.key}
                    className={`cursor-pointer border-b last:border-0 hover:bg-gray-50 ${selectedFile?.key === file.key ? "bg-blue-50" : ""}`}
                    onClick={() => setSelectedFile(file)}
                  >
                    <td className="py-2 pr-4 font-mono text-xs">{file.filename}</td>
                    <td className="py-2 text-xs text-gray-500">
                      {new Date(file.last_modified).toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {selectedFile && (
          <div className="mt-4 flex items-center justify-between rounded border border-blue-200 bg-blue-50 p-3">
            <p className="text-xs text-blue-800">
              Import <span className="font-medium">{selectedFile.filename}</span>?
            </p>
            <div className="flex gap-2">
              <button
                onClick={() => setSelectedFile(null)}
                className="rounded px-2 py-1 text-xs text-gray-600 hover:bg-gray-200"
              >
                Cancel
              </button>
              <button
                onClick={handleImport}
                disabled={importing}
                className="rounded bg-blue-600 px-3 py-1 text-xs font-medium text-white hover:bg-blue-700 disabled:opacity-50"
              >
                {importing ? "Importing..." : "Import"}
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
