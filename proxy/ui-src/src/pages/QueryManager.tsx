import { useState, useEffect } from "react";
import { useNavigate } from "react-router";
import { Card } from "../components/ui";
import { Plus, Trash2 } from "lucide-react";
import { getProjections, deleteProjection, subscribe, type SavedProjection } from "../data/projections";

export function QueryManager() {
  const navigate = useNavigate();
  const [projections, setProjections] = useState<SavedProjection[]>(getProjections());

  useEffect(() => {
    return subscribe(() => setProjections(getProjections()));
  }, []);

  return (
    <div className="mx-auto max-w-4xl space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold">Query Manager</h1>
          <p className="text-sm text-gray-500">Saved projections for graph imports</p>
        </div>
        <button
          onClick={() => navigate("/add-projection")}
          className="inline-flex items-center gap-2 rounded-md bg-blue-600 px-3 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700"
        >
          <Plus className="h-4 w-4" />
          New Projection
        </button>
      </div>

      {projections.length === 0 ? (
        <p className="text-sm text-gray-500">No saved projections yet.</p>
      ) : (
        <Card className="overflow-hidden p-0">
          <table className="w-full text-left text-sm">
            <thead className="border-b bg-gray-50">
              <tr>
                <th className="px-4 py-3 font-medium">Name</th>
                <th className="px-4 py-3 font-medium">Database</th>
                <th className="px-4 py-3 font-medium">S3 Bucket</th>
                <th className="px-4 py-3 font-medium">Project</th>
                <th className="px-4 py-3 font-medium">Added Date</th>
                <th className="px-4 py-3 font-medium"></th>
              </tr>
            </thead>
            <tbody>
              {projections.map((p) => (
                <tr key={p.id} className="border-b last:border-0">
                  <td className="px-4 py-3 font-medium">{p.name}</td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-600">{p.database}</td>
                  <td className="px-4 py-3 font-mono text-xs text-gray-600">{p.s3Bucket}</td>
                  <td className="px-4 py-3 text-gray-600">{p.project || "—"}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{p.addedDate}</td>
                  <td className="px-4 py-3">
                    <button
                      onClick={() => deleteProjection(p.id)}
                      className="text-gray-400 hover:text-red-600"
                      title="Delete projection"
                    >
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
