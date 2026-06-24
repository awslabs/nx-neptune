import { useNavigate } from "react-router";
import { Plus } from "lucide-react";

interface MockGraph {
  id: string;
  name: string;
  status: "AVAILABLE" | "CREATING";
}

const MOCK_GRAPHS: MockGraph[] = [
  { id: "g-abc123def4", name: "Social Network", status: "AVAILABLE" },
  { id: "g-xyz789ghi0", name: "Fraud Detection", status: "AVAILABLE" },
  { id: "g-mno456pqr1", name: "Supply Chain", status: "CREATING" },
];

export function Project() {
  const navigate = useNavigate();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-lg font-semibold">MyProject</h1>
        <p className="text-sm text-gray-500">Project overview</p>
      </div>

      <div className="rounded-lg border border-gray-200 bg-white shadow-sm">
        {MOCK_GRAPHS.map((g, i) => (
          <div
            key={g.id}
            className={`flex items-center justify-between px-4 py-3 ${i < MOCK_GRAPHS.length - 1 ? "border-b border-gray-200" : ""}`}
          >
            <div className="flex items-center gap-3">
              <h3 className="text-sm font-medium text-gray-900">{g.name}</h3>
              <span className="font-mono text-xs text-gray-500">{g.id}</span>
            </div>
            <span
              className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                g.status === "AVAILABLE"
                  ? "bg-green-100 text-green-700"
                  : "bg-blue-100 text-blue-700"
              }`}
            >
              {g.status}
            </span>
          </div>
        ))}
      </div>

      <button
        onClick={() => navigate("/import")}
        className="inline-flex items-center gap-2 rounded-md border border-dashed border-gray-300 px-4 py-2 text-sm text-gray-600 hover:border-blue-400 hover:text-blue-600"
      >
        <Plus className="h-4 w-4" />
        Add Graph
      </button>
    </div>
  );
}
