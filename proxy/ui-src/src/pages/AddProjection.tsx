import { useState } from "react";
import { useNavigate } from "react-router";
import { Card } from "../components/ui";
import { addProjection } from "../data/projections";

export function AddProjection() {
  const navigate = useNavigate();
  const [name, setName] = useState("");
  const [catalog, setCatalog] = useState("AwsDataCatalog");
  const [database, setDatabase] = useState("");
  const [nodeQuery, setNodeQuery] = useState("");
  const [edgeQuery, setEdgeQuery] = useState("");
  const [s3Bucket, setS3Bucket] = useState("");
  const [project, setProject] = useState("");

  function handleCreate() {
    addProjection({
      name,
      catalog,
      database,
      nodeQuery,
      edgeQuery,
      s3Bucket,
      project: project || null,
    });
    navigate("/query-manager");
  }

  return (
    <div className="mx-auto max-w-xl space-y-6">
      <div>
        <h1 className="text-lg font-semibold">Add Saved Projection</h1>
        <p className="text-sm text-gray-500">Create a reusable projection configuration</p>
      </div>

      <Card>
        <div className="space-y-4">
          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Name</span>
            <input
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              placeholder="My Projection"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </label>

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Catalog</span>
            <input
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              value={catalog}
              onChange={(e) => setCatalog(e.target.value)}
            />
          </label>

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Database</span>
            <input
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              placeholder="my_database"
              value={database}
              onChange={(e) => setDatabase(e.target.value)}
            />
          </label>

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Node Query</span>
            <textarea
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              rows={3}
              placeholder='SELECT id AS "~id", type AS "~label", ... FROM nodes_table'
              value={nodeQuery}
              onChange={(e) => setNodeQuery(e.target.value)}
            />
          </label>

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Edge Query</span>
            <textarea
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              rows={3}
              placeholder='SELECT id AS "~id", type AS "~label", src AS "~from", dst AS "~to" FROM edges_table'
              value={edgeQuery}
              onChange={(e) => setEdgeQuery(e.target.value)}
            />
          </label>

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">S3 Staging Bucket</span>
            <input
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              placeholder="s3://my-bucket/"
              value={s3Bucket}
              onChange={(e) => setS3Bucket(e.target.value)}
            />
          </label>

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Project (optional)</span>
            <input
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              placeholder="MyProject"
              value={project}
              onChange={(e) => setProject(e.target.value)}
            />
          </label>
        </div>
      </Card>

      <div className="flex gap-3">
        <button
          onClick={handleCreate}
          disabled={!name.trim() || !database.trim()}
          className="inline-flex items-center rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          Create Projection
        </button>
        <button
          onClick={() => navigate("/query-manager")}
          className="inline-flex items-center rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
