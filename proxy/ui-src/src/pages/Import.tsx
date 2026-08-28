import { useEffect, useState, useCallback, useRef } from "react";
import { useSearchParams } from "react-router";
import { metadata, projection, projectApi, type Projection, type ProjectionStatus, type Project, type NodeQueryInput, type EdgeQueryInput } from "../api";
import { Button, Select, ProgressBar, Card, RefreshButton } from "../components/ui";
import { Play, CheckCircle, Eye, Plus, Trash2 } from "lucide-react";

export function Import() {
  const [searchParams] = useSearchParams();

  // --- Metadata state ---
  const [catalogs, setCatalogs] = useState<{ name: string; status: string }[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<string[]>([]);
  const [dbLoading, setDbLoading] = useState(false);

  // --- Project state ---
  const [projects, setProjects] = useState<Project[]>([]);
  const [projectId, setProjectId] = useState<string>("");

  // --- Form state ---
  const [catalog, setCatalog] = useState("AwsDataCatalog");
  const [database, setDatabase] = useState("");
  const [bucket, setBucket] = useState("");
  const [graphName, setGraphName] = useState("");
  const [graphMemoryGb, setGraphMemoryGb] = useState(16);

  // --- Multi-query state ---
  const [nodeQueries, setNodeQueries] = useState<NodeQueryInput[]>([{ sql: "" }]);
  const [edgeQueries, setEdgeQueries] = useState<EdgeQueryInput[]>([{ sql: "" }]);

  // --- Projection state ---
  const [projectionsList, setProjectionsList] = useState<Projection[]>([]);
  const [currentId, setCurrentId] = useState<string | null>(null);
  const [status, setStatus] = useState<ProjectionStatus | null>(null);
  const [polling, setPolling] = useState(false);

  // --- Validation/Preview ---
  const [checks, setChecks] = useState<{ check: string; passed: boolean; message?: string }[]>([]);
  const [preview, setPreview] = useState<{ columns: string[]; rows: string[][] }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);

  // --- Load metadata ---
  useEffect(() => {
    metadata.catalogs().then((d) => setCatalogs(d.catalogs));
    metadata.buckets().then((d) => setBuckets(d.buckets));
    projectApi.list().then(setProjects);
    loadProjections().then(() => {
      const projectionId = searchParams.get("projection");
      if (projectionId) {
        projection.get(projectionId).then(loadProjection);
      }
    });
    const wsParam = searchParams.get("project");
    if (wsParam) setProjectId(wsParam);
  }, []);

  useEffect(() => {
    const wsParam = searchParams.get("project");
    const projectionId = searchParams.get("projection");
    if (wsParam) {
      setProjectId(wsParam);
      if (!projectionId) {
        resetForm();
      }
    }
    if (projectionId) {
      projection.get(projectionId).then(loadProjection);
    }
  }, [searchParams]);

  useEffect(() => {
    setDbLoading(true);
    metadata.databases(catalog).then((d) => { setDatabases(d.databases); setDbLoading(false); });
  }, [catalog]);

  function resetForm() {
    setCurrentId(null);
    setStatus(null);
    setPolling(false);
    setChecks([]);
    setPreview(null);
    setError(null);
    setCatalog("AwsDataCatalog");
    setDatabase("");
    setNodeQueries([{ sql: "" }]);
    setEdgeQueries([{ sql: "" }]);
    setBucket("");
    setGraphName("");
    setGraphMemoryGb(16);
  }

  async function loadProjections() {
    const list = await projection.list();
    setProjectionsList(list);
  }

  // --- Projection management ---

  // Auto-create projection once user starts filling the form
  useEffect(() => {
    if (currentId) return;
    const hasContent = database || bucket || graphName || nodeQueries.some(q => q.sql.trim()) || edgeQueries.some(q => q.sql.trim());
    if (!hasContent) return;
    projection.create({
      catalog,
      database,
      s3_staging_bucket: bucket,
      graph_name: graphName,
      graph_memory_gb: graphMemoryGb,
      project_id: projectId || undefined,
    }).then((p) => {
      setCurrentId(p.id);
      loadProjections();
      window.dispatchEvent(new Event("projects-changed"));
    });
  }, [database, bucket, graphName, nodeQueries, edgeQueries]);

  // Auto-create projection once user starts filling the form, then auto-save config on changes
  const configTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const hasContent = database || bucket || graphName || nodeQueries.some(q => q.sql.trim()) || edgeQueries.some(q => q.sql.trim());
    if (!hasContent) return;

    const data = {
      catalog,
      database,
      s3_staging_bucket: bucket,
      graph_name: graphName,
      graph_memory_gb: graphMemoryGb,
      project_id: projectId || undefined,
    };

    if (!currentId) {
      // First time — create
      projection.create(data).then((p) => {
        setCurrentId(p.id);
        loadProjections();
        window.dispatchEvent(new Event("projects-changed"));
      });
    } else {
      // Subsequent changes — debounced update
      if (configTimer.current) clearTimeout(configTimer.current);
      configTimer.current = setTimeout(() => {
        projection.update(currentId, data);
      }, 1000);
    }
  }, [catalog, database, bucket, graphName, graphMemoryGb, projectId]);

  async function ensureProjection(): Promise<string> {
    const data = {
      catalog,
      database,
      s3_staging_bucket: bucket,
      graph_name: graphName,
      graph_memory_gb: graphMemoryGb,
      project_id: projectId || undefined,
    };
    if (currentId) {
      await projection.update(currentId, data);
      await projection.saveQueries(currentId, { node_queries: nodeQueries, edge_queries: edgeQueries });
      return currentId;
    }
    const p = await projection.create(data);
    setCurrentId(p.id);
    await projection.saveQueries(p.id, { node_queries: nodeQueries, edge_queries: edgeQueries });
    await loadProjections();
    window.dispatchEvent(new Event("projects-changed"));
    return p.id;
  }

  function loadProjection(p: Projection) {
    setCurrentId(p.id);
    if (p.catalog) setCatalog(p.catalog);
    if (p.database) setDatabase(p.database);
    if (p.s3_staging_bucket) setBucket(p.s3_staging_bucket);
    if (p.graph_name) setGraphName(p.graph_name);
    if (p.graph_memory_gb) setGraphMemoryGb(p.graph_memory_gb);
    setChecks([]);
    setPreview(null);
    setError(null);

    // Load multi-queries
    projection.getQueries(p.id).then((res) => {
      if (res.node_queries.length > 0) setNodeQueries(res.node_queries.map((q) => ({ id: q.id, sql: q.sql })));
      else setNodeQueries([{ sql: "" }]);

      if (res.edge_queries.length > 0) setEdgeQueries(res.edge_queries.map((q) => ({ id: q.id, sql: q.sql })));
      else setEdgeQueries([{ sql: "" }]);
    });

    if (p.status === "executing") startPolling(p.id);
    else if (p.status === "complete") {
      setStatus({ id: p.id, status: "complete", progress: 100, graph_endpoint: p.graph_endpoint });
    } else {
      setStatus(null);
      setPolling(false);
    }
  }

  // --- Query management ---
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  function saveCurrentQueries() {
    if (!currentId) return;
    projection.saveQueries(currentId, { node_queries: nodeQueries, edge_queries: edgeQueries });
  }

  function scheduleSave(nq: NodeQueryInput[], eq: EdgeQueryInput[]) {
    if (!currentId) return;
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      projection.saveQueries(currentId, { node_queries: nq, edge_queries: eq });
    }, 1000);
  }

  function updateNodeQuery(index: number, sql: string) {
    const updated = nodeQueries.map((q, i) => (i === index ? { ...q, sql } : q));
    setNodeQueries(updated);
    scheduleSave(updated, edgeQueries);
  }
  function addNodeQuery() {
    setNodeQueries((prev) => [...prev, { sql: "" }]);
  }
  function removeNodeQuery(index: number) {
    const updated = nodeQueries.filter((_, i) => i !== index);
    setNodeQueries(updated);
    if (currentId) projection.saveQueries(currentId, { node_queries: updated, edge_queries: edgeQueries });
  }

  function updateEdgeQuery(index: number, updates: Partial<EdgeQueryInput>) {
    const updated = edgeQueries.map((q, i) => (i === index ? { ...q, ...updates } : q));
    setEdgeQueries(updated);
    scheduleSave(nodeQueries, updated);
  }
  function addEdgeQuery() {
    setEdgeQueries((prev) => [...prev, { sql: "" }]);
  }
  function removeEdgeQuery(index: number) {
    const updated = edgeQueries.filter((_, i) => i !== index);
    setEdgeQueries(updated);
    if (currentId) projection.saveQueries(currentId, { node_queries: nodeQueries, edge_queries: updated });
  }

  // --- Actions ---
  async function handleValidate() {
    setChecks([]);
    setPreview(null);
    setError(null);
    setLoading("validate");
    try {
      const id = await ensureProjection();
      const res = await projection.validate(id);
      setChecks(res.checks);
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  async function handleValidateQuery() {
    setChecks([]);
    setPreview(null);
    setError(null);
    setLoading("validate-query");
    try {
      const id = await ensureProjection();
      const res = await projection.validateQuery(id);
      setChecks(res.checks);
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  async function handlePreview() {
    setChecks([]);
    setPreview(null);
    setError(null);
    setLoading("preview");
    try {
      const id = await ensureProjection();
      const res = await projection.preview(id, 10);
      if (res.error) setError(res.error);
      else setPreview(res.results);
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  async function handleExecute() {
    setError(null);
    setLoading("execute");
    try {
      const id = await ensureProjection();
      await projection.execute(id);
      startPolling(id);
    } catch (e: any) {
      setError(e.message);
    } finally { setLoading(null); }
  }

  // --- Polling ---
  const startPolling = useCallback((id: string) => {
    setPolling(true);
    const interval = setInterval(async () => {
      const s = await projection.status(id);
      setStatus(s);
      if (s.status === "complete" || s.status === "failed") {
        clearInterval(interval);
        setPolling(false);
        if (s.error) setError(s.error);
      }
    }, 5000);
  }, []);

  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">Import</h1>
        <div className="flex items-center gap-2">
          <Select
            className="w-56"
            value={currentId || ""}
            onChange={(e) => {
              const id = e.target.value;
              if (!id) { setCurrentId(null); setStatus(null); setChecks([]); setPreview(null); return; }
              const s = projectionsList.find((s) => s.id === id);
              if (s) loadProjection(s);
            }}
          >
            <option value="">+ New projection</option>
            {projectionsList.map((s) => (
              <option key={s.id} value={s.id}>{s.graph_name || s.id.slice(0, 8)} ({s.status})</option>
            ))}
          </Select>
          <RefreshButton onClick={loadProjections} />
        </div>
      </div>

      <Card>
        <div className="space-y-4">
          <label className="block space-y-1">
              <span className="text-sm font-medium text-gray-700">Project</span>
              <Select
                className="flex-1"
                value={projectId}
                disabled
              >
                <option value="" disabled>Select a project</option>
                {projects.map((ws) => (
                  <option key={ws.id} value={ws.id}>{ws.name}</option>
                ))}
              </Select>
            </label>
          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Copy config from</span>
            <Select
              value=""
              onChange={(e) => {
                const s = projectionsList.find((s) => s.id === e.target.value);
                if (!s) return;
                if (s.catalog) setCatalog(s.catalog);
                if (s.database) setDatabase(s.database);
                if (s.s3_staging_bucket) setBucket(s.s3_staging_bucket);
                if (s.graph_memory_gb) setGraphMemoryGb(s.graph_memory_gb);
                // Load queries from source projection
                projection.getQueries(s.id).then((res) => {
                  if (res.node_queries.length > 0) setNodeQueries(res.node_queries.map((q) => ({ sql: q.sql })));
                  if (res.edge_queries.length > 0) setEdgeQueries(res.edge_queries.map((q) => ({ sql: q.sql })));
                });
              }}
            >
              <option value="">Select a projection...</option>
              {(projectId ? projectionsList.filter(s => s.project_id === projectId) : projectionsList).map((s) => (
                <option key={s.id} value={s.id}>{s.graph_name || s.id.slice(0, 8)}</option>
              ))}
            </Select>
          </label>
          <div className="grid grid-cols-2 gap-4">
            <label className="space-y-1">
              <span className="text-sm font-medium text-gray-700">Catalog</span>
              <Select value={catalog} onChange={(e) => setCatalog(e.target.value)}>
                {catalogs.map((c) => <option key={c.name} value={c.name}>{c.name}</option>)}
              </Select>
            </label>
            <label className="space-y-1">
              <span className="text-sm font-medium text-gray-700">Database</span>
              <Select value={database} onChange={(e) => setDatabase(e.target.value)} disabled={dbLoading}>
                <option value="">{dbLoading ? "Loading..." : "Select database..."}</option>
                {databases.map((db) => <option key={db} value={db}>{db}</option>)}
              </Select>
            </label>
          </div>
          <div className="grid grid-cols-3 gap-4">
            <label className="space-y-1">
              <span className="text-sm font-medium text-gray-700">S3 Staging Bucket</span>
              <Select value={bucket} onChange={(e) => setBucket(e.target.value)}>
                <option value="">Select bucket...</option>
                {buckets.map((b) => <option key={b} value={`s3://${b}/`}>s3://{b}/</option>)}
              </Select>
            </label>
            <label className="space-y-1">
              <span className="text-sm font-medium text-gray-700">Memory (m-NCU)</span>
              <Select value={graphMemoryGb} onChange={(e) => setGraphMemoryGb(Number(e.target.value))}>
                {[16, 32, 64, 128, 256, 512, 1024, 2048, 4096].map((size) => (
                  <option key={size} value={size}>{size} GB</option>
                ))}
              </Select>
            </label>
            <label className="space-y-1">
              <span className="text-sm font-medium text-gray-700">Graph Name</span>
              <input
                className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                placeholder="my-graph"
                value={graphName}
                onChange={(e) => setGraphName(e.target.value)}
              />
            </label>
          </div>
        </div>
      </Card>

      {/* Node Queries */}
      <Card>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold">Node Queries</h2>
            <button onClick={addNodeQuery} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800">
              <Plus className="h-3 w-3" /> Add
            </button>
          </div>
          <div className="max-h-72 space-y-3 overflow-y-auto pr-1">
            {nodeQueries.map((nq, i) => (
              <div key={i} className="rounded-md border border-gray-200 overflow-hidden">
                <div className="flex items-center justify-between bg-gray-50 px-3 py-1.5 border-b border-gray-200">
                  <span className="text-xs font-medium text-gray-700">Node {i + 1}</span>
                  {nodeQueries.length > 1 && (
                    <button onClick={() => removeNodeQuery(i)} className="text-gray-400 hover:text-red-600">
                      <Trash2 className="h-3 w-3" />
                    </button>
                  )}
                </div>
                <textarea
                  className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-none"
                  rows={3}
                  placeholder="SELECT id AS &quot;~id&quot;, 'Label' AS &quot;~label&quot;, col1 FROM table"
                  value={nq.sql}
                  onChange={(e) => updateNodeQuery(i, e.target.value)}
                  onBlur={() => saveCurrentQueries()}
                />
              </div>
            ))}
          </div>
        </div>
      </Card>

      {/* Edge Queries */}
      <Card>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold">Edge Queries</h2>
            <button onClick={addEdgeQuery} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800">
              <Plus className="h-3 w-3" /> Add
            </button>
          </div>
          <div className="max-h-72 space-y-3 overflow-y-auto pr-1">
            {edgeQueries.map((eq, i) => (
              <div key={i} className="rounded-md border border-gray-200 overflow-hidden">
                <div className="flex items-center justify-between bg-gray-50 px-3 py-1.5 border-b border-gray-200">
                  <span className="text-xs font-medium text-gray-700">Edge {i + 1}</span>
                  {edgeQueries.length > 1 && (
                    <button onClick={() => removeEdgeQuery(i)} className="text-gray-400 hover:text-red-600">
                      <Trash2 className="h-3 w-3" />
                    </button>
                  )}
                </div>
                <textarea
                  className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-none"
                  rows={3}
                  placeholder="SELECT id AS &quot;~id&quot;, src AS &quot;~from&quot;, dst AS &quot;~to&quot;, 'Label' AS &quot;~label&quot; FROM table"
                  value={eq.sql}
                  onChange={(e) => updateEdgeQuery(i, { sql: e.target.value })}
                  onBlur={() => saveCurrentQueries()}
                />
              </div>
            ))}
          </div>
        </div>
      </Card>

      {/* Actions */}
      <div className="flex gap-2">
        <Button variant="secondary" onClick={handleValidate} disabled={!!loading}><CheckCircle className="h-4 w-4" /> {loading === "validate" ? "Validating..." : "Validate Resources"}</Button>
        <Button variant="secondary" onClick={handleValidateQuery} disabled={!!loading}><CheckCircle className="h-4 w-4" /> {loading === "validate-query" ? "Validating..." : "Validate Query"}</Button>
        <Button variant="secondary" onClick={handlePreview} disabled={!!loading}><Eye className="h-4 w-4" /> {loading === "preview" ? "Loading..." : "Preview"}</Button>
        <Button onClick={handleExecute} disabled={polling || !!loading}><Play className="h-4 w-4" /> Execute</Button>
      </div>

      {/* Validation checks */}
      {checks.length > 0 && (
        <Card>
          <h2 className="mb-2 text-sm font-medium">Validation</h2>
          <ul className="space-y-1">
            {checks.map((c, i) => (
              <li key={i} className="flex items-center gap-2 text-sm">
                <span className={c.passed ? "text-green-600" : "text-red-600"}>{c.passed ? "✓" : "✗"}</span>
                <span className="font-medium">{c.check}</span>
                {c.message && <span className="text-gray-500">— {c.message}</span>}
              </li>
            ))}
          </ul>
        </Card>
      )}

      {/* Preview */}
      {preview && (
        <div className="space-y-4">
          {preview.map((result, i) => {
            const activeNodeCount = nodeQueries.filter(q => q.sql.trim()).length;
            const isNode = i < activeNodeCount;
            return (
              <Card key={i}>
                <h2 className="mb-2 text-sm font-medium">{isNode ? `Node ${i + 1}` : `Edge ${i - activeNodeCount + 1}`} Preview</h2>
                <div className="overflow-auto">
                  <table className="w-full text-left text-sm">
                    <thead className="border-b bg-gray-50">
                      <tr>{result.columns.map((col) => <th key={col} className="px-3 py-2 font-medium">{col}</th>)}</tr>
                    </thead>
                    <tbody>
                      {result.rows.map((row, ri) => (
                        <tr key={ri} className="border-b last:border-0">
                          {row.map((cell, ci) => <td key={ci} className="px-3 py-2">{cell}</td>)}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </Card>
            );
          })}
        </div>
      )}

      {/* Progress */}
      {status && (
        <Card>
          {status.status === "complete" ? (
            <div className="space-y-2">
              <p className="text-sm font-medium text-green-700">✓ Graph ready</p>
              {status.graph_endpoint && <p className="text-sm text-gray-600">Endpoint: <code className="rounded bg-gray-100 px-1">{status.graph_endpoint}</code></p>}
            </div>
          ) : (
            <div className="space-y-2">
              <ProgressBar value={status.progress} label={status.step_label || status.step || "Running..."} />
            </div>
          )}
        </Card>
      )}

      {/* Error */}
      {error && (
        <div className="rounded-md border border-red-200 bg-red-50 p-4 text-sm text-red-700">{error}</div>
      )}
    </div>
  );
}
