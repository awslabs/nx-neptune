import { useEffect, useState, useCallback, useMemo } from "react";
import { useSearchParams } from "react-router";
import { metadata, projection, projectApi, type Projection, type ProjectionStatus, type Project, type NodeQueryInput, type EdgeQueryInput } from "../api";
import { Button, Select, ProgressBar, Card, RefreshButton } from "../components/ui";
import { Play, CheckCircle, Eye, Network, Plus, Trash2 } from "lucide-react";
import { SchemaPreview, type SchemaNode, type SchemaEdge } from "../components/SchemaPreview";

function extractLabel(sql: string): string | null {
  // Matches: 'Label' AS "~label", 'Label' AS `~label`, 'Label' AS [~label], 'Label' AS ~label
  // Also handles: 'Label' "~label" (no AS keyword), multi-word labels, underscores, numbers
  const match = sql.match(/'([^']+)'\s+(?:AS\s+)?(?:"|`|\[)?~label(?:"|`|\])?/i);
  return match?.[1]?.trim() ?? null;
}

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
  const [edgeQueries, setEdgeQueries] = useState<EdgeQueryInput[]>([{ sql: "", from_type: "", to_type: "" }]);

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

  // --- Schema Preview ---
  const [rightPaneOpen, setRightPaneOpen] = useState(false);
  const [rightPaneTab, setRightPaneTab] = useState<"schema" | "data">("schema");
  const [dataTab, setDataTab] = useState(0);

  // Derived: node types from current queries
  const nodeTypes = useMemo(
    () => nodeQueries.map((q) => extractLabel(q.sql)).filter((l): l is string => l !== null),
    [nodeQueries],
  );

  // Derived: schema graph from local state
  const schemaNodes: SchemaNode[] = useMemo(
    () => nodeTypes.map((label) => ({ id: label, label })),
    [nodeTypes],
  );
  const schemaEdges: SchemaEdge[] = useMemo(
    () =>
      edgeQueries
        .map((eq, i) => {
          const label = extractLabel(eq.sql);
          if (!label || !eq.from_type || !eq.to_type) return null;
          return { id: `e${i}`, source: eq.from_type, target: eq.to_type, label };
        })
        .filter((e): e is SchemaEdge => e !== null),
    [edgeQueries],
  );

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
    setBucket("");
    setGraphName("");
    setGraphMemoryGb(16);
    setNodeQueries([{ sql: "" }]);
    setEdgeQueries([{ sql: "", from_type: "", to_type: "" }]);
    setRightPaneOpen(false);
  }

  async function loadProjections() {
    const list = await projection.list();
    setProjectionsList(list);
  }

  // --- Projection management ---
  async function ensureProjection(): Promise<string> {
    const data = { catalog, database, graph_name: graphName, graph_memory_gb: graphMemoryGb, s3_staging_bucket: bucket, project_id: projectId || undefined };
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

    // Load queries
    projection.getQueries(p.id).then((res) => {
      setNodeQueries(res.node_queries.length > 0 ? res.node_queries.map((q) => ({ id: q.id, sql: q.sql })) : [{ sql: "" }]);
      setEdgeQueries(res.edge_queries.length > 0 ? res.edge_queries.map((q) => ({ id: q.id, sql: q.sql, from_type: q.from_type ?? "", to_type: q.to_type ?? "" })) : [{ sql: "", from_type: "", to_type: "" }]);
    });

    if (p.status === "executing") startPolling(p.id);
    else if (p.status === "complete") {
      setStatus({ id: p.id, status: "complete", progress: 100, graph_endpoint: p.graph_endpoint });
    } else {
      setStatus(null);
      setPolling(false);
    }
  }

  // --- Node query handlers ---
  function updateNodeQuery(index: number, sql: string) {
    setNodeQueries((prev) => prev.map((q, i) => (i === index ? { ...q, sql } : q)));
  }
  function addNodeQuery() {
    setNodeQueries((prev) => [...prev, { sql: "" }]);
  }
  function removeNodeQuery(index: number) {
    setNodeQueries((prev) => prev.filter((_, i) => i !== index));
  }

  // --- Edge query handlers ---
  function updateEdgeQuery(index: number, updates: Partial<EdgeQueryInput>) {
    setEdgeQueries((prev) => prev.map((q, i) => (i === index ? { ...q, ...updates } : q)));
  }
  function addEdgeQuery() {
    setEdgeQueries((prev) => [...prev, { sql: "", from_type: "", to_type: "" }]);
  }
  function removeEdgeQuery(index: number) {
    setEdgeQueries((prev) => prev.filter((_, i) => i !== index));
  }

  // --- Actions ---
  async function handleValidate() {
    setChecks([]); setPreview(null); setError(null); setLoading("validate");
    try {
      const id = await ensureProjection();
      const res = await projection.validate(id);
      setChecks(res.checks);
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  async function handleValidateQuery() {
    setChecks([]); setPreview(null); setError(null); setLoading("validate-query");
    try {
      const id = await ensureProjection();
      const res = await projection.validateQuery(id);
      setChecks(res.checks);
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  async function handlePreview() {
    setChecks([]); setPreview(null); setError(null); setLoading("preview");
    try {
      const id = await ensureProjection();
      const res = await projection.preview(id);
      if (res.error) setError(res.error);
      else {
        setPreview(res.results);
        setDataTab(0);
        setRightPaneOpen(true);
        setRightPaneTab("data");
      }
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  async function handleExecute() {
    setError(null); setLoading("execute");
    try {
      const id = await ensureProjection();
      await projection.execute(id);
      startPolling(id);
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  function handleSchemaPreview() {
    setRightPaneOpen(true);
    setRightPaneTab("schema");
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
    <div className="flex h-full">
      {/* Left pane: form */}
      <div className={`overflow-auto p-6 ${rightPaneOpen ? "w-1/2 border-r border-gray-200" : "w-full max-w-4xl mx-auto"}`}>
      <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-semibold">Import</h1>
        <div className="flex items-center gap-2">
          <Select
            className="w-56"
            value={currentId || ""}
            onChange={(e) => {
              const id = e.target.value;
              if (!id) { resetForm(); return; }
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

      {/* Config */}
      <Card>
        <div className="space-y-4">
          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Project</span>
            <div className="flex gap-2">
              <Select className="flex-1" value={projectId} onChange={(e) => setProjectId(e.target.value)}>
                <option value="">No project</option>
                {projects.map((ws) => <option key={ws.id} value={ws.id}>{ws.name}</option>)}
              </Select>
              <Button variant="secondary" onClick={async () => {
                const name = prompt("Project name:");
                if (!name) return;
                const ws = await projectApi.create(name);
                setProjects((prev) => [...prev, ws]);
                setProjectId(ws.id);
              }}>+</Button>
            </div>
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
                // Load queries from the selected projection
                projection.getQueries(s.id).then((res) => {
                  if (res.node_queries.length > 0) setNodeQueries(res.node_queries.map((q) => ({ id: q.id, sql: q.sql })));
                  if (res.edge_queries.length > 0) setEdgeQueries(res.edge_queries.map((q) => ({ id: q.id, sql: q.sql, from_type: q.from_type ?? "", to_type: q.to_type ?? "" })));
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
          {nodeQueries.map((nq, i) => {
            const label = extractLabel(nq.sql);
            return (
              <div key={i} className="rounded-md border border-gray-200 overflow-hidden">
                <div className="flex items-center justify-between bg-gray-50 px-3 py-1.5 border-b border-gray-200">
                  <span className="text-xs font-medium text-gray-700">
                    {label ?? <span className="italic text-gray-400">No ~label detected</span>}
                  </span>
                  {nodeQueries.length > 1 && (
                    <button onClick={() => removeNodeQuery(i)} className="text-gray-400 hover:text-red-600">
                      <Trash2 className="h-3 w-3" />
                    </button>
                  )}
                </div>
                <textarea
                  className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-none"
                  rows={2}
                  placeholder="SELECT id AS ~id, 'TypeName' AS ~label, col1 FROM table"
                  value={nq.sql}
                  onChange={(e) => updateNodeQuery(i, e.target.value)}
                />
              </div>
            );
          })}
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
          {edgeQueries.map((eq, i) => {
            const label = extractLabel(eq.sql);
            const fromInvalid = eq.from_type && !nodeTypes.includes(eq.from_type);
            const toInvalid = eq.to_type && !nodeTypes.includes(eq.to_type);
            return (
              <div key={i} className="rounded-md border border-gray-200 overflow-hidden">
                <div className="flex items-center justify-between bg-gray-50 px-3 py-1.5 border-b border-gray-200">
                  <span className="text-xs font-medium text-gray-700">
                    {label ?? <span className="italic text-gray-400">No ~label detected</span>}
                  </span>
                  {edgeQueries.length > 1 && (
                    <button onClick={() => removeEdgeQuery(i)} className="text-gray-400 hover:text-red-600">
                      <Trash2 className="h-3 w-3" />
                    </button>
                  )}
                </div>
                <textarea
                  className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-none"
                  rows={2}
                  placeholder="SELECT id AS ~id, src AS ~from, dst AS ~to, 'EdgeType' AS ~label FROM table"
                  value={eq.sql}
                  onChange={(e) => updateEdgeQuery(i, { sql: e.target.value })}
                />
                <div className="flex gap-4 px-3 py-2 bg-gray-50 border-t border-gray-200">
                  <label className="flex items-center gap-2 text-xs">
                    <span className={fromInvalid ? "text-red-500 font-medium" : "text-gray-600"}>From:</span>
                    <select
                      className={`rounded border px-2 py-1 text-xs ${fromInvalid ? "border-red-300 bg-red-50" : "border-gray-300"}`}
                      value={eq.from_type ?? ""}
                      onChange={(e) => updateEdgeQuery(i, { from_type: e.target.value })}
                    >
                      <option value="">Select node type...</option>
                      {nodeTypes.map((t) => <option key={t} value={t}>{t}</option>)}
                    </select>
                    {fromInvalid && <span className="text-red-500">⚠</span>}
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <span className={toInvalid ? "text-red-500 font-medium" : "text-gray-600"}>To:</span>
                    <select
                      className={`rounded border px-2 py-1 text-xs ${toInvalid ? "border-red-300 bg-red-50" : "border-gray-300"}`}
                      value={eq.to_type ?? ""}
                      onChange={(e) => updateEdgeQuery(i, { to_type: e.target.value })}
                    >
                      <option value="">Select node type...</option>
                      {nodeTypes.map((t) => <option key={t} value={t}>{t}</option>)}
                    </select>
                    {toInvalid && <span className="text-red-500">⚠</span>}
                  </label>
                </div>
              </div>
            );
          })}
          {nodeTypes.length === 0 && (
            <p className="text-xs text-gray-400 italic">Define node queries with ~label to populate the dropdowns</p>
          )}
        </div>
      </Card>

      {/* Actions */}
      <div className="flex flex-wrap gap-2">
        <Button variant="secondary" onClick={handleValidate} disabled={!!loading}><CheckCircle className="h-4 w-4" /> {loading === "validate" ? "Validating..." : "Validate Resources"}</Button>
        <Button variant="secondary" onClick={handleValidateQuery} disabled={!!loading}><CheckCircle className="h-4 w-4" /> {loading === "validate-query" ? "Validating..." : "Validate Query"}</Button>
        <Button variant="secondary" onClick={handlePreview} disabled={!!loading}><Eye className="h-4 w-4" /> {loading === "preview" ? "Loading..." : "Preview Data"}</Button>
        <Button variant="secondary" onClick={handleSchemaPreview} disabled={schemaNodes.length === 0}><Network className="h-4 w-4" /> Preview Schema</Button>
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
      </div>

      {/* Right pane: Schema + Data tabs */}
      {rightPaneOpen && (
        <div className="w-1/2 flex flex-col">
          <div className="flex items-center justify-between border-b border-gray-200 px-4 py-2">
            <div className="flex gap-4">
              <button
                onClick={() => setRightPaneTab("schema")}
                className={`pb-1 text-sm ${rightPaneTab === "schema" ? "border-b-2 border-blue-600 text-blue-600 font-medium" : "text-gray-500 hover:text-gray-700"}`}
              >
                Schema
              </button>
              <button
                onClick={() => setRightPaneTab("data")}
                className={`pb-1 text-sm ${rightPaneTab === "data" ? "border-b-2 border-blue-600 text-blue-600 font-medium" : "text-gray-500 hover:text-gray-700"}`}
              >
                Data
              </button>
            </div>
            <button
              onClick={() => setRightPaneOpen(false)}
              className="text-xs text-gray-400 hover:text-gray-600"
            >
              ✕ Close
            </button>
          </div>

          {/* Schema tab */}
          {rightPaneTab === "schema" && (
            <div className="flex-1 flex flex-col">
              <div className="flex items-center justify-between px-4 py-2 border-b border-gray-100">
                <span className="text-xs text-gray-400">
                  {schemaNodes.length} node type{schemaNodes.length !== 1 ? "s" : ""} · {schemaEdges.length} edge type{schemaEdges.length !== 1 ? "s" : ""}
                </span>
              </div>
              <div className="flex-1">
                <SchemaPreview nodes={schemaNodes} edges={schemaEdges} />
              </div>
            </div>
          )}

          {/* Data tab */}
          {rightPaneTab === "data" && (
            <div className="flex-1 flex flex-col overflow-hidden">
              {preview ? (
                <>
                  {/* Sub-tabs for each query result */}
                  <div className="flex gap-1 px-4 py-2 border-b border-gray-100 overflow-x-auto">
                    {preview.map((_, i) => {
                      const activeNodeCount = nodeQueries.filter(q => q.sql.trim()).length;
                      const isNode = i < activeNodeCount;
                      const label = isNode
                        ? extractLabel(nodeQueries[i]?.sql ?? "") ?? `Node ${i + 1}`
                        : extractLabel(edgeQueries[i - activeNodeCount]?.sql ?? "") ?? `Edge ${i - activeNodeCount + 1}`;
                      const activeClass = isNode ? "bg-blue-100 text-blue-700" : "bg-purple-100 text-purple-700";
                      return (
                        <button
                          key={i}
                          onClick={() => setDataTab(i)}
                          className={`px-3 py-1 text-xs rounded-md whitespace-nowrap flex items-center gap-1.5 ${dataTab === i ? `${activeClass} font-medium` : "text-gray-500 hover:bg-gray-100"}`}
                        >
                          <span className={`h-2 w-2 rounded-full ${isNode ? "bg-blue-400" : "bg-purple-400"}`}></span>
                          {label}
                        </button>
                      );
                    })}
                  </div>
                  {/* Table for selected tab */}
                  <div className="flex-1 overflow-auto p-4">
                    {preview[dataTab] && (
                      <table className="w-full text-left text-sm">
                        <thead className="border-b bg-gray-50 sticky top-0">
                          <tr>{preview[dataTab].columns.map((col) => <th key={col} className="px-3 py-2 font-medium">{col}</th>)}</tr>
                        </thead>
                        <tbody>
                          {preview[dataTab].rows.map((row, ri) => (
                            <tr key={ri} className="border-b last:border-0">
                              {row.map((cell, ci) => <td key={ci} className="px-3 py-2">{cell}</td>)}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </>
              ) : (
                <div className="flex-1 flex items-center justify-center text-sm text-gray-400">
                  Click "Preview Data" to see query results
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
