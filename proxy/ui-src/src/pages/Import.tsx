import { useEffect, useState, useCallback } from "react";
import { metadata, projection, type Projection, type ProjectionStatus } from "../api";
import { Button, Select, ProgressBar, Card } from "../components/ui";
import { Play, CheckCircle, Eye, RefreshCw } from "lucide-react";

export function Import() {
  // --- Metadata state ---
  const [catalogs, setCatalogs] = useState<{ name: string; status: string }[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<string[]>([]);
  const [dbLoading, setDbLoading] = useState(false);

  // --- Form state ---
  const [catalog, setCatalog] = useState("AwsDataCatalog");
  const [database, setDatabase] = useState("");
  const [sqlQuery, setSqlQuery] = useState("");
  const [bucket, setBucket] = useState("");
  const [graphName, setGraphName] = useState("");
  const [graphMemoryGb, setGraphMemoryGb] = useState(16);

  // --- Session state ---
  const [sessions, setSessions] = useState<Projection[]>([]);
  const [currentId, setCurrentId] = useState<string | null>(null);
  const [status, setStatus] = useState<ProjectionStatus | null>(null);
  const [polling, setPolling] = useState(false);

  // --- Validation/Preview ---
  const [checks, setChecks] = useState<{ check: string; passed: boolean; message?: string }[]>([]);
  const [preview, setPreview] = useState<{ columns: string[]; rows: string[][] }[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  // --- Load metadata ---
  useEffect(() => {
    metadata.catalogs().then((d) => setCatalogs(d.catalogs));
    metadata.buckets().then((d) => setBuckets(d.buckets));
    loadSessions();
  }, []);

  useEffect(() => {
    setDbLoading(true);
    metadata.databases(catalog).then((d) => { setDatabases(d.databases); setDbLoading(false); });
  }, [catalog]);

  async function loadSessions() {
    const list = await projection.list();
    setSessions(list);
  }

  // --- Session management ---
  async function ensureSession(): Promise<string> {
    const data = { catalog, database, sql_query: sqlQuery, s3_staging_bucket: bucket, graph_name: graphName, graph_memory_gb: graphMemoryGb };
    if (currentId) {
      await projection.update(currentId, data);
      return currentId;
    }
    const p = await projection.create(data);
    setCurrentId(p.id);
    await loadSessions();
    return p.id;
  }

  function loadSession(p: Projection) {
    setCurrentId(p.id);
    if (p.catalog) setCatalog(p.catalog);
    if (p.database) setDatabase(p.database);
    if (p.sql_query) setSqlQuery(p.sql_query);
    if (p.s3_staging_bucket) setBucket(p.s3_staging_bucket);
    if (p.graph_name) setGraphName(p.graph_name);
    if (p.graph_memory_gb) setGraphMemoryGb(p.graph_memory_gb);
    setChecks([]);
    setPreview(null);
    setError(null);

    if (p.status === "executing") startPolling(p.id);
    else if (p.status === "complete") {
      setStatus({ id: p.id, status: "complete", progress: 100, graph_endpoint: p.graph_endpoint });
    } else {
      setStatus(null);
      setPolling(false);
    }
  }

  // --- Actions ---
  async function handleValidate() {
    setError(null);
    const id = await ensureSession();
    const res = await projection.validate(id);
    setChecks(res.checks);
  }

  async function handleValidateQuery() {
    setError(null);
    const id = await ensureSession();
    const res = await projection.validateQuery(id);
    setChecks(res.checks);
  }

  async function handlePreview() {
    setError(null);
    const id = await ensureSession();
    const res = await projection.preview(id);
    if (res.error) setError(res.error);
    else setPreview(res.results);
  }

  async function handleExecute() {
    setError(null);
    const id = await ensureSession();
    await projection.execute(id);
    startPolling(id);
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
              const s = sessions.find((s) => s.id === id);
              if (s) loadSession(s);
            }}
          >
            <option value="">+ New session</option>
            {sessions.map((s) => (
              <option key={s.id} value={s.id}>{s.graph_name || s.id.slice(0, 8)} ({s.status})</option>
            ))}
          </Select>
          <Button variant="ghost" onClick={loadSessions} title="Refresh sessions">
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <Card>
        <div className="space-y-4">
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

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">SQL Query</span>
            <textarea
              className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              rows={4}
              placeholder="SELECT ~id, ~label, col1, col2 FROM my_table"
              value={sqlQuery}
              onChange={(e) => setSqlQuery(e.target.value)}
            />
          </label>

          <div className="grid grid-cols-2 gap-4">
            <label className="space-y-1">
              <span className="text-sm font-medium text-gray-700">S3 Staging Bucket</span>
              <Select value={bucket} onChange={(e) => setBucket(e.target.value)}>
                <option value="">Select bucket...</option>
                {buckets.map((b) => <option key={b} value={`s3://${b}/`}>s3://{b}/</option>)}
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

          <label className="block space-y-1">
            <span className="text-sm font-medium text-gray-700">Graph Memory (GB)</span>
            <input
              type="number"
              className="w-32 rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
              value={graphMemoryGb}
              onChange={(e) => setGraphMemoryGb(Number(e.target.value))}
              min={16}
              step={16}
            />
          </label>
        </div>
      </Card>

      {/* Actions */}
      <div className="flex gap-2">
        <Button variant="secondary" onClick={handleValidate}><CheckCircle className="h-4 w-4" /> Validate</Button>
        <Button variant="secondary" onClick={handleValidateQuery}><CheckCircle className="h-4 w-4" /> Validate Query</Button>
        <Button variant="secondary" onClick={handlePreview}><Eye className="h-4 w-4" /> Preview</Button>
        <Button onClick={handleExecute} disabled={polling}><Play className="h-4 w-4" /> Execute</Button>
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
        <Card>
          <h2 className="mb-2 text-sm font-medium">Preview</h2>
          {preview.map((result, i) => (
            <div key={i} className="overflow-auto">
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
          ))}
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
  );
}
