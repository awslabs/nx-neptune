import { useEffect, useState, useCallback, useRef } from "react";
import { useSearchParams, useNavigate } from "react-router";
import { metadata, projection, projectApi, type Projection, type ProjectionStatus, type Project, type NodeQueryInput, type EdgeQueryInput } from "../api";
import { Button, Select, ProgressBar, Card, RefreshButton } from "../components/ui";
import { usePageBridge, type ImportPersistData } from "../assistant/context";
import { Play, CheckCircle, Eye, Wand2, ChevronRight, Plus, Trash2, AlertTriangle } from "lucide-react";

type UiNodeQuery = NodeQueryInput & { failed?: boolean };
type UiEdgeQuery = EdgeQueryInput & { failed?: boolean };

const stripUi = <T extends { failed?: boolean }>(qs: T[]) => qs.map(({ failed, ...q }) => q);

function persistQueries(id: string, nq: UiNodeQuery[], eq: UiEdgeQuery[]) {
  return projection.saveQueries(id, { node_queries: stripUi(nq), edge_queries: stripUi(eq) });
}

const NA_GRAPH_NAME_MAX_LENGTH = 63;
const SESSION_SUFFIX_LENGTH = 37;
const GRAPH_NAME_MIN_LENGTH = 3;
const GRAPH_NAME_CHARS = /^[a-z0-9]+(-[a-z0-9]+)*$/;

function graphNameMaxLength(prefix: string | null): number | null {
  if (prefix === null) return null;
  return NA_GRAPH_NAME_MAX_LENGTH - prefix.length - SESSION_SUFFIX_LENGTH;
}

function graphNameError(name: string, prefix: string | null): string | null {
  const max = graphNameMaxLength(prefix);
  if (max !== null && max < GRAPH_NAME_MIN_LENGTH) {
    return `The graph name prefix "${prefix}" is too long to leave room for a name. Shorten GRAPH_PREFIX.`;
  }
  if (!name) return null;
  if (max !== null && name.length > max) {
    return `The graph name is too long.`;
  }
  if (name.length < GRAPH_NAME_MIN_LENGTH) {
    return `Graph name must be at least ${GRAPH_NAME_MIN_LENGTH} characters.`;
  }
  if (!GRAPH_NAME_CHARS.test(name)) {
    return "Use lowercase letters, digits and single hyphens, without a hyphen at the start or end.";
  }
  return null;
}

export function Import() {
  const [searchParams] = useSearchParams();
  const navigate = useNavigate();

  // --- Metadata state ---
  const [catalogs, setCatalogs] = useState<{ name: string; status: string }[]>([]);
  const [databases, setDatabases] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<string[]>([]);
  const [dbLoading, setDbLoading] = useState(false);
  const [dbError, setDbError] = useState<string | null>(null);

  // --- Project state ---
  const [projects, setProjects] = useState<Project[]>([]);
  const [projectId, setProjectId] = useState<string>("");

  // --- Form state ---
  const [catalog, setCatalog] = useState("AwsDataCatalog");
  const [database, setDatabase] = useState("");
  const [propertyGraph, setPropertyGraph] = useState("");
  const [pgqOpen, setPgqOpen] = useState(true);
  const [nodeEdgeOpen, setNodeEdgeOpen] = useState(false);
  const [translateValidated, setTranslateValidated] = useState(false);
  const [translateError, setTranslateError] = useState<string | null>(null);
  const [bucket, setBucket] = useState("");
  const [graphName, setGraphName] = useState("");
  const [graphPrefix, setGraphPrefix] = useState<string | null>(null);
  const [graphMemoryGb, setGraphMemoryGb] = useState(16);

  // --- Multi-query state ---
  const [nodeQueries, setNodeQueries] = useState<UiNodeQuery[]>([{ sql: "" }]);
  const [edgeQueries, setEdgeQueries] = useState<UiEdgeQuery[]>([{ sql: "" }]);

  // --- Post-import openCypher graph queries ---
  // Authored here but never run on this page — they carry over to the read-only
  // /details page (via navigation state) where the live graph can execute them.
  const [graphQueries, setGraphQueries] = useState<{ cypher: string }[]>([{ cypher: "" }]);

  // --- Projection state ---
  const [projectionsList, setProjectionsList] = useState<Projection[]>([]);
  const [currentId, setCurrentId] = useState<string | null>(null);
  const [graphId, setGraphId] = useState<string | null>(null);
  const [status, setStatus] = useState<ProjectionStatus | null>(null);
  const [polling, setPolling] = useState(false);

  // --- Validation/Preview ---
  const [checks, setChecks] = useState<{ check: string; passed: boolean; message?: string }[]>([]);
  const [checksSource, setChecksSource] = useState<"resources" | "queries" | null>(null);
  const [preview, setPreview] = useState<{ columns: string[]; rows: string[][] }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState<string | null>(null);

  const graphNameMax = graphNameMaxLength(graphPrefix);
  const graphNameErrorText = graphNameError(graphName, graphPrefix);

  // --- Load metadata ---
  useEffect(() => {
    metadata.catalogs().then((d) => setCatalogs(d.catalogs));
    metadata.buckets().then((d) => setBuckets(d.buckets)).catch((e) => setError(e.message));
    metadata.config().then((c) => setGraphPrefix(c.graph_prefix)).catch(() => {});
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
    // Ignore a response for a catalog that is no longer selected, so a slow
    // (e.g. federated) catalog can't overwrite the current list.
    let stale = false;
    setDbLoading(true);
    setDbError(null);
    metadata.databases(catalog)
      .then((d) => { if (!stale) setDatabases(d.databases); })
      .catch((e) => {
        if (stale) return;
        setDatabases([]);
        setDbError(`Could not load databases for ${catalog}: ${e.message}`);
      })
      .finally(() => { if (!stale) setDbLoading(false); });
    return () => { stale = true; };
  }, [catalog]);

  function resetForm() {
    setCurrentId(null);
    setGraphId(null);
    setStatus(null);
    setPolling(false);
    setChecks([]);
    setTranslateError(null);
    setPreview(null);
    setError(null);
    setCatalog("AwsDataCatalog");
    setDatabase("");
    setNodeQueries([{ sql: "" }]);
    setEdgeQueries([{ sql: "" }]);
    setGraphQueries([{ cypher: "" }]);
    setBucket("");
    setGraphName("");
    setGraphMemoryGb(16);
  }

  async function loadProjections() {
    const list = await projection.list();
    setProjectionsList(list);
  }

  // --- Projection management ---

  // Guards projection creation so the several triggers that can fire in the
  // same tick — the two auto-create effects below, an assistant apply, and a
  // manual Validate/Execute — create exactly one row instead of racing (setId
  // is async, so a plain `!currentId` check is not enough).
  const creatingRef = useRef(false);

  async function createProjectionOnce(
    data: Record<string, unknown>,
  ): Promise<string | null> {
    if (currentId || creatingRef.current) return currentId;
    creatingRef.current = true;
    try {
      const p = await projection.create(data);
      setCurrentId(p.id);
      // Flush any graph queries typed before the projection existed — the
      // debounced save skips while there is no id, so persist them now.
      if (graphQueries.some((q) => q.cypher.trim())) {
        await projection
          .saveGraphQueries(p.id, graphQueries.map((q) => ({ cypher: q.cypher })))
          .catch(() => {});
      }
      await loadProjections();
      window.dispatchEvent(new Event("projects-changed"));
      return p.id;
    } finally {
      creatingRef.current = false;
    }
  }

  function configData() {
    return {
      catalog,
      database,
      property_graph: propertyGraph || undefined,
      s3_staging_bucket: bucket,
      graph_name: graphName || undefined,
      graph_memory_gb: graphMemoryGb,
      project_id: projectId || undefined,
    };
  }

  // Auto-create projection once user starts filling the form
  useEffect(() => {
    if (currentId) return;
    const hasContent = database || bucket || graphName || nodeQueries.some(q => q.sql.trim()) || edgeQueries.some(q => q.sql.trim()) || graphQueries.some(q => q.cypher.trim());
    if (!hasContent) return;
    createProjectionOnce(configData());
  }, [database, bucket, graphName, nodeQueries, edgeQueries, graphQueries]);

  // Auto-create projection once user starts filling the form, then auto-save config on changes
  const configTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    const hasContent = database || bucket || graphName || propertyGraph.trim() || nodeQueries.some(q => q.sql.trim()) || edgeQueries.some(q => q.sql.trim());
    if (!hasContent) return;

    if (!currentId) {
      // First time — create (deduped against the effect above)
      createProjectionOnce(configData());
    } else {
      // Subsequent changes — debounced update
      pendingSaveRef.current.configDirty = true;
      if (configTimer.current) clearTimeout(configTimer.current);
      configTimer.current = setTimeout(() => {
        pendingSaveRef.current.configDirty = false;
        projection.update(currentId, configData());
      }, 1000);
    }
  }, [catalog, database, propertyGraph, bucket, graphName, graphMemoryGb, projectId]);

  async function ensureProjection(): Promise<string> {
    if (!projectId) {
      throw new Error("No project selected. Open Import from a project to continue.");
    }
    const data = configData();
    if (currentId) {
      await projection.update(currentId, data);
      await persistQueries(currentId, nodeQueries, edgeQueries);
      return currentId;
    }
    const id = await createProjectionOnce(data);
    if (!id) throw new Error("Could not create the projection");
    await persistQueries(id, nodeQueries, edgeQueries);
    return id;
  }

  // Persist the import the assistant just applied. Called from the page bridge
  // right after the proposal fills the form, with the applied values passed in
  // directly (state has not re-rendered yet). Creates the projection if none
  // exists — the whole point of this hook — then saves any node/edge queries,
  // which the raw setters do not persist on their own.
  async function persistImport(data: ImportPersistData): Promise<void> {
    const config: Record<string, unknown> = {
      graph_memory_gb: graphMemoryGb,
      project_id: projectId || undefined,
    };
    if (data.catalog != null) config.catalog = data.catalog;
    if (data.database != null) config.database = data.database;
    if (data.bucket != null) config.s3_staging_bucket = data.bucket;
    if (data.graphName != null) config.graph_name = data.graphName;

    let id = currentId;
    if (!id) {
      id = await createProjectionOnce({ catalog: data.catalog ?? catalog, ...config });
      if (!id) return; // a create is already in flight; it will carry the fields
    } else {
      await projection.update(id, config);
    }

    if (data.nodeQueries || data.edgeQueries) {
      await projection.saveQueries(id, {
        node_queries: (data.nodeQueries ?? nodeQueries).map((q) => ({ sql: q.sql })),
        edge_queries: (data.edgeQueries ?? edgeQueries).map((q) => ({ sql: q.sql })),
      });
    }
  }

  function loadProjection(p: Projection) {
    // A completed projection is immutable — its config lives on the read-only
    // /details page. Bounce any completed projection there so Import only ever
    // shows work that is still editable (draft/executing/failed).
    if (p.status === "complete") {
      navigate(`/details?projection=${p.id}`, { replace: true });
      return;
    }
    // Switching projections: cancel the previous projection's poll and clear
    // its status so its progress bar can't bleed onto this one.
    stopPolling();
    setStatus(null);
    setPolling(false);
    setCurrentId(p.id);
    setGraphId(p.graph_id ?? null);
    if (p.project_id) setProjectId(p.project_id);
    // Set every field unconditionally (defaulting when absent) so this
    // projection fully REPLACES the previously loaded one — a missing value
    // must clear the old value, not keep it.
    setCatalog(p.catalog || "AwsDataCatalog");
    setDatabase(p.database || "");
    setPropertyGraph(p.property_graph || "");
    if (p.property_graph) setPgqOpen(true);
    setBucket(p.s3_staging_bucket || "");
    setGraphName(p.graph_name || "");
    setGraphMemoryGb(p.graph_memory_gb || 16);
    setChecks([]);
    setTranslateError(null);
    setPreview(null);
    setError(null);

    // Load multi-queries
    projection.getQueries(p.id).then((res) => {
      if (res.node_queries.length > 0) setNodeQueries(res.node_queries.map((q) => ({ id: q.id, sql: q.sql })));
      else setNodeQueries([{ sql: "" }]);

      if (res.edge_queries.length > 0) setEdgeQueries(res.edge_queries.map((q) => ({ id: q.id, sql: q.sql })));
      else setEdgeQueries([{ sql: "" }]);

      // Post-import openCypher graph queries (persisted text only, no results).
      // Clear when this projection has none, so a previous projection's queries
      // don't linger.
      if (res.graph_queries.length > 0) setGraphQueries(res.graph_queries.map((q) => ({ cypher: q.cypher })));
      else setGraphQueries([{ cypher: "" }]);
    });

    // Completed projections are redirected to /details above, so only in-flight
    // and not-yet-run states remain here. The backend marks an in-progress
    // import "importing" (see pipeline.py); "executing" is accepted too for
    // consistency with the rest of the app (Sidebar/Projections).
    if (p.status === "importing" || p.status === "executing") startPolling(p.id);
    else {
      setStatus(null);
      setPolling(false);
    }
  }

  // --- Query management ---
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  function saveCurrentQueries() {
    if (!currentId) return;
    persistQueries(currentId, nodeQueries, edgeQueries);
  }

  function scheduleSave(nq: UiNodeQuery[], eq: UiEdgeQuery[], id?: string) {
    const targetId = id ?? currentId;
    if (!targetId) return;
    pendingSaveRef.current.queriesDirty = true;
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      pendingSaveRef.current.queriesDirty = false;
      persistQueries(targetId, nq, eq);
    }, 1000);
  }

  function updateNodeQuery(index: number, sql: string) {
    setTranslateValidated(false);
    const updated = nodeQueries.map((q, i) => (i === index ? { ...q, sql, failed: undefined } : q));
    setNodeQueries(updated);
    scheduleSave(updated, edgeQueries);
  }
  function addNodeQuery() {
    setNodeQueries((prev) => [...prev, { sql: "" }]);
  }
  function removeNodeQuery(index: number) {
    setTranslateValidated(false);
    const updated = nodeQueries.filter((_, i) => i !== index);
    setNodeQueries(updated);
    if (currentId) persistQueries(currentId, updated, edgeQueries);
  }

  function updateEdgeQuery(index: number, updates: Partial<EdgeQueryInput>) {
    setTranslateValidated(false);
    const updated = edgeQueries.map((q, i) => (i === index ? { ...q, ...updates, failed: undefined } : q));
    setEdgeQueries(updated);
    scheduleSave(nodeQueries, updated);
  }
  function addEdgeQuery() {
    setEdgeQueries((prev) => [...prev, { sql: "" }]);
  }
  function removeEdgeQuery(index: number) {
    setTranslateValidated(false);
    const updated = edgeQueries.filter((_, i) => i !== index);
    setEdgeQueries(updated);
    if (currentId) persistQueries(currentId, nodeQueries, updated);
  }

  // --- Graph query management (openCypher; text persisted, results are not) ---
  // Graph queries are saved via the graph-only endpoint so authoring them here
  // never touches the node/edge queries, and they carry over to /details.
  const graphSaveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // Holds the latest values + which debounced saves are still pending, so we
  // can flush them if the component unmounts (navigating away) before the 1s
  // debounce fires — otherwise in-flight edits like graph queries are lost.
  const pendingSaveRef = useRef({
    currentId: null as string | null,
    nodeQueries: [] as NodeQueryInput[],
    edgeQueries: [] as EdgeQueryInput[],
    graphQueries: [] as { cypher: string }[],
    queriesDirty: false,
    graphDirty: false,
    configDirty: false,
    config: {} as Record<string, unknown>,
  });
  function scheduleGraphSave(gq: { cypher: string }[]) {
    if (!currentId) return;
    pendingSaveRef.current.graphDirty = true;
    if (graphSaveTimer.current) clearTimeout(graphSaveTimer.current);
    graphSaveTimer.current = setTimeout(() => {
      pendingSaveRef.current.graphDirty = false;
      projection.saveGraphQueries(currentId, gq.map((q) => ({ cypher: q.cypher })));
    }, 1000);
  }
  function updateGraphQuery(index: number, cypher: string) {
    const updated = graphQueries.map((q, i) => (i === index ? { cypher } : q));
    setGraphQueries(updated);
    scheduleGraphSave(updated);
  }
  function addGraphQuery() {
    setGraphQueries((prev) => [...prev, { cypher: "" }]);
  }
  function removeGraphQuery(index: number) {
    const updated = graphQueries.filter((_, i) => i !== index);
    setGraphQueries(updated);
    if (currentId) projection.saveGraphQueries(currentId, updated.map((q) => ({ cypher: q.cypher })));
  }

  // Expose the Import form to the global assistant: current field values, the
  // setters the mock fills, the page actions the assistant can surface as chat
  // buttons, and the active project for cross-page jumps.
  usePageBridge({
    page: "import",
    fields: {
      catalog, database, bucket, graphName, nodeQueries, edgeQueries, graphQueries,
      // Import state so the assistant knows a projection/graph already exists.
      projectionId: currentId, graphStatus: status?.status ?? null, graphId,
    },
    setters: {
      catalog: setCatalog,
      databases: setDatabases,
      database: setDatabase,
      bucket: setBucket,
      graphName: setGraphName,
      nodeQueries: setNodeQueries,
      edgeQueries: setEdgeQueries,
      graphQueries: setGraphQueries,
    },
    actions: {
      execute: { label: "Execute", run: handleExecute, enabled: !polling && !loading },
      validateQuery: { label: "Validate Query", run: handleValidateQuery, enabled: !loading },
      preview: { label: "Preview Schema", run: handlePreview, enabled: !loading },
    },
    persistImport,
    jumpContext: { projectId: projectId || searchParams.get("project") },
  });

  // --- Actions ---
  function markFailures(results: { check: string; passed: boolean }[]) {
    const failed = new Set(results.filter((c) => !c.passed).map((c) => c.check));
    setNodeQueries((prev) => prev.map((q, i) => ({ ...q, failed: failed.has(`node_query_${i + 1}`) || undefined })));
    setEdgeQueries((prev) => prev.map((q, i) => ({ ...q, failed: failed.has(`edge_query_${i + 1}`) || undefined })));
  }

  function missingFields() {
    const missing: { check: string; passed: boolean; message?: string }[] = [];
    if (!bucket) missing.push({ check: "S3 staging bucket", passed: false, message: "No S3 staging bucket selected." });
    if (!catalog) missing.push({ check: "Athena catalog", passed: false, message: "No Athena catalog selected." });
    if (!database) missing.push({ check: "Athena database", passed: false, message: "No Athena database selected." });
    const nameError = graphNameError(graphName, graphPrefix);
    if (!graphName.trim()) missing.push({ check: "Graph name", passed: false, message: "No graph name set." });
    else if (graphPrefix === null) missing.push({ check: "Graph name", passed: false, message: "Proxy configuration not loaded yet. Try again." });
    else if (nameError) missing.push({ check: "Graph name", passed: false, message: nameError });
    return missing;
  }

  async function handleValidate() {
    setChecks([]);
    setPreview(null);
    setError(null);
    setChecksSource("resources");
    const missing = missingFields();
    if (missing.length > 0) {
      setChecks(missing);
      return;
    }
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
    setChecksSource("queries");
    setLoading("validate-query");
    try {
      const id = await ensureProjection();
      const res = await projection.validateQuery(id);
      setChecks(res.checks);
      markFailures(res.checks);
    } catch (e: any) { setError(e.message); } finally { setLoading(null); }
  }

  async function handleTranslate() {
    setTranslateValidated(false);
    setChecks([]);
    setPreview(null);
    setError(null);
    setTranslateError(null);
    setLoading("translate");
    try {
      const id = await ensureProjection();
      const res = await projection.translate(id, propertyGraph);
      if (res.error) { setTranslateError(res.error); return; }
      const nodes = (res.node_queries ?? []).map((sql) => ({ sql }));
      const edges = (res.edge_queries ?? []).map((sql) => ({ sql }));
      const nq = nodes.length ? nodes : [{ sql: "" }];
      const eq = edges.length ? edges : [{ sql: "" }];
      setNodeQueries(nq);
      setEdgeQueries(eq);
      if (saveTimer.current) clearTimeout(saveTimer.current);
      await persistQueries(id, nq, eq);
      setLoading("translate-validate");
      try {
        const check = await projection.validateQuery(id);
        setChecksSource("queries");
        setChecks(check.checks);
        markFailures(check.checks);
        if (check.checks.some((c) => !c.passed)) setNodeEdgeOpen(true);
        else setTranslateValidated(true);
      } catch (e: any) {
        setTranslateError(e.message);
        setNodeEdgeOpen(true);
      }
    } catch (e: any) { setTranslateError(e.message); } finally { setLoading(null); }
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
    const missing = missingFields();
    if (missing.length > 0) {
      setChecksSource("resources");
      setChecks(missing);
      return;
    }
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
  // Hold the active poll interval so we can cancel it whenever we switch to a
  // different projection (or unmount). Without this, a previous projection's
  // interval keeps ticking setStatus(...) and its progress bar bleeds onto the
  // projection now on screen.
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current !== null) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  const startPolling = useCallback((id: string) => {
    // Cancel any in-flight poll (e.g. the previously loaded projection's) so
    // only this projection drives status/the progress bar.
    stopPolling();
    setPolling(true);

    // Fetch status once right away so the progress bar shows immediately on
    // load — otherwise it stays hidden until the first interval tick (below),
    // leaving a blank ~5s window when you open an in-flight projection.
    const poll = async () => {
      const s = await projection.status(id);
      setStatus(s);
      if (s.status === "complete" || s.status === "failed") {
        stopPolling();
        setPolling(false);
        if (s.error) setError(s.error);
      }
    };
    void poll();

    const interval = setInterval(poll, 5000);
    pollRef.current = interval;
  }, [stopPolling]);

  // Cancel polling on unmount so a background interval never updates unmounted
  // state (or leaks across a full page navigation).
  useEffect(() => stopPolling, [stopPolling]);

  // Keep the pending-save snapshot current every render so the unmount flush
  // below has the latest values (refs don't trigger re-renders).
  pendingSaveRef.current.currentId = currentId;
  pendingSaveRef.current.nodeQueries = nodeQueries;
  pendingSaveRef.current.edgeQueries = edgeQueries;
  pendingSaveRef.current.graphQueries = graphQueries;
  pendingSaveRef.current.config = configData();

  // Flush any pending debounced saves on unmount (navigating away). Without
  // this, edits made within the 1s debounce window — most visibly the
  // post-import graph queries — are lost because the timer never fires.
  useEffect(() => {
    return () => {
      const p = pendingSaveRef.current;
      if (!p.currentId) return;
      if (saveTimer.current) clearTimeout(saveTimer.current);
      if (graphSaveTimer.current) clearTimeout(graphSaveTimer.current);
      if (configTimer.current) clearTimeout(configTimer.current);
      // Fire-and-forget: the requests go out as the component tears down.
      if (p.configDirty) projection.update(p.currentId, p.config).catch(() => {});
      if (p.queriesDirty) {
        projection
          .saveQueries(p.currentId, { node_queries: p.nodeQueries, edge_queries: p.edgeQueries })
          .catch(() => {});
      }
      if (p.graphDirty) {
        projection
          .saveGraphQueries(p.currentId, p.graphQueries.map((q) => ({ cypher: q.cypher })))
          .catch(() => {});
      }
    };
  }, []);

  const hasQueries = nodeQueries.some((q) => q.sql.trim()) || edgeQueries.some((q) => q.sql.trim());

  function renderChecks(source: "resources" | "queries") {
    if (checksSource !== source || checks.length === 0) return null;
    return (
      <ul className="mt-2 space-y-1">
        {checks.map((c, i) => (
          <li key={i} className="flex items-center gap-2 text-sm">
            <span className={c.passed ? "text-green-600" : "text-red-600"}>{c.passed ? "✓" : "✗"}</span>
            <span className="font-medium">{c.check}</span>
            {c.message && <span className="text-gray-500">— {c.message}</span>}
          </li>
        ))}
      </ul>
    );
  }

  return (
    <div className="mx-auto max-w-3xl space-y-6">
      <div className="flex items-center justify-between pr-32">
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
              <Select value={database} onChange={(e) => setDatabase(e.target.value)} disabled={dbLoading} aria-invalid={!!dbError} aria-describedby="database-error">
                <option value="">{dbLoading ? "Loading..." : "Select database..."}</option>
                {databases.map((db) => <option key={db} value={db}>{db}</option>)}
              </Select>
              <span id="database-error" className="text-xs text-red-600" aria-live="polite">{dbError}</span>
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
              {graphMemoryGb > 1024 && (
                <p className="flex items-center gap-1 text-xs text-amber-700">
                  <AlertTriangle className="h-3.5 w-3.5 flex-shrink-0" />
                  This m-NCU size may not be available unless requested.
                </p>
              )}
            </label>
            <label className="space-y-1">
              <span className="text-sm font-medium text-gray-700">Graph Name</span>
              <input
                className={`w-full rounded-md border px-3 py-2 text-sm shadow-sm focus:ring-1 ${graphNameErrorText ? "border-red-500 focus:border-red-500 focus:ring-red-500" : "border-gray-300 focus:border-blue-500 focus:ring-blue-500"}`}
                placeholder="my-graph"
                value={graphName}
                onChange={(e) => setGraphName(e.target.value)}
                aria-invalid={!!graphNameErrorText}
                aria-describedby="graph-name-help"
              />
              <span id="graph-name-help" className="flex justify-between gap-2 text-xs">
                <span className="text-red-600" aria-live="polite">{graphNameErrorText}</span>
                {graphNameMax !== null && (
                  <span className={graphName.length > graphNameMax ? "text-red-600" : "text-gray-500"}>
                    {graphName.length}/{graphNameMax}
                  </span>
                )}
              </span>
            </label>
          </div>
          <div>
            <Button variant="secondary" onClick={handleValidate} disabled={!!loading}><CheckCircle className="h-4 w-4" /> {loading === "validate" ? "Validating..." : "Validate Resources"}</Button>
            {renderChecks("resources")}
          </div>
        </div>
      </Card>

      {/* Queries — PGQ generator + node/edge query editors */}
      <Card>
        <div className="space-y-5">
          <p className="text-sm text-gray-600">Start a graph projection from a data lake using Athena. Define the projection with PGQ, which translates to Athena SQL, OR write Athena SQL directly. Validate the SQL queries against the selected catalog and database before executing running the projection.</p>
          {/* PGQ generator (optional, collapsible) */}
          <div className="rounded-md border border-gray-200">
            <button
              type="button"
              onClick={() => setPgqOpen((o) => !o)}
              className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              <ChevronRight className={`h-4 w-4 transition-transform ${pgqOpen ? "rotate-90" : ""}`} />
              Property Graph Schema (PGQ)
              <span className="font-normal text-gray-400">optional</span>
            </button>
            {pgqOpen && (
              <div className="space-y-2 border-t border-gray-200 p-3">
                <p className="text-xs text-gray-500">Define the graph with a <em>PGQ (property graph query)</em> statement beginning with <code>CREATE PROPERTY GRAPH</code>. <em>Translate and Validate SQL Queries</em> translates it into node and edge SQL queries and validates the SQL against the selected catalog and database.</p>
                <textarea
                  className="w-full rounded-md border border-gray-300 px-3 py-2 text-sm font-mono shadow-sm focus:border-blue-500 focus:ring-1 focus:ring-blue-500"
                  rows={10}
                  placeholder={"CREATE PROPERTY GRAPH g\n  VERTEX TABLES (\n    accounts AS account KEY (id) LABEL account PROPERTIES (name)\n  )\n  EDGE TABLES (\n    transfers SOURCE (src) REFERENCES account\n              DESTINATION (dst) REFERENCES account\n              LABEL transfer PROPERTIES (amount)\n  )"}
                  value={propertyGraph}
                  onChange={(e) => { setPropertyGraph(e.target.value); setTranslateValidated(false); }}
                />
                <Button variant="secondary" onClick={handleTranslate} disabled={!!loading || !propertyGraph.trim()}>
                  <Wand2 className="h-4 w-4" /> {loading === "translate" ? "Translating..." : loading === "translate-validate" ? "Validating..." : "Translate and Validate SQL Queries"}
                </Button>
                {translateValidated && (
                  <span className="ml-2 inline-flex items-center gap-1 text-sm text-green-700">
                    <CheckCircle className="h-4 w-4" /> Validated
                  </span>
                )}
                {translateError && (
                  <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">{translateError}</div>
                )}
              </div>
            )}
          </div>

          <div className="rounded-md border border-gray-200">
            <button
              type="button"
              onClick={() => setNodeEdgeOpen((o) => !o)}
              className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              <ChevronRight className={`h-4 w-4 transition-transform ${nodeEdgeOpen ? "rotate-90" : ""}`} />
              Athena SQL (node &amp; edge queries)
            </button>
            {nodeEdgeOpen && (
              <div className="space-y-5 border-t border-gray-200 p-3">
                <p className="text-xs text-gray-500">These queries are imported as written. Validate after making changes.</p>
                {/* Node Queries */}
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h2 className="text-sm font-semibold">Node Queries</h2>
                    <button onClick={addNodeQuery} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800">
                      <Plus className="h-3 w-3" /> Add
                    </button>
                  </div>
                  <div className="max-h-72 space-y-3 overflow-y-auto pr-1">
                    {nodeQueries.map((nq, i) => (
                      <div key={i} className={`rounded-md border overflow-hidden ${nq.failed ? "border-red-400" : "border-gray-200"}`}>
                        <div className={`flex items-center justify-between px-3 py-1.5 border-b ${nq.failed ? "bg-red-50 border-red-200" : "bg-gray-50 border-gray-200"}`}>
                          <span className={`text-xs font-medium ${nq.failed ? "text-red-700" : "text-gray-700"}`}>
                            Node {i + 1}{nq.failed && " — failed validation"}
                          </span>
                          {nodeQueries.length > 1 && (
                            <button onClick={() => removeNodeQuery(i)} className="text-gray-400 hover:text-red-600">
                              <Trash2 className="h-3 w-3" />
                            </button>
                          )}
                        </div>
                        <textarea
                          className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-y"
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

                {/* Edge Queries */}
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h2 className="text-sm font-semibold">Edge Queries</h2>
                    <button onClick={addEdgeQuery} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800">
                      <Plus className="h-3 w-3" /> Add
                    </button>
                  </div>
                  <div className="max-h-72 space-y-3 overflow-y-auto pr-1">
                    {edgeQueries.map((eq, i) => (
                      <div key={i} className={`rounded-md border overflow-hidden ${eq.failed ? "border-red-400" : "border-gray-200"}`}>
                        <div className={`flex items-center justify-between px-3 py-1.5 border-b ${eq.failed ? "bg-red-50 border-red-200" : "bg-gray-50 border-gray-200"}`}>
                          <span className={`text-xs font-medium ${eq.failed ? "text-red-700" : "text-gray-700"}`}>
                            Edge {i + 1}{eq.failed && " — failed validation"}
                          </span>
                          {edgeQueries.length > 1 && (
                            <button onClick={() => removeEdgeQuery(i)} className="text-gray-400 hover:text-red-600">
                              <Trash2 className="h-3 w-3" />
                            </button>
                          )}
                        </div>
                        <textarea
                          className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-y"
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
                <Button variant="secondary" onClick={handleValidateQuery} disabled={!!loading}>
                  <CheckCircle className="h-4 w-4" /> {loading === "validate-query" ? "Validating..." : "Validate SQL Queries"}
                </Button>
                {renderChecks("queries")}
              </div>
            )}
          </div>
        </div>
      </Card>

      {/* Graph Queries (openCypher) — run against the graph after import */}
      <Card>
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <div>
              <h2 className="text-sm font-semibold">Graph Queries (openCypher)</h2>
              <p className="text-xs text-gray-500">Query or mutate the graph after import. Runs in sequence.</p>
            </div>
            <button onClick={addGraphQuery} className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800">
              <Plus className="h-3 w-3" /> Add
            </button>
          </div>
          <div className="max-h-72 space-y-3 overflow-y-auto pr-1">
            {graphQueries.map((gq, i) => (
              <div key={i} className="rounded-md border border-gray-200 overflow-hidden">
                <div className="flex items-center justify-between bg-gray-50 px-3 py-1.5 border-b border-gray-200">
                  <span className="text-xs font-medium text-gray-700">Query {i + 1}</span>
                  {graphQueries.length > 1 && (
                    <button onClick={() => removeGraphQuery(i)} className="text-gray-400 hover:text-red-600">
                      <Trash2 className="h-3 w-3" />
                    </button>
                  )}
                </div>
                <textarea
                  className="w-full px-3 py-2 text-sm font-mono border-0 focus:ring-0 resize-y"
                  rows={3}
                  placeholder="MATCH (n) RETURN n LIMIT 10"
                  value={gq.cypher}
                  onChange={(e) => updateGraphQuery(i, e.target.value)}
                />
              </div>
            ))}
          </div>
          <p className="text-xs text-gray-400">These run on the graph's details page once the import is complete.</p>
        </div>
      </Card>

      {/* Actions */}
      <div className="flex justify-end gap-2">
        <Button variant="secondary" onClick={handlePreview} disabled={!!loading || !hasQueries}><Eye className="h-4 w-4" /> {loading === "preview" ? "Loading..." : "Preview"}</Button>
        <Button onClick={handleExecute} disabled={polling || !!loading || !hasQueries}><Play className="h-4 w-4" /> Execute</Button>
      </div>

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
            <div className="space-y-3">
              <p className="text-sm font-medium text-green-700">✓ Graph ready</p>
              {status.graph_endpoint && <p className="text-sm text-gray-600">Endpoint: <code className="rounded bg-gray-100 px-1">{status.graph_endpoint}</code></p>}
              {currentId && (
                <Button onClick={() => navigate(`/details?projection=${currentId}`, { state: { graphQueries } })}>
                  View Details
                </Button>
              )}
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
