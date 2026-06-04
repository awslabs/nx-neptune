import { useEffect, useState, useCallback } from "react";
import { metadata } from "../api";
import { Button, Select, Card } from "../components/ui";
import { Plus, Copy, Trash2 } from "lucide-react";

interface Column { name: string; type: string }
interface Join { table: string; leftCol: string; rightCol: string }

export function QueryBuilder() {
  const [catalogs, setCatalogs] = useState<{ name: string; status: string }[]>([]);
  const [catalog, setCatalog] = useState("AwsDataCatalog");
  const [databases, setDatabases] = useState<string[]>([]);
  const [dbLoading, setDbLoading] = useState(false);
  const [database, setDatabase] = useState("");
  const [tables, setTables] = useState<string[]>([]);
  const [primaryTable, setPrimaryTable] = useState("");
  const [tableColumns, setTableColumns] = useState<Record<string, Column[]>>({});
  const [joins, setJoins] = useState<Join[]>([]);
  const [mapping, setMapping] = useState({ id: "", label: "", from: "", to: "" });
  const [propChecked, setPropChecked] = useState<Set<string>>(new Set());
  const [aliases, setAliases] = useState<Record<string, string>>({});
  const [savedQueries, setSavedQueries] = useState<string[]>([]);

  useEffect(() => { metadata.catalogs().then(d => setCatalogs(d.catalogs)); }, []);

  useEffect(() => {
    if (!catalog) return;
    setDbLoading(true);
    metadata.databases(catalog).then(d => { setDatabases(d.databases); setDbLoading(false); });
  }, [catalog]);

  useEffect(() => {
    if (!database) return;
    metadata.tables(database, catalog).then(d => setTables(d.tables));
    setTableColumns({});
    setJoins([]);
    setPrimaryTable("");
  }, [database, catalog]);

  const loadColumns = useCallback(async (table: string) => {
    if (tableColumns[table]) return tableColumns[table];
    const data = await metadata.columns(database, table, catalog);
    setTableColumns(prev => ({ ...prev, [table]: data.columns }));
    return data.columns;
  }, [database, catalog, tableColumns]);

  useEffect(() => {
    if (primaryTable) loadColumns(primaryTable);
  }, [primaryTable, loadColumns]);

  function getAllColumns() {
    const cols: { full: string; name: string; table: string; type: string }[] = [];
    if (primaryTable && tableColumns[primaryTable]) {
      tableColumns[primaryTable].forEach(c => cols.push({ full: `${primaryTable}.${c.name}`, name: c.name, table: primaryTable, type: c.type }));
    }
    joins.forEach(j => {
      if (j.table && tableColumns[j.table]) {
        tableColumns[j.table].forEach(c => cols.push({ full: `${j.table}.${c.name}`, name: c.name, table: j.table, type: c.type }));
      }
    });
    return cols;
  }

  function generateSQL() {
    const cols = getAllColumns();
    if (!cols.length) return "";
    const mapped = new Set([mapping.id, mapping.label, mapping.from, mapping.to].filter(Boolean));
    const parts: string[] = [];
    if (mapping.id) parts.push(`${mapping.id} AS "~id"`);
    if (mapping.label) parts.push(`${mapping.label} AS "~label"`);
    if (mapping.from) parts.push(`${mapping.from} AS "~from"`);
    if (mapping.to) parts.push(`${mapping.to} AS "~to"`);
    for (const col of cols) {
      if (propChecked.has(col.full) && !mapped.has(col.full)) {
        const alias = aliases[col.full];
        parts.push(alias ? `${col.full} AS "${alias}"` : col.full);
      }
    }
    if (!parts.length) return "";
    let sql = `SELECT ${parts.join(",\n       ")}\nFROM ${primaryTable}`;
    joins.forEach(j => {
      if (j.table && j.leftCol && j.rightCol) {
        sql += `\nJOIN ${j.table} ON ${j.leftCol} = ${j.rightCol}`;
      }
    });
    return sql;
  }

  async function addJoin() {
    const available = tables.filter(t => t !== primaryTable && !joins.some(j => j.table === t));
    if (!available.length) return;
    const table = available[0];
    await loadColumns(table);
    setJoins([...joins, { table, leftCol: "", rightCol: "" }]);
  }

  function updateJoin(idx: number, field: keyof Join, value: string) {
    const next = [...joins];
    next[idx] = { ...next[idx], [field]: value };
    setJoins(next);
  }

  async function changeJoinTable(idx: number, table: string) {
    await loadColumns(table);
    updateJoin(idx, "table", table);
  }

  function removeJoin(idx: number) {
    setJoins(joins.filter((_, i) => i !== idx));
  }

  const allCols = getAllColumns();
  const sql = generateSQL();
  const colOptions = allCols.map(c => <option key={c.full} value={c.full}>{c.full} ({c.type})</option>);

  return (
    <div className="mx-auto max-w-4xl space-y-6">
      <h1 className="text-lg font-semibold">Query Builder</h1>

      <Card>
        <div className="grid grid-cols-3 gap-4">
          <label className="space-y-1">
            <span className="text-sm font-medium text-gray-700">Catalog</span>
            <Select value={catalog} onChange={e => setCatalog(e.target.value)}>
              {catalogs.map(c => <option key={c.name} value={c.name}>{c.name}</option>)}
            </Select>
          </label>
          <label className="space-y-1">
            <span className="text-sm font-medium text-gray-700">Database</span>
            <Select value={database} onChange={e => setDatabase(e.target.value)} disabled={dbLoading}>
              <option value="">{dbLoading ? "Loading..." : "— select —"}</option>
              {databases.map(db => <option key={db} value={db}>{db}</option>)}
            </Select>
          </label>
          <label className="space-y-1">
            <span className="text-sm font-medium text-gray-700">Primary Table</span>
            <Select value={primaryTable} onChange={e => setPrimaryTable(e.target.value)} disabled={!tables.length}>
              <option value="">— select —</option>
              {tables.map(t => <option key={t} value={t}>{t}</option>)}
            </Select>
          </label>
        </div>
      </Card>

      {/* Joins */}
      <Card>
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold">Joins</h2>
          <Button variant="secondary" onClick={addJoin} disabled={!primaryTable}><Plus className="h-3 w-3" /> Add Join</Button>
        </div>
        {joins.length === 0 && <p className="text-sm text-gray-500">No joins added.</p>}
        {joins.map((j, idx) => {
          const primaryCols = (tableColumns[primaryTable] || []).map(c => `${primaryTable}.${c.name}`);
          const joinCols = (tableColumns[j.table] || []).map(c => `${j.table}.${c.name}`);
          return (
            <div key={idx} className="mt-2 flex items-center gap-2 rounded border border-gray-200 bg-gray-50 p-2">
              <span className="text-xs font-medium text-gray-500">JOIN</span>
              <Select className="flex-1" value={j.table} onChange={e => changeJoinTable(idx, e.target.value)}>
                {tables.filter(t => t !== primaryTable).map(t => <option key={t} value={t}>{t}</option>)}
              </Select>
              <span className="text-xs text-gray-500">ON</span>
              <Select className="flex-1" value={j.leftCol} onChange={e => updateJoin(idx, "leftCol", e.target.value)}>
                <option value="">—</option>
                {primaryCols.map(c => <option key={c} value={c}>{c}</option>)}
              </Select>
              <span className="text-xs">=</span>
              <Select className="flex-1" value={j.rightCol} onChange={e => updateJoin(idx, "rightCol", e.target.value)}>
                <option value="">—</option>
                {joinCols.map(c => <option key={c} value={c}>{c}</option>)}
              </Select>
              <button onClick={() => removeJoin(idx)} className="text-red-500 hover:text-red-700"><Trash2 className="h-4 w-4" /></button>
            </div>
          );
        })}
      </Card>

      {/* Column Mapping */}
      {allCols.length > 0 && (
        <Card>
          <h2 className="text-sm font-semibold mb-3">Column Mapping</h2>
          <div className="grid grid-cols-2 gap-3">
            {(["id", "label", "from", "to"] as const).map(field => (
              <label key={field} className="space-y-1">
                <span className="text-sm font-medium text-gray-700">~{field}</span>
                <Select value={mapping[field]} onChange={e => setMapping({ ...mapping, [field]: e.target.value })}>
                  <option value="">(none)</option>
                  {colOptions}
                </Select>
              </label>
            ))}
          </div>
        </Card>
      )}

      {/* Properties */}
      {allCols.length > 0 && (
        <Card>
          <h2 className="text-sm font-semibold mb-3">Include as Properties</h2>
          <div className="max-h-40 overflow-y-auto space-y-1">
            {allCols.map(col => (
              <div key={col.full} className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={propChecked.has(col.full)}
                  onChange={e => {
                    const next = new Set(propChecked);
                    e.target.checked ? next.add(col.full) : next.delete(col.full);
                    setPropChecked(next);
                  }}
                />
                <span className="min-w-[180px] font-mono text-xs">{col.full}</span>
                <input
                  className="w-24 rounded border border-gray-300 px-2 py-0.5 text-xs"
                  placeholder="alias"
                  value={aliases[col.full] || ""}
                  onChange={e => setAliases({ ...aliases, [col.full]: e.target.value })}
                />
              </div>
            ))}
          </div>
        </Card>
      )}

      {/* SQL Output */}
      {sql && (
        <Card>
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-sm font-semibold">Generated SQL</h2>
            <div className="flex gap-2">
              <Button variant="secondary" onClick={() => { setSavedQueries([...savedQueries, sql]); }}>
                <Plus className="h-3 w-3" /> Save
              </Button>
              <Button variant="ghost" onClick={() => navigator.clipboard.writeText(sql)}>
                <Copy className="h-3 w-3" /> Copy
              </Button>
            </div>
          </div>
          <pre className="overflow-auto rounded bg-gray-50 p-3 font-mono text-xs">{sql}</pre>
        </Card>
      )}

      {/* Saved Queries */}
      {savedQueries.length > 0 && (
        <Card>
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-sm font-semibold">Saved Queries</h2>
            <Button variant="ghost" onClick={() => navigator.clipboard.writeText(savedQueries.join(";\n"))}>
              <Copy className="h-3 w-3" /> Copy All
            </Button>
          </div>
          {savedQueries.map((q, i) => (
            <div key={i} className="relative mt-2 rounded bg-gray-50 p-3">
              <button className="absolute right-2 top-2 text-red-500 hover:text-red-700" onClick={() => setSavedQueries(savedQueries.filter((_, j) => j !== i))}>
                <Trash2 className="h-3 w-3" />
              </button>
              <pre className="font-mono text-xs">{q}</pre>
            </div>
          ))}
        </Card>
      )}
    </div>
  );
}
