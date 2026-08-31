import { useState, useRef, useEffect, useCallback } from "react";
import { Play, X, AlertTriangle, Loader2 } from "lucide-react";

/**
 * GraphQueryConsole — bottom-docked, resizable query console (UI skeleton).
 *
 * Implements the manual (Phase 1) layout in
 * docs/design-graph-query-console-full.md:
 *   compact meta header · editable query + Run | results
 *
 * SCOPE: This PR ships the UI skeleton only — the layout, the resizable panel,
 * the query editor, the Run button, and the results/error render shells.
 * Run is intentionally NOT wired to the backend yet: the backend endpoint
 * (POST /api/v0/projection/{id}/graph-query) ships separately, and wiring
 * `handleRun` to it — plus rendering real { columns, rows } — is the next PR.
 * There is deliberately no canned/dummy data here.
 */

interface GraphMeta {
  graphName: string;
  graphId: string;
  status: string;
}

interface ResultTable {
  columns: string[];
  rows: string[][];
}

const DEFAULT_QUERY = "MATCH (n) RETURN n LIMIT 25";

export function GraphQueryConsole({
  meta,
  onClose,
}: {
  meta: GraphMeta;
  onClose: () => void;
}) {
  const [query, setQuery] = useState(DEFAULT_QUERY);
  // Result/error state and the render shells below are intentionally kept so the
  // next PR only needs to populate them from the API response.
  const [result] = useState<ResultTable | null>(null);
  const [error] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  // --- resizable height ---
  const [height, setHeight] = useState(340);
  const dragging = useRef(false);

  const onDrag = useCallback((e: MouseEvent) => {
    if (!dragging.current) return;
    const next = window.innerHeight - e.clientY;
    setHeight(Math.min(Math.max(next, 180), window.innerHeight * 0.85));
  }, []);

  useEffect(() => {
    const stop = () => (dragging.current = false);
    window.addEventListener("mousemove", onDrag);
    window.addEventListener("mouseup", stop);
    return () => {
      window.removeEventListener("mousemove", onDrag);
      window.removeEventListener("mouseup", stop);
    };
  }, [onDrag]);

  // --- run action ---
  // TODO(next PR): call projection.graphQuery(meta.graphId, query) and set
  // result / error from the { columns, rows } | { error } response.
  function handleRun() {
    // Not wired yet — UI skeleton only.
  }

  return (
    <div
      className="fixed inset-x-0 bottom-0 z-30 flex flex-col border-t border-gray-300 bg-white shadow-[0_-4px_20px_rgba(0,0,0,0.08)]"
      style={{ height }}
    >
      {/* drag handle */}
      <div
        onMouseDown={() => (dragging.current = true)}
        className="flex h-3 w-full cursor-row-resize items-center justify-center border-b border-gray-100 bg-gray-50 hover:bg-gray-100"
        title="Drag to resize"
      >
        <div className="h-1 w-10 rounded-full bg-gray-300" />
      </div>

      {/* compact meta header */}
      <div className="flex items-center justify-between border-b border-gray-200 px-4 py-2 text-sm">
        <div className="flex items-center gap-2 truncate">
          <span className="font-semibold">{meta.graphName}</span>
          <span className="text-gray-400">·</span>
          <span className="font-mono text-xs text-gray-500">{meta.graphId}</span>
          <span className="text-gray-400">·</span>
          <span className="inline-flex rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700">
            {meta.status}
          </span>
        </div>
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600" title="Close">
          <X className="h-4 w-4" />
        </button>
      </div>

      {/* body: left (query) | right (results) */}
      <div className="flex min-h-0 flex-1">
        {/* LEFT COLUMN */}
        <div className="flex w-1/2 min-w-0 flex-col gap-2 border-r border-gray-200 p-3">
          {/* editable query */}
          <div className="flex min-h-0 flex-1 flex-col rounded-md border border-gray-200">
            <div className="border-b border-gray-200 bg-gray-50 px-3 py-1.5 text-xs font-medium text-gray-600">
              Query
            </div>
            <textarea
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
              }}
              spellCheck={false}
              className="min-h-0 flex-1 resize-none px-3 py-2 font-mono text-sm focus:outline-none"
            />
            <div className="flex justify-end border-t border-gray-100 p-2">
              <button
                onClick={handleRun}
                disabled={running}
                className="inline-flex items-center gap-1.5 rounded-md bg-blue-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
              >
                {running ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                Run
              </button>
            </div>
          </div>
        </div>

        {/* RIGHT COLUMN: results */}
        <div className="flex w-1/2 min-w-0 flex-col p-3">
          <div className="mb-2 text-xs font-medium text-gray-600">Results</div>
          <div className="min-h-0 flex-1 overflow-auto rounded-md border border-gray-200">
            {error ? (
              <div className="flex items-start gap-2 p-3 text-sm text-red-700">
                <AlertTriangle className="mt-0.5 h-4 w-4 flex-shrink-0" />
                <span>{error}</span>
              </div>
            ) : result ? (
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 border-b bg-gray-50">
                  <tr>
                    {result.columns.map((c) => (
                      <th key={c} className="px-3 py-2 font-medium">{c}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {result.rows.map((row, i) => (
                    <tr key={i} className="border-b last:border-0">
                      {row.map((cell, j) => (
                        <td key={j} className="px-3 py-2 text-gray-700">{cell}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="p-3 text-sm text-gray-400">Run a query to see results.</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
