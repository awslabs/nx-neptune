import { useEffect, useState } from "react";
import { NavLink } from "react-router";
import { Upload, ListTodo, Network, PanelLeftClose, PanelLeftOpen, Wrench, FolderPlus, ChevronDown, ChevronRight, Circle, RefreshCw } from "lucide-react";
import { clsx } from "clsx";
import { workspaceApi, projection, type Workspace, type Projection } from "../api";

const links = [
  { to: "/import", label: "Import", icon: Upload },
  { to: "/query-builder", label: "Query Builder", icon: Wrench },
  { to: "/sessions", label: "Sessions", icon: ListTodo },
  { to: "/graphs", label: "Graphs", icon: Network },
  { to: "/workspaces", label: "Workspaces", icon: FolderPlus },
];

const statusColor: Record<string, string> = {
  complete: "text-green-500",
  executing: "text-blue-500",
  importing: "text-blue-500",
  failed: "text-red-500",
};

export function Sidebar({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  const [workspaces, setWorkspaces] = useState<Workspace[]>([]);
  const [projections, setProjections] = useState<Projection[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  useEffect(() => {
    loadWorkspaces();
  }, []);

  function loadWorkspaces() {
    workspaceApi.list().then(setWorkspaces);
    projection.list().then(setProjections);
  }

  function toggle(id: string) {
    setExpanded(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  }

  const projByWorkspace = (wsId: string) => projections.filter(p => p.workspace_id === wsId);

  return (
    <aside className={clsx("flex flex-col border-r border-gray-200 bg-white transition-all", collapsed ? "w-14" : "w-60")}>
      <div className="flex h-14 items-center justify-between border-b border-gray-200 px-3">
        {!collapsed && (
          <div className="flex items-center gap-2">
            <Network className="h-5 w-5 text-blue-600" />
            <span className="text-sm font-semibold">nx-neptune</span>
          </div>
        )}
        <button onClick={onToggle} className="rounded p-1 text-gray-500 hover:bg-gray-100">
          {collapsed ? <PanelLeftOpen className="h-4 w-4" /> : <PanelLeftClose className="h-4 w-4" />}
        </button>
      </div>
      <nav className="flex-1 overflow-y-auto p-2">
        <div className="space-y-1">
          {links.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              title={label}
              className={({ isActive }) =>
                clsx(
                  "flex items-center rounded-md px-3 py-2 text-sm transition-colors",
                  collapsed ? "justify-center" : "gap-3",
                  isActive
                    ? "bg-blue-50 text-blue-700 font-medium"
                    : "text-gray-600 hover:bg-gray-100 hover:text-gray-900",
                )
              }
            >
              <Icon className="h-4 w-4 shrink-0" />
              {!collapsed && label}
            </NavLink>
          ))}
        </div>

        {!collapsed && workspaces.length > 0 && (
          <div className="mt-4 border-t border-gray-200 pt-3">
            <div className="flex items-center justify-between px-3">
              <span className="text-xs font-semibold uppercase text-gray-400">Workspaces</span>
              <button onClick={loadWorkspaces} className="rounded p-0.5 text-gray-400 hover:text-gray-600" title="Refresh">
                <RefreshCw className="h-3 w-3" />
              </button>
            </div>
            <div className="mt-2 space-y-0.5">
              {workspaces.map(ws => {
                const projs = projByWorkspace(ws.id);
                const isOpen = expanded.has(ws.id);
                return (
                  <div key={ws.id}>
                    <div className="flex w-full items-center gap-1 rounded-md px-3 py-1.5 text-sm text-gray-700 hover:bg-gray-100">
                      <button onClick={() => toggle(ws.id)} className="shrink-0 p-0.5">
                        {isOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                      </button>
                      <NavLink
                        to={`/sessions?workspace=${ws.id}`}
                        className="flex-1 truncate font-medium hover:text-blue-600"
                      >{ws.name}</NavLink>
                      {projs.length > 0 && <span className="text-xs text-gray-400">{projs.length}</span>}
                    </div>
                    {isOpen && (
                      <div className="ml-5 space-y-0.5">
                        {projs.map(p => (
                          <NavLink
                            key={p.id}
                            to={`/import?session=${p.id}`}
                            className="flex items-center gap-2 rounded-md px-2 py-1 text-xs text-gray-600 hover:bg-gray-100"
                          >
                            <Circle className={clsx("h-2 w-2 fill-current", statusColor[p.status] || "text-gray-400")} />
                            <span className="truncate">{p.graph_name || p.id.slice(0, 8)}</span>
                          </NavLink>
                        ))}
                        <NavLink
                          to={`/import?workspace=${ws.id}`}
                          className="flex items-center gap-2 rounded-md px-2 py-1 text-xs text-gray-500 hover:bg-gray-100 hover:text-gray-700"
                        >
                          <span>+ Add Graph</span>
                        </NavLink>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </nav>
    </aside>
  );
}
