import { useEffect, useState } from "react";
import { NavLink } from "react-router";
import { Network, PanelLeftClose, PanelLeftOpen, ChevronDown, ChevronRight, Circle, Plus, Trash2 } from "lucide-react";
import { clsx } from "clsx";
import { projectApi, projection, type Project, type Projection } from "../api";

const statusColor: Record<string, string> = {
  complete: "text-green-500",
  executing: "text-blue-500",
  importing: "text-blue-500",
  failed: "text-red-500",
};

export function Sidebar({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  const [projects, setProjects] = useState<Project[]>([]);
  const [projections, setProjections] = useState<Projection[]>([]);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  useEffect(() => {
    loadProjects();
    const handler = () => loadProjects();
    window.addEventListener("projects-changed", handler);
    return () => window.removeEventListener("projects-changed", handler);
  }, []);

  function loadProjects() {
    projectApi.list().then(setProjects);
    projection.list().then(setProjections);
  }

  function toggle(id: string) {
    setExpanded(prev => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });
  }

  const projByProject = (projectId: string) => projections.filter(p => p.project_id === projectId);

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
        <NavLink
          to="/graphs"
          title="Graphs"
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
          <Network className="h-4 w-4 shrink-0" />
          {!collapsed && "Graphs"}
        </NavLink>
        {!collapsed && (
          <div>
            <div className="flex items-center justify-between px-3">
              <span className="text-xs font-semibold uppercase text-gray-400">Projects</span>
              <NavLink to="/projects" className="rounded p-0.5 text-gray-400 hover:text-gray-600" title="Add Project">
                <Plus className="h-3 w-3" />
              </NavLink>
            </div>
            <div className="mt-2 space-y-0.5">
              {projects.map(proj => {
                const projs = projByProject(proj.id);
                const isOpen = expanded.has(proj.id);
                const isDeleting = proj.status === "deleting";
                return (
                  <div key={proj.id}>
                    <div className={clsx("group flex w-full items-center gap-1 rounded-md px-3 py-1.5 text-sm hover:bg-gray-100", isDeleting ? "text-gray-400 italic" : "text-gray-700")}>
                      <button onClick={() => !isDeleting && toggle(proj.id)} className="shrink-0 p-0.5" disabled={isDeleting}>
                        {isOpen ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
                      </button>
                      {isDeleting ? (
                        <span className="flex-1 truncate">{proj.name} (deleting...)</span>
                      ) : (
                        <NavLink
                          to={`/sessions?project=${proj.id}`}
                          className="flex-1 truncate font-medium hover:text-blue-600"
                        >{proj.name}</NavLink>
                      )}
                      {!isDeleting && (
                        <button
                          onClick={async (e) => {
                            e.stopPropagation();
                            if (!confirm(`Delete project "${proj.name}"?`)) return;
                            await projectApi.delete(proj.id);
                            loadProjects();
                          }}
                          className="hidden shrink-0 rounded p-0.5 text-gray-400 hover:text-red-600 group-hover:block"
                          title="Delete project"
                        >
                          <Trash2 className="h-3 w-3" />
                        </button>
                      )}
                      {!isDeleting && projs.length > 0 && <span className="text-xs text-gray-400 group-hover:hidden">{projs.length}</span>}
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
                          to={`/import?project=${proj.id}&t=${Date.now()}`}
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
