import { NavLink, useNavigate, useLocation } from "react-router";
import { Wrench, FolderOpen, Plus, PanelLeftClose, PanelLeftOpen, Network } from "lucide-react";
import { clsx } from "clsx";

const MOCK_GRAPHS = [
  { id: "g-abc123def4" },
  { id: "g-xyz789ghi0" },
  { id: "g-mno456pqr1" },
];

const navLinks = [
  { to: "/query-manager", label: "Query Manager", icon: Wrench },
];

export function Sidebar({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  const navigate = useNavigate();
  const location = useLocation();
  const isAddGraph = location.pathname === "/import";
  const isAddProject = location.pathname === "/add-project";

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
      <nav className="flex-1 space-y-1 overflow-y-auto p-2">
        {/* Project section */}
        {collapsed ? (
          <NavLink
            to="/"
            title="Project"
            className={({ isActive }) =>
              clsx(
                "flex items-center justify-center rounded-md px-3 py-2 text-sm transition-colors",
                isActive
                  ? "bg-blue-50 text-blue-700 font-medium"
                  : "text-gray-600 hover:bg-gray-100 hover:text-gray-900",
              )
            }
          >
            <FolderOpen className="h-4 w-4 shrink-0" />
          </NavLink>
        ) : (
          <div className="mb-1">
            <div className="flex items-center justify-between px-3 py-2">
              <span className="text-xs font-semibold uppercase tracking-wide text-gray-400">Project</span>
              <button
                onClick={() => navigate("/add-project")}
                className={clsx(
                  "rounded p-1",
                  isAddProject ? "bg-blue-100 text-blue-700" : "text-gray-400 hover:bg-gray-100 hover:text-gray-600",
                )}
                title="New Project"
              >
                <Plus className="h-3.5 w-3.5" />
              </button>
            </div>
            <NavLink
              to="/"
              end
              className={({ isActive }) =>
                clsx(
                  "flex items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors",
                  isActive
                    ? "bg-blue-50 text-blue-700 font-medium"
                    : "text-gray-600 hover:bg-gray-100 hover:text-gray-900",
                )
              }
            >
              <FolderOpen className="h-4 w-4 shrink-0" />
              MyProject
            </NavLink>
            {/* Graph sub-list */}
            <div className="ml-5 border-l border-gray-200 pl-3">
              {MOCK_GRAPHS.map((g) => (
                <div
                  key={g.id}
                  className="flex items-center gap-2 rounded-md px-2 py-1.5 text-xs text-gray-500"
                >
                  <Network className="h-3 w-3 shrink-0 text-gray-400" />
                  <span className="truncate font-mono">{g.id}</span>
                </div>
              ))}
              <button
                onClick={() => navigate("/import")}
                className={clsx(
                  "flex w-full items-center gap-2 rounded-md px-2 py-1.5 text-xs",
                  isAddGraph
                    ? "bg-blue-50 text-blue-700 font-medium"
                    : "text-gray-400 hover:text-blue-600",
                )}
                title="Add graph"
              >
                <Plus className="h-3 w-3 shrink-0" />
                <span>Add graph</span>
              </button>
            </div>
          </div>
        )}

        {/* Nav links */}
        {navLinks.map(({ to, label, icon: Icon }) => (
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
      </nav>
    </aside>
  );
}
