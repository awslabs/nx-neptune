import { NavLink } from "react-router";
import { Upload, ListTodo, Network, PanelLeftClose, PanelLeftOpen, Wrench } from "lucide-react";
import { clsx } from "clsx";

const links = [
  { to: "/import", label: "Import", icon: Upload },
  { to: "/query-builder", label: "Query Builder", icon: Wrench },
  { to: "/sessions", label: "Sessions", icon: ListTodo },
  { to: "/graphs", label: "Graphs", icon: Network },
];

export function Sidebar({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
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
      <nav className="flex-1 space-y-1 p-2">
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
      </nav>
    </aside>
  );
}
