import { NavLink } from "react-router";
import { Upload, ListTodo, Network } from "lucide-react";
import { clsx } from "clsx";

const links = [
  { to: "/import", label: "Import", icon: Upload },
  { to: "/sessions", label: "Sessions", icon: ListTodo },
  { to: "/graphs", label: "Graphs", icon: Network },
];

export function Sidebar() {
  return (
    <aside className="flex w-60 flex-col border-r border-gray-200 bg-white">
      <div className="flex h-14 items-center gap-2 border-b border-gray-200 px-4">
        <Network className="h-5 w-5 text-blue-600" />
        <span className="text-sm font-semibold">nx-neptune</span>
      </div>
      <nav className="flex-1 space-y-1 p-3">
        {links.map(({ to, label, icon: Icon }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              clsx(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm transition-colors",
                isActive
                  ? "bg-blue-50 text-blue-700 font-medium"
                  : "text-gray-600 hover:bg-gray-100 hover:text-gray-900",
              )
            }
          >
            <Icon className="h-4 w-4" />
            {label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
