import { useState, useMemo, useSyncExternalStore } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router";
import { AlertTriangle } from "lucide-react";
import { Sidebar } from "./components/Sidebar";
import { Import } from "./pages/Import";
import { Projections } from "./pages/Projections";
import { Graphs } from "./pages/Graphs";
import { Projects } from "./pages/Projects";
import { Landing } from "./pages/Landing";
import { useKeyboard } from "./hooks/useKeyboard";
import { tokenStatus, TOKEN_EXPIRED_MESSAGE } from "./api";

export default function App() {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();

  const handlers = useMemo(() => ({
    "[": () => setCollapsed(c => !c),
    "r": () => navigate(0),
  }), [navigate]);

  useKeyboard(handlers);

  const tokenExpired = useSyncExternalStore(tokenStatus.subscribe, tokenStatus.isExpired);

  return (
    <div className="flex h-screen">
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed(c => !c)} />
      <main className="flex-1 overflow-auto p-6">
        {tokenExpired && (
          <div role="alert" className="mb-4 flex items-start gap-3 rounded-lg border border-red-200 bg-red-50 p-3">
            <AlertTriangle aria-hidden="true" className="mt-0.5 h-4 w-4 flex-shrink-0 text-red-600" />
            <p className="text-sm text-red-700">{TOKEN_EXPIRED_MESSAGE}</p>
          </div>
        )}
        <Routes>
          <Route path="/" element={<Landing />} />
          <Route path="/import" element={<Import />} />
          <Route path="/projections" element={<Projections />} />
          <Route path="/graphs" element={<Graphs />} />
          <Route path="/projects" element={<Projects />} />
        </Routes>
      </main>
    </div>
  );
}
