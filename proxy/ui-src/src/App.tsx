import { useState, useMemo } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router";
import { Sidebar } from "./components/Sidebar";
import { Import } from "./pages/Import";
import { Project } from "./pages/Project";
import { AddProject } from "./pages/AddProject";
import { QueryManager } from "./pages/QueryManager";
import { AddProjection } from "./pages/AddProjection";
import { useKeyboard } from "./hooks/useKeyboard";

export default function App() {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();

  const handlers = useMemo(() => ({
    "[": () => setCollapsed(c => !c),
    "r": () => navigate(0),
  }), [navigate]);

  useKeyboard(handlers);

  return (
    <div className="flex h-screen">
      <Sidebar collapsed={collapsed} onToggle={() => setCollapsed(c => !c)} />
      <main className="flex-1 overflow-auto p-6">
        <Routes>
          <Route path="/" element={<Project />} />
          <Route path="/add-project" element={<AddProject />} />
          <Route path="/import" element={<Import />} />
          <Route path="/query-manager" element={<QueryManager />} />
          <Route path="/add-projection" element={<AddProjection />} />
        </Routes>
      </main>
    </div>
  );
}
