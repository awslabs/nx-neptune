import { useState, useMemo } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router";
import { Sidebar } from "./components/Sidebar";
import { Import } from "./pages/Import";
import { Sessions } from "./pages/Sessions";
import { Graphs } from "./pages/Graphs";
import { Projects } from "./pages/Projects";
import { Landing } from "./pages/Landing";
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
          <Route path="/" element={<Landing />} />
          <Route path="/import" element={<Import />} />
          <Route path="/sessions" element={<Sessions />} />
          <Route path="/graphs" element={<Graphs />} />
          <Route path="/projects" element={<Projects />} />
        </Routes>
      </main>
    </div>
  );
}
