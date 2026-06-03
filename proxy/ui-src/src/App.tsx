import { Routes, Route, Navigate } from "react-router";
import { Sidebar } from "./components/Sidebar";
import { Import } from "./pages/Import";
import { Sessions } from "./pages/Sessions";
import { Graphs } from "./pages/Graphs";
import { QueryBuilder } from "./pages/QueryBuilder";

export default function App() {
  return (
    <div className="flex h-screen">
      <Sidebar />
      <main className="flex-1 overflow-auto p-6">
        <Routes>
          <Route path="/" element={<Navigate to="/import" replace />} />
          <Route path="/import" element={<Import />} />
          <Route path="/sessions" element={<Sessions />} />
          <Route path="/graphs" element={<Graphs />} />
          <Route path="/query-builder" element={<QueryBuilder />} />
        </Routes>
      </main>
    </div>
  );
}
