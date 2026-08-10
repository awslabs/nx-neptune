import { useRef, useEffect, useCallback } from "react";
import CytoscapeComponent from "react-cytoscapejs";
import cytoscape from "cytoscape";

export interface SchemaNode {
  id: string;
  label: string;
}

export interface SchemaEdge {
  id: string;
  source: string;
  target: string;
  label: string;
}

interface SchemaPreviewProps {
  nodes: SchemaNode[];
  edges: SchemaEdge[];
}

const LAYOUT: cytoscape.LayoutOptions = {
  name: "cose",
  animate: true,
  animationDuration: 500,
  nodeRepulsion: () => 8000,
  idealEdgeLength: () => 120,
  padding: 40,
};

const STYLESHEET: cytoscape.StylesheetCSS[] = [
  {
    selector: "node",
    css: {
      label: "data(label)",
      "text-valign": "center",
      "text-halign": "center",
      "background-color": "#3b82f6",
      color: "#ffffff",
      "font-size": "12px",
      "font-weight": 600,
      width: 60,
      height: 60,
      "text-wrap": "wrap",
      "text-max-width": "56px",
    },
  },
  {
    selector: "edge",
    css: {
      label: "data(label)",
      "curve-style": "bezier",
      "target-arrow-shape": "triangle",
      "target-arrow-color": "#9ca3af",
      "line-color": "#9ca3af",
      width: 2,
      "font-size": "10px",
      color: "#6b7280",
      "text-rotation": "autorotate",
      "text-margin-y": -10,
    },
  },
  {
    selector: "node:grabbed",
    css: {
      "background-color": "#2563eb",
      "border-width": 2,
      "border-color": "#1d4ed8",
    },
  },
];

export function SchemaPreview({ nodes, edges }: SchemaPreviewProps) {
  const cyRef = useRef<cytoscape.Core | null>(null);

  const elements = [
    ...nodes.map((n) => ({ data: { id: n.id, label: n.label } })),
    ...edges.map((e) => ({
      data: { id: e.id, source: e.source, target: e.target, label: e.label },
    })),
  ];

  const handleCy = useCallback((cy: cytoscape.Core) => {
    cyRef.current = cy;
  }, []);

  useEffect(() => {
    if (cyRef.current) {
      cyRef.current.layout(LAYOUT).run();
    }
  }, [nodes, edges]);

  return (
    <div className="relative h-full w-full">
      <CytoscapeComponent
        elements={elements}
        layout={LAYOUT}
        stylesheet={STYLESHEET}
        cy={handleCy}
        className="h-full w-full"
        userZoomingEnabled={true}
        userPanningEnabled={true}
        boxSelectionEnabled={false}
      />
      <div className="absolute bottom-3 right-3 flex gap-2">
        <button
          onClick={() => cyRef.current?.fit(undefined, 40)}
          className="rounded bg-white/90 px-2 py-1 text-xs text-gray-600 shadow hover:bg-white"
        >
          Fit
        </button>
        <button
          onClick={() => cyRef.current?.layout(LAYOUT).run()}
          className="rounded bg-white/90 px-2 py-1 text-xs text-gray-600 shadow hover:bg-white"
        >
          Re-layout
        </button>
      </div>
    </div>
  );
}
