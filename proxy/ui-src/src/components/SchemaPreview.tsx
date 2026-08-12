import { useRef, useEffect, useCallback, useMemo } from "react";
import CytoscapeComponent from "react-cytoscapejs";
import cytoscape from "cytoscape";

/** Simple string hash → stable hue per label */
function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = str.charCodeAt(i) + ((hash << 5) - hash);
  }
  return Math.abs(hash);
}

function labelToColor(label: string): string {
  // Golden angle for hue spread, convert HSL to hex for canvas compatibility
  const hue = (hashString(label) * 137.508) % 360;
  const s = 0.55, l = 0.45;
  const a = s * Math.min(l, 1 - l);
  const f = (n: number) => {
    const k = (n + hue / 30) % 12;
    const color = l - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
    return Math.round(255 * color).toString(16).padStart(2, "0");
  };
  return `#${f(0)}${f(8)}${f(4)}`;
}

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

const STYLESHEET: cytoscape.StylesheetStyle[] = [
  {
    selector: "node",
    style: {
      "label": "data(label)",
      "text-valign": "center",
      "text-halign": "center",
      "background-color": "data(color)" as any,
      "color": "#ffffff",
      "font-size": "12px",
      "font-weight": 600,
      "width": 60,
      "height": 60,
      "text-wrap": "wrap",
      "text-max-width": "56px",
    },
  },
  {
    selector: "edge",
    style: {
      "label": "data(label)",
      "curve-style": "bezier",
      "target-arrow-shape": "triangle",
      "target-arrow-color": "#9ca3af",
      "line-color": "#9ca3af",
      "width": 2,
      "font-size": "10px",
      "color": "#6b7280",
      "text-rotation": "autorotate",
      "text-margin-y": -10,
    },
  },
  {
    selector: "node:grabbed",
    style: {
      "border-width": 2,
      "border-color": "#1d4ed8",
    },
  },
];

export function SchemaPreview({ nodes, edges }: SchemaPreviewProps) {
  const cyRef = useRef<cytoscape.Core | null>(null);

  const elements = useMemo(() => [
    ...nodes.map((n) => ({ data: { id: n.id, label: n.label, color: labelToColor(n.label) } })),
    ...edges.map((e) => ({
      data: { id: e.id, source: e.source, target: e.target, label: e.label },
    })),
  ], [nodes, edges]);

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
