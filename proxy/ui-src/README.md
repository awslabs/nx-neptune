# nx-neptune Proxy UI

React frontend for the nx-neptune proxy server.

## Prerequisites

- Node.js 18+
- Python 3.11+ (for the backend)

## Quick start

```bash
# 1. Install backend
cd proxy && make install

# 2. Build the UI
make ui

# 3. Run everything on :8080
make dev
```

Open http://localhost:8080

## Development with hot-reload

If you're actively editing the frontend, use two terminals:

```bash
# Terminal 1 — backend on :8080
cd proxy && make dev

# Terminal 2 — Vite dev server on :5173 (proxies /api to :8080)
cd proxy/ui-src && npm run dev
```

Open http://localhost:5173 — changes to React code update instantly.

## Build for production

```bash
cd proxy && make ui
```

This runs `npm install && npm run build` in `ui-src/` and outputs static files to `proxy/ui/`. The FastAPI server serves them automatically.

## Project structure

```
ui-src/
├── src/
│   ├── api/index.ts        # Typed API client
│   ├── components/
│   │   ├── Sidebar.tsx     # Navigation
│   │   └── ui.tsx          # Button, Select, ProgressBar, Card
│   ├── pages/
│   │   ├── Import.tsx      # Import wizard
│   │   ├── Sessions.tsx    # Session list
│   │   └── Graphs.tsx      # Neptune graphs
│   ├── App.tsx             # Layout + routes
│   ├── main.tsx            # Entry point
│   └── index.css           # Tailwind
├── package.json
├── vite.config.ts          # Build config + dev proxy
├── tailwind.config.ts
└── tsconfig.json
```

## Stack

- React 19
- Tailwind CSS 3
- Lucide icons
- React Router 7
- Vite 6
