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

## Browser tests

`ui-src/ui-tests` holds Playwright tests that run the built UI in a real browser with the proxy API faked (`ui-tests/mockApi.ts`), so they need no AWS. A request the mock doesn't handle fails the test.

```bash
cd proxy && make ui-browser
```

## Live browser tests

`ui-tests-live/` holds Playwright tests that click through the real UI against a real proxy and real AWS. They prove the product works end to end, which the mocked tests above cannot. They **create and delete real Neptune Analytics graphs**, so they run on demand and never in CI.

Test cases:

| Test case | What it does |
|---|---|
| Import into a graph session | Creates a project, imports your Athena data into a 4 NCU graph session, waits for "Graph ready", checks the graph is `AVAILABLE`, deletes it from the UI. |
| Import into a provisioned graph | The same journey with a 16 NCU provisioned graph. This one is slower. |

Every step asserts what the user sees. The API is never called to help a step along.

**Set up**

```bash
cd proxy/ui-src/ui-tests-live
cp .env.example .env.local        # git-ignored: your bucket and database names stay local
mkdir data.local                  # git-ignored: your real queries
cp data.example/import-nodes.sql data.local/   # then edit it, and import-edges.sql if you need edges
```

Fill in `.env.local`. Each test case has its own section, and its keys start with `E2E_<CASE>_`. Values already exported in your shell win over the file. A test case with missing keys is skipped.

AWS credentials and region come from your shell, as for the proxy itself. The shell also needs the Python environment that has `nx_neptune_proxy` installed, or set `E2E_PYTHON`.

**Run**

```bash
cd proxy
make ui-browser-live
npx playwright test -c playwright.live.config.ts -g "Graph sessions"   # from ui-src: one case only
```

**What to know**
- The tests start their own proxy on `127.0.0.1:18080`, so a dev proxy on 8080 keeps working. They use a temporary database, so your saved projects are not touched. Set `E2E_PORT` to change the port.
- The test proxy only manages graphs named `nxp-e2e-*`. Public connectivity is on and deletion protection is off, as for `make dev`, so it can reach and delete its graphs.
- One graph exists at a time. A graph a failed run leaves behind is deleted when the run ends. If cleanup cannot finish, it prints the run id to look for in the graph names.
- Nothing is recorded: no screenshots, traces or videos, and the proxy token and your resource names are never printed.
- The tests wait up to 15 minutes for an import, so a run can take a while and costs real money.

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
│   │   ├── Projections.tsx  # Projection list
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
