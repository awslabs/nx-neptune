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
| Import into a graph session | Creates a project, imports your Athena data into a 4 NCU graph session, waits for "Graph ready", checks the projection's import status is `complete` (an `AVAILABLE` graph alone does not prove the import worked), checks the graph is `AVAILABLE`, deletes it from the UI. |
| Import into a provisioned graph | The same journey with a 16 NCU provisioned graph. This one is slower. |

Every step asserts what the user sees. The API is never called to help a step along.

**Set up**

Keep your settings in a file outside the repository, so no bucket, database or query is ever in the project folder. Copy `ui-tests-live/.env.example` there, fill it in, and point the tests at it:

```bash
cp proxy/ui-src/ui-tests-live/.env.example /path/to/e2e.env
chmod 600 /path/to/e2e.env          # then edit it
export E2E_ENV_FILE=/path/to/e2e.env
```

`E2E_ENV_FILE` is the only place the tests read settings from. It is not searched for, and if it is set to a missing file the run fails. Values already exported in your shell win over the file (an empty value counts as unset), so don't leave old `E2E_*` exports around. A secrets tool can inject the same variables instead (for example `op run -- make ui-browser-live`), and then no file is needed.

In the file, each test case has its own section, and its keys start with `E2E_<CASE>_`. The bucket is its name (`s3://` and a trailing `/` are accepted and removed). The Athena queries are plain strings, one line each, in backticks, so they can contain both `'` and `"`. Don't put `--` comments in them: on one line they would comment out the rest of the query. Backticks work only in this file. In a shell, `export` needs different quoting. A test case with missing keys is skipped.

AWS credentials and region come from your shell, as for the proxy itself. The shell also needs the Python environment that has `nx_neptune_proxy` installed, or set `E2E_PYTHON`.

**Run**

```bash
cd proxy
make ui-browser-live
make ui-browser-live-headed                       # same, with a visible browser window
make ui-browser-live ARGS='-g "Graph sessions"'   # one test case only (ARGS also works with -headed)
```

**What to know**
- The tests start their own proxy on `127.0.0.1:18080`, so a dev proxy on 8080 keeps working. They use a temporary database, so your saved projects are not touched. Set `E2E_PORT` to change the port.
- The test proxy only manages graphs named `nxp-e2e-*`. Public connectivity is on and deletion protection is off, as for `make dev`, so it can reach and delete its graphs.
- One graph exists at a time. A graph a failed run leaves behind is deleted when the run ends. If cleanup cannot finish, it prints the run id to look for in the graph names.
- Nothing is recorded: no screenshots, traces or videos, and the proxy token and your resource names are never printed.
- The tests wait up to 15 minutes for an import, so a run can take a while and costs real money. A run stops early if the Import page shows a red error box or a ✗ check list.
- With `--headed`, leave the browser window alone. Navigating away or closing it fails the test. The graph is still cleaned up when the run ends.
- If the test proxy cannot start (for example, port 18080 is busy), the run fails with a message instead of choosing another port.

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
