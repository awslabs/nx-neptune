# nx-neptune-proxy

A local FastAPI server with React UI for managing Neptune Analytics graph projections from data lake sources.

## Quick Start

1. Install dependencies
```
pip install -e ".[dev]"
```

2. Build the UI (required before running the server — see note below)
```
make ui
```

> **Important:** `make dev` serves the pre-built UI from the `ui/` directory but does **not** build it. You must run `make ui` first, and re-run it whenever you change anything under `ui-src/`. Skipping this serves a stale bundle — a common symptom is `401 Unauthorized` on every `/api/v0/*` call, because an outdated bundle doesn't attach the per-run access token. (`make up` and `make test` build the UI for you; `make dev` does not.)

3. Run the server
```
# run localhost
make dev

# Or with Docker (builds the UI automatically)
make up
```

The UI is available at `http://localhost:8080/?token=<token>`.

> See [Access token](#access-token) below. Opening `http://localhost:8080` without the token will load the UI but every `/api/v0/*` call will fail with `401 Unauthorized`.

### Access token

On startup the proxy prints a launch URL containing a per-run access token:

```
http://127.0.0.1:8080/?token=<token>
```

Open that exact URL. The UI reads the token once, strips it from the address bar, and keeps it in memory only. A full page reload (or reusing an old tab) drops the token and causes `401`s — reopen the current launch URL in a new tab instead.

#### Stable token for local development (`NX_DEBUG`)

Because the token is regenerated on every process start, the launch URL changes each time you restart the server. For local development you can set `NX_DEBUG` to reuse a fixed, well-known token instead:

```bash
NX_DEBUG=1 make dev
```

Accepted truthy values (case-insensitive): `1`, `true`, `yes`, `on`. When enabled, the token is always:

```
http://localhost:8080/?token=nx-debug-local-token
```

This lets you bookmark the launch URL and survive restarts without copying a new token each time.

> ⚠️ **Local development only.** `NX_DEBUG` swaps the random per-run token for a hardcoded string, so anyone who knows it can call the API. The server logs a warning at startup when it's enabled. Never set `NX_DEBUG` in shared or production environments.

## Architecture

- **Backend**: FastAPI (Python) — manages projections, executes Athena queries, orchestrates Neptune graph lifecycle
- **Frontend**: React + TypeScript + Vite — SPA served by FastAPI in production
- **Storage**: SQLite (`~/.nx-neptune/proxy.db`) — stores project and projection metadata
- **Graph Explorer**: Optional sidecar container for visual graph exploration

## Docker Compose

The `docker-compose.yml` runs both the proxy and Graph Explorer locally.

### ⚠️ Security: AWS Credentials

AWS credentials are passed via environment variables for **local development only**. Never use long-lived credentials (IAM user access keys). Always use short-lived session tokens from:

- `aws sso login`
- `aws sts assume-role`
- Instance profile (when running on EC2/ECS)

Do not deploy this docker-compose configuration to shared or production environments.

### Verify your AWS setup

Before running the proxy, sanity-check that your credentials **and region** are set. `aws configure list` shows the effective access key, secret key, and region along with where each value came from (env var vs. profile):

```bash
aws configure list          # shows access_key, secret_key, region + their source
aws configure get region    # prints just the resolved region
aws sts get-caller-identity # confirms the credentials are valid / not expired
```

A missing region is a common cause of failures (e.g. `502 Bad Gateway` on `/api/v0/metadata/s3/buckets`). Set it via `AWS_DEFAULT_REGION` (the proxy reads this env var, which overrides the profile) or in your profile with `aws configure set region us-west-2`.

## Development

```bash
# Run backend tests (builds the UI first)
make test

# Build the UI into ui/ (run after any ui-src/ change)
make ui

# Run UI dev server with hot reload (Vite, separate port)
cd ui-src && npm run dev
```

When using the Vite dev server (`npm run dev`), open the proxy launch URL with the `?token=` query string against the Vite port so the UI still picks up the access token; Vite proxies `/api` calls through to the FastAPI server on port 8080.

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | Server port |
| `CORS_ALLOWED_ORIGINS` | *(none)* | Comma-separated list of allowed browser origins (full `scheme://host[:port]`, e.g. `https://app.example.com`). This is the single source of truth for origins: it drives the CORS response headers, the server-side Origin check, and the hostnames trusted in the `Host` header. Loopback origins are always allowed, so no configuration is needed for local/Docker use. |
| `LOG_LEVEL` | `info` | Logging level |
| `NX_NEPTUNE_DB_PATH` | `~/.nx-neptune/proxy.db` | SQLite database path |
| `NX_DEBUG` | *(unset)* | When truthy (`1`/`true`/`yes`/`on`), use a fixed access token (`nx-debug-local-token`) instead of a random per-run one, giving a stable launch URL. **Local development only** — see [Access token](#access-token). |
