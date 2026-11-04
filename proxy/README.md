# nx-neptune-proxy

A local FastAPI server with React UI for managing Neptune Analytics graph projections from data lake sources.

For a setup guide and an end-to-end walkthrough, see the [Proxy UI docs](https://awslabs.github.io/nx-neptune/proxy/setup/).

## Quick Start

Prerequisites: Python 3.11 or later, Node.js and npm (to build the UI), and AWS
credentials with a region configured (see the security note under Docker Compose).

```bash
# From the repository root: install the nx-neptune library from source
pip install -e .

# From proxy/: install the proxy with its test dependencies
cd proxy
make install

# Build the UI into ui/
make ui

# Run the server with auto-reload
make dev
```

On startup the server prints a launch URL with an access token:

```
Open the proxy UI (token valid for this run only):
  http://127.0.0.1:8080/?token=<token>
```

Open that URL to use the UI. The token is generated each time the server starts and
is required for every API call, so `http://localhost:8080` without it loads the page
but the API calls fail. The UI removes the token from the address bar and keeps it in
memory only. After a page refresh or a server restart (including an auto-reload by
`make dev`), open the new launch URL from the server output. When running in a
container, the URL is in the container output (`make logs`).

`make dev` binds to `127.0.0.1:8080`
and sets `NETWORKX_PUBLIC_CONNECTIVITY=true` and `NETWORKX_DELETION_PROTECTION=false`,
so the graphs it creates are reachable from your machine and can be deleted. If you
start the server another way, set these variables yourself (see
[Graph creation defaults](../README.md#graph-creation-defaults)).

To start Graph Explorer next to `make dev`, run `make up-ge` (the Makefile uses finch by
default; pass `RUNTIME=docker` to every target to use Docker). See the
[setup guide](https://awslabs.github.io/nx-neptune/proxy/setup/#start-graph-explorer).

To run the proxy and Graph Explorer together in containers instead, export your AWS
credentials (see [Docker Compose](#docker-compose)) and run:

```bash
make up                  # proxy + Graph Explorer with compose
make up RUNTIME=docker   # the Makefile uses finch by default
```

The image builds the UI itself, so `make ui` isn't needed. Projects are kept in the
`proxy-data` volume across runs (`make down` keeps it). The launch URL with the token
is in `make logs`. When your credentials expire, export new ones and run `make up`
again: this restarts both containers, and the proxy prints a new launch URL.

| Target | Description |
|---|---|
| `make install` | Install the proxy in editable mode with test dependencies |
| `make ui` | Build the UI into `ui/` |
| `make dev` | Run the server with auto-reload on `127.0.0.1:8080` |
| `make build` / `make run` | Build the container image and run it on `127.0.0.1:8080`, passing AWS credentials from the environment |
| `make up` / `make down` / `make logs` | Start, stop and tail the proxy and Graph Explorer with compose |
| `make up-ge` | Start only Graph Explorer |
| `make test` | Build the UI, then run the tests |

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

The containers only receive `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
`AWS_SESSION_TOKEN`, so with `aws sso login` or a profile, export them first:

```bash
aws sso login --profile <profile>
eval "$(aws configure export-credentials --profile <profile> --format env)"
```

Do not deploy this docker-compose configuration to shared or production environments.

## Development

```bash
# Run backend tests
pytest tests/ -q

# Build UI
cd ui-src && npm run build

# Run UI dev server (hot reload)
cd ui-src && npm run dev
```

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `HOST` | `127.0.0.1` | Bind address for `python -m nx_neptune_proxy`. Non-loopback addresses are refused unless `ALLOW_NON_LOOPBACK_BIND=1` |
| `ALLOW_NON_LOOPBACK_BIND` | *(unset)* | Set to `1` to allow a non-loopback `HOST`. The container image sets it, and publishes the port on `127.0.0.1` only |
| `PORT` | `8080` | Server port for `python -m nx_neptune_proxy` (`make dev` always uses `8080`) |
| `CORS_ALLOWED_ORIGINS` | *(none)* | Comma-separated list of allowed browser origins (full `scheme://host[:port]`, e.g. `https://app.example.com`). This is the single source of truth for origins: it drives the CORS response headers, the server-side Origin check, and the hostnames trusted in the `Host` header. Loopback origins are always allowed, so no configuration is needed for local/Docker use. |
| `LOG_LEVEL` | `info` | Logging level |
| `NX_NEPTUNE_DB_PATH` | `~/.nx-neptune/proxy.db` | SQLite database path |
