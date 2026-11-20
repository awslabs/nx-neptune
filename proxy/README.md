# nx-neptune-proxy

A local FastAPI server with React UI for managing Neptune Analytics graph projections from data lake sources.

For a setup guide and an end-to-end walkthrough, see the [Proxy UI docs](https://awslabs.github.io/nx-neptune/proxy/setup/).

## Quick Start

Prerequisites: Python 3.11 or later, Node.js and npm (to build the UI), and AWS
credentials with a region configured (see the security note under Docker Compose). The
credentials need the IAM permissions listed in
[Prerequisites](https://awslabs.github.io/nx-neptune/getting-started/prerequisites/)
for Athena, S3 and Neptune Analytics.

> **Warning:** `make dev`, `make run`, `make up` and `docker-compose.yml` set
> `NETWORKX_PUBLIC_CONNECTIVITY=true` and `NETWORKX_DELETION_PROTECTION=false`, so the
> graphs they create have a **public endpoint** and **no deletion protection**. This
> lets you reach them from your machine and delete them when you're done. Use them only
> with test data in a development account. If you run `python -m nx_neptune_proxy`
> yourself without setting `NETWORKX_*`, you get the secure defaults (see
> [Graph creation defaults](../README.md#graph-creation-defaults)).

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

Open that URL to use the UI. `make dev` opens it in your browser on startup
(`OPEN_BROWSER=true`); other launch paths only print it. The token is generated each time the server starts and
is required for every API call, so `http://127.0.0.1:8080` without it loads the page
but the API calls fail. Pages opened on `localhost` redirect to `127.0.0.1`. The UI removes the token from the address bar and stores it in
the browser's local storage for `http://127.0.0.1:8080`, so page refreshes and new tabs
keep working. After a server restart (including an auto-reload by `make dev`), open the
new launch URL from the server output. When running in a
container, the URL is in the container output (`make logs`).

`make dev` binds to `127.0.0.1:8080`.

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

### Stable token for local development (`NX_DEBUG`)

Because the token is regenerated on every process start, the launch URL changes each time you restart the server. For local development you can set `NX_DEBUG` to reuse a fixed, well-known token instead:

```bash
NX_DEBUG=1 make dev
```

Accepted truthy values (case-insensitive): `1`, `true`, `yes`, `on`. When enabled, the token is always:

```
http://127.0.0.1:8080/?token=nx-debug-local-token
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

With finch, compose runs inside the finch VM and only sees the AWS credential variables
from your shell. Set `AWS_DEFAULT_REGION` and `GE_PORT` in `proxy/.env` (gitignored)
instead of exporting them.

The containers only receive `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
`AWS_SESSION_TOKEN`, so with `aws sso login` or a profile, export them first:

```bash
aws sso login --profile <profile>
eval "$(aws configure export-credentials --profile <profile> --format env)"
```

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
| `HOST` | `127.0.0.1` | Bind address for `python -m nx_neptune_proxy`. Non-loopback addresses are refused unless `ALLOW_NON_LOOPBACK_BIND=1` |
| `ALLOW_NON_LOOPBACK_BIND` | *(unset)* | Set to `1` to allow a non-loopback `HOST`. The container image sets it, and publishes the port on `127.0.0.1` only |
| `PORT` | `8080` | Server port for `python -m nx_neptune_proxy` (`make dev` always uses `8080`) |
| `OPEN_BROWSER` | *(unset)* | Set to `true` to open the launch URL in your browser on startup. `make dev` sets it. Has no effect in a container |
| `CORS_ALLOWED_ORIGINS` | *(none)* | Comma-separated list of allowed browser origins (full `scheme://host[:port]`, e.g. `https://app.example.com`). This is the single source of truth for origins: it drives the CORS response headers, the server-side Origin check, and the hostnames trusted in the `Host` header. Loopback origins are always allowed, so no configuration is needed for local/Docker use. |
| `LOG_LEVEL` | `info` | Logging level |
| `GE_PORT` | `443` | Host port for Graph Explorer (`make up`, `make up-ge`), published on `127.0.0.1`. `make up` builds the UI with the matching link; with `make dev`, build it with `VITE_GRAPH_EXPLORER_URL=https://localhost:<port>/explorer make ui` |
| `NX_NEPTUNE_DB_PATH` | `~/.nx-neptune/proxy.db` | SQLite database path |
| `NX_DEBUG` | *(unset)* | When truthy (`1`/`true`/`yes`/`on`), use a fixed access token (`nx-debug-local-token`) instead of a random per-run one, giving a stable launch URL. **Local development only** — see [Access token](#access-token). |
| `BEDROCK_MODEL` | `us.anthropic.claude-sonnet-4-5-20250929-v1:0` | Default Bedrock model ID (cross-region inference profile) for the AI assistant agent pipeline. Overridable per request via the chat panel's model selector. |
| `BEDROCK_REGION` | *(falls back to `AWS_DEFAULT_REGION`/`AWS_REGION`)* | AWS region for Bedrock calls, if Bedrock runs in a different region than Athena/Neptune. |
| `BEDROCK_AGENT_ROLE_ARN` | *(unset → uses the proxy's process role)* | Optional scoped, **read-only** IAM role for the AI assistant's agent path (spec §9.11). When set, the proxy assumes it via STS and runs the agent's Bedrock/Athena calls under the resulting short-lived credentials, so "the agent can only read/propose" holds at the AWS layer. Set this in any environment where the proxy's own role carries write/destructive permissions. |

## IAM Permissions

Beyond the Athena / S3 / Neptune Analytics permissions used by the core proxy, the AI assistant (spec §9) needs:

- **Bedrock** — `bedrock:InvokeModel`, `bedrock:InvokeModelWithResponseStream` on the configured model(s).
- **Athena** — schema discovery reuses the existing Athena **metadata** calls (`GetTableMetadata`, `ListTableMetadata`) already required for the metadata endpoints; the optional data-sampling tool runs `SELECT ... LIMIT 10`, covered by existing Athena query + S3 result permissions.
- **Neptune Analytics** — `neptune-graph:ExecuteQuery` for running the generated post-import openCypher queries/mutations.

When splitting the agent path onto a scoped role via `BEDROCK_AGENT_ROLE_ARN`, the proxy's own role additionally needs `sts:AssumeRole` on that role, and the scoped role grants only the read-only subset above (`bedrock:InvokeModel`, Athena metadata + `StartQueryExecution`/`GetQueryResults` + the sampling S3 output location, and `neptune-graph:ReadDataViaQuery`) — no write, delete, or mutation permissions.
