# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import logging
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from nx_neptune_proxy.auth import get_token, log_token_notice, require_token
from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.routers.graph import router as graph_router
from nx_neptune_proxy.routers.metadata import router as metadata_router
from nx_neptune_proxy.utils.sanitize import sanitize_error_message

from nx_neptune_proxy.routers.projection import router as projection_router
from nx_neptune_proxy.routers.project import router as project_router
from nx_neptune_proxy.services.db import init_db
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.project_deletion import delete_project

from starlette.middleware.trustedhost import TrustedHostMiddleware

settings = Settings.from_env()
settings.validate_host()

# --- Database ---
init_db()

# --- Structured logging ---

logging.basicConfig(
    level=settings.log_level,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("nx_neptune_proxy")

# --- Proxy access token (per-run bearer token, distinct from AWS credentials) ---
# Printed directly to stdout (not through the structured logger) so it never
# ends up in aggregated/forwarded logs. Only a caller with access to this
# process's own console output, or the bundled UI (which receives it via
# index.html), can read it.
print(f"\nProxy access token for this run: {get_token()}\n")
log_token_notice()


# --- App ---

app = FastAPI(title="nx-neptune-proxy", version="0.1.0", docs_url=None, redoc_url=None)

# --- TrustedHost middleware (blocks DNS rebinding) ---


app.add_middleware(
    TrustedHostMiddleware, allowed_hosts=["localhost", "127.0.0.1", "[::1]"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
)


# --- Origin validation middleware (server-side, defense-in-depth) ---

_ALLOWED_ORIGIN_HOSTS = {"localhost", "127.0.0.1", "[::1]"}


@app.middleware("http")
async def origin_validation(request: Request, call_next):
    """Reject requests with an Origin header not on the allowlist.

    Browsers always send a truthful Origin header on cross-origin requests.
    This blocks DNS rebinding even if TrustedHost is bypassed.
    """
    origin = request.headers.get("origin")
    if origin:
        # Parse origin to extract host (e.g. "http://localhost:8080" -> "localhost")
        try:
            parsed = urlparse(origin)
            host = parsed.hostname or ""
        except Exception:
            host = ""
        if host not in _ALLOWED_ORIGIN_HOSTS:
            return JSONResponse(
                status_code=403,
                content={
                    "error": "origin_rejected",
                    "message": f"Origin '{origin}' is not allowed",
                },
            )
    return await call_next(request)


# --- CSRF protection middleware ---

_CSRF_SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


@app.middleware("http")
async def csrf_protection(request: Request, call_next):
    """Require X-Requested-With header on state-changing requests.

    This forces browsers to send a CORS preflight, which blocks cross-origin
    requests from malicious tabs that don't pass the Origin check.
    """
    if request.method not in _CSRF_SAFE_METHODS:
        if not request.headers.get("x-requested-with"):
            return JSONResponse(
                status_code=403,
                content={
                    "error": "csrf_rejected",
                    "message": "Missing required X-Requested-With header",
                },
            )
    return await call_next(request)


# --- Request logging middleware ---


@app.middleware("http")
async def request_logging(request: Request, call_next):
    request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
    start = time.time()
    response = await call_next(request)
    duration_ms = (time.time() - start) * 1000
    logger.info(
        f"{request.method} {request.url.path} {response.status_code} {duration_ms:.0f}ms request_id={request_id}"
    )
    response.headers["x-request-id"] = request_id
    return response


# --- Error handlers ---

_AWS_STATUS_MAP = {
    "AccessDeniedException": 403,
    "UnauthorizedAccess": 403,
    "ResourceNotFoundException": 404,
    "MetadataException": 400,
    "InvalidRequestException": 400,
    "ThrottlingException": 503,
}


@app.exception_handler(ClientError)
async def aws_exception_handler(request: Request, exc: ClientError):
    code = exc.response["Error"]["Code"]
    message = exc.response["Error"]["Message"]
    status = _AWS_STATUS_MAP.get(code, 502)
    logger.warning(f"AWS {code} on {request.method} {request.url.path}: {message}")
    return JSONResponse(
        status_code=status,
        content={"error": code, "message": sanitize_error_message(message)},
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled error on {request.method} {request.url.path}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_server_error",
            "message": "An unexpected error occurred",
        },
    )


# --- Health ---


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/api/v0/info", summary="Service info")
def info():
    return {"name": "nx-neptune-proxy", "version": "0.1.0"}


# --- Routers (all /api/* routes require the per-run bearer token) ---

app.include_router(metadata_router, dependencies=[Depends(require_token)])
app.include_router(projection_router, dependencies=[Depends(require_token)])
app.include_router(project_router, dependencies=[Depends(require_token)])
app.include_router(graph_router, dependencies=[Depends(require_token)])


# --- Startup: resume stuck deletions ---


@app.on_event("startup")
async def resume_pending_deletions():
    for p in project_store.list():
        if p.status == "deleting":
            logger.info(f"Resuming deletion of project {p.id} ({p.name})")
            asyncio.create_task(delete_project(p.id))


# --- Static UI (must be last — catch-all) ---

UI_DIR = Path(__file__).parent.parent.parent / "ui"
if not UI_DIR.exists():
    UI_DIR = Path("/app/proxy/ui")
if UI_DIR.exists():
    app.mount("/assets", StaticFiles(directory=UI_DIR / "assets"), name="assets")

    _INDEX_TOKEN_PLACEHOLDER = "</head>"

    def _serve_index() -> HTMLResponse:
        """Serve index.html with this run's token injected for the bundled UI.

        The token is embedded in the HTML response body, which is only
        readable by same-origin JavaScript (browser Same-Origin Policy) —
        a cross-origin or rebound page cannot read it even if it can reach
        this endpoint.
        """
        html = (UI_DIR / "index.html").read_text()
        snippet = f'<meta name="nx-neptune-proxy-token" content="{get_token()}">'
        if _INDEX_TOKEN_PLACEHOLDER in html:
            html = html.replace(
                _INDEX_TOKEN_PLACEHOLDER, f"{snippet}{_INDEX_TOKEN_PLACEHOLDER}", 1
            )
        else:
            html = snippet + html
        return HTMLResponse(html)

    @app.get("/{path:path}")
    async def spa_fallback(path: str):
        if path.startswith("api/"):
            raise HTTPException(status_code=404)

        # Control 1: Input sanitization — reject any path containing traversal
        if ".." in path:
            raise HTTPException(status_code=403)

        # Control 2: Output validation — verify resolved path is inside ui_root
        ui_root = UI_DIR.resolve()
        try:
            normalized_relative = Path("/", path).resolve().relative_to("/")
        except ValueError:
            raise HTTPException(status_code=403)

        # Stops symlinks that resolve outside ui_root
        file_path = (ui_root / normalized_relative).resolve()
        try:
            file_path.relative_to(ui_root)
        except ValueError:
            raise HTTPException(status_code=403)

        if file_path.is_file() and file_path != (ui_root / "index.html"):
            return FileResponse(file_path)
        return _serve_index()
