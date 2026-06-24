# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import uuid
from pathlib import Path

from botocore.exceptions import ClientError
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.routers.metadata import router as metadata_router
from nx_neptune_proxy.routers.preview import router as preview_router
from nx_neptune_proxy.routers.projection import router as projection_router
from nx_neptune_proxy.routers.workspace import router as workspace_router

settings = Settings.from_env()

# --- Structured logging ---

logging.basicConfig(
    level=settings.log_level,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("nx_neptune_proxy")


# --- App ---

app = FastAPI(title="nx-neptune-proxy", version="0.1.0", docs_url="/docs", redoc_url=None)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


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
        content={"error": code, "message": message},
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception(f"Unhandled error on {request.method} {request.url.path}")
    return JSONResponse(
        status_code=500,
        content={"error": "internal_server_error", "message": "An unexpected error occurred"},
    )


# --- Health ---


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/api/v0/info", summary="Service info")
def info():
    return {"name": "nx-neptune-proxy", "version": "0.1.0"}


# --- Routers ---

app.include_router(metadata_router)
app.include_router(projection_router)
app.include_router(preview_router)
app.include_router(workspace_router)


# --- Static UI (must be last — catch-all) ---

UI_DIR = Path(__file__).parent.parent.parent / "ui"
if not UI_DIR.exists():
    UI_DIR = Path("/app/proxy/ui")
if UI_DIR.exists():
    app.mount("/assets", StaticFiles(directory=UI_DIR / "assets"), name="assets")

    @app.get("/{path:path}")
    async def spa_fallback(path: str):
        from fastapi.responses import FileResponse

        if path.startswith("api/"):
            from fastapi import HTTPException

            raise HTTPException(status_code=404)
        file_path = UI_DIR / path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(UI_DIR / "index.html")
