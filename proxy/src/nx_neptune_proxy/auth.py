# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-run bearer token for the proxy's own HTTP API.

Gates who may call this proxy's /api/* routes. A fresh, random token is
generated once per process start and kept in memory only — it is never
written to disk or logged in full.
"""

import logging
import os
import secrets

from fastapi import Header, HTTPException

logger = logging.getLogger("nx_neptune_proxy")

# Fixed, well-known token used only when NX_DEBUG is enabled. This is NOT a
# secret — it exists so a developer can bookmark a stable launch URL across
# restarts instead of copying a fresh token each run. Never enable NX_DEBUG
# outside local development.
_DEBUG_TOKEN = "nx-debug-local-token"  # noqa: S105 (not a real credential)


def _debug_enabled() -> bool:
    """True when NX_DEBUG requests the fixed local token."""
    return os.environ.get("NX_DEBUG", "").strip().lower() in ("1", "true", "yes", "on")


# Generated once at import time, i.e. once per process ("per run").
# Single-worker only: with uvicorn --workers N each worker would generate a
# different token. The shipped launch paths run one worker.
# When NX_DEBUG is set, use a fixed token instead so the launch URL is stable.
_TOKEN = _DEBUG_TOKEN if _debug_enabled() else secrets.token_urlsafe(32)


def get_token() -> str:
    """Return this run's proxy access token."""
    return _TOKEN


def log_token_notice() -> None:
    """Log a startup notice without leaking the token to log aggregation.

    The token itself is surfaced only via the launch URL printed to this
    process's stdout. The bundled UI reads it from that URL's query string;
    other API clients read it from the startup output. It is never embedded
    in an HTTP response body.
    """
    if _debug_enabled():
        logger.warning(
            "NX_DEBUG is enabled: using a FIXED, well-known access token. This "
            "is insecure and intended for local development only. Do not enable "
            "NX_DEBUG in shared or production environments."
        )
        return
    logger.info(
        "Proxy access token generated for this run. Open the launch URL "
        "printed to stdout; other API clients must read the token from that "
        "startup output."
    )


async def require_token(authorization: str | None = Header(default=None)) -> None:
    """FastAPI dependency: enforce `Authorization: Bearer <token>` on a route.

    Uses a constant-time comparison to avoid leaking the token via timing.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=401, detail="Missing or invalid Authorization header"
        )

    presented = authorization[len("Bearer ") :]
    # Compare on encoded bytes: compare_digest raises TypeError on non-ASCII
    # str inputs, which would surface as a 500. Bytes never raise, so a
    # non-ASCII (or any wrong) token cleanly fails the comparison -> 401.
    if not secrets.compare_digest(presented.encode("utf-8"), _TOKEN.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid access token")
