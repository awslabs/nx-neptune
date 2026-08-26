# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-run bearer token for the proxy's own HTTP API.

Gates who may call this proxy's /api/* routes. A fresh, random token is
generated once per process start and kept in memory only — it is never
written to disk or logged in full.
"""

import logging
import secrets

from fastapi import Header, HTTPException

logger = logging.getLogger("nx_neptune_proxy")

# Generated once at import time, i.e. once per process ("per run").
# Single-worker only: with uvicorn --workers N each worker would generate a
# different token. The shipped launch paths run one worker.
_TOKEN = secrets.token_urlsafe(32)


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
