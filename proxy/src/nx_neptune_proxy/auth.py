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
_TOKEN = secrets.token_urlsafe(32)


def get_token() -> str:
    """Return this run's proxy access token."""
    return _TOKEN


def log_token_notice() -> None:
    """Log a startup notice without leaking the token to log aggregation.

    The token itself is only surfaced to same-origin callers: the bundled
    UI (injected into index.html) or a caller reading it from this
    process's own stdout on the machine that started it.
    """
    logger.info(
        "Proxy access token generated for this run. The bundled UI receives it "
        "automatically; other API clients must read it from this process's "
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
    if not secrets.compare_digest(presented, _TOKEN):
        raise HTTPException(status_code=401, detail="Invalid access token")
