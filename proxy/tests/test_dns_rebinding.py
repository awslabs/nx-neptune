# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""DNS rebinding protection via TrustedHost + Origin validation."""

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app


class TestTrustedHost:
    """Requests with untrusted Host header must be rejected."""

    @pytest.mark.asyncio
    async def test_evil_host_rejected(self):
        transport = ASGITransport(app=app)
        client = AsyncClient(
            transport=transport,
            base_url="http://evil.com",
            headers={"X-Requested-With": "nx-neptune"},
        )
        resp = await client.get("/health")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_localhost_allowed(self):
        transport = ASGITransport(app=app)
        client = AsyncClient(
            transport=transport,
            base_url="http://localhost",
            headers={"X-Requested-With": "nx-neptune"},
        )
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_127_0_0_1_allowed(self):
        transport = ASGITransport(app=app)
        client = AsyncClient(
            transport=transport,
            base_url="http://127.0.0.1",
            headers={"X-Requested-With": "nx-neptune"},
        )
        resp = await client.get("/health")
        assert resp.status_code == 200


class TestOriginValidation:
    """Requests with disallowed Origin header must be rejected."""

    @pytest.mark.asyncio
    async def test_evil_origin_rejected(self):
        transport = ASGITransport(app=app)
        client = AsyncClient(
            transport=transport,
            base_url="http://localhost",
            headers={"X-Requested-With": "nx-neptune", "Origin": "http://evil.com"},
        )
        resp = await client.get("/health")
        assert resp.status_code == 403
        assert resp.json()["error"] == "origin_rejected"

    @pytest.mark.asyncio
    async def test_localhost_origin_allowed(self):
        transport = ASGITransport(app=app)
        client = AsyncClient(
            transport=transport,
            base_url="http://localhost",
            headers={"X-Requested-With": "nx-neptune", "Origin": "http://localhost:8080"},
        )
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_no_origin_allowed(self):
        """Requests without Origin header (same-origin, curl) must pass."""
        transport = ASGITransport(app=app)
        client = AsyncClient(
            transport=transport,
            base_url="http://localhost",
            headers={"X-Requested-With": "nx-neptune"},
        )
        resp = await client.get("/health")
        assert resp.status_code == 200


class TestDocsDisabled:
    """API docs endpoints must not be accessible."""

    @pytest.mark.asyncio
    async def test_docs_not_found(self):
        transport = ASGITransport(app=app)
        client = AsyncClient(transport=transport, base_url="http://localhost")
        resp = await client.get("/docs")
        # Should not return 200 with Swagger UI
        assert resp.status_code != 200 or "swagger" not in resp.text.lower()

    @pytest.mark.asyncio
    async def test_redoc_not_found(self):
        transport = ASGITransport(app=app)
        client = AsyncClient(transport=transport, base_url="http://localhost")
        resp = await client.get("/redoc")
        assert resp.status_code != 200 or "redoc" not in resp.text.lower()
