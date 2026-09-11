# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Server-side Origin header validation (origin_validation middleware)."""

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport,
        base_url="http://localhost",
        headers={"X-Requested-With": "nx-neptune"},
    )


class TestOriginValidation:
    @pytest.mark.asyncio
    async def test_evil_origin_rejected(self, client):
        resp = await client.get("/health", headers={"Origin": "http://evil.com"})
        assert resp.status_code == 403
        assert resp.json()["error"] == "origin_rejected"

    @pytest.mark.asyncio
    async def test_localhost_origin_allowed(self, client):
        resp = await client.get("/health", headers={"Origin": "http://localhost:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_127_0_0_1_origin_allowed(self, client):
        resp = await client.get("/health", headers={"Origin": "http://127.0.0.1:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_ipv6_loopback_origin_allowed(self, client):
        """urlparse correctly strips IPv6 brackets before comparison, unlike
        Starlette's TrustedHostMiddleware, which cannot parse them at all."""
        resp = await client.get("/health", headers={"Origin": "http://[::1]:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_no_origin_allowed(self, client):
        """Requests without an Origin header (e.g. non-browser callers) are
        not subject to this check."""
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_malformed_origin_rejected(self, client):
        resp = await client.get("/health", headers={"Origin": "not-a-valid-url"})
        assert resp.status_code == 403


class TestOriginValidationFullOrigin:
    """origin_validation matches the full origin (scheme+host+port) against
    the configured allowed_origins, not just the hostname."""

    @pytest.fixture
    def client_with_allowed_origin(self, monkeypatch):
        import nx_neptune_proxy.app as app_module
        from nx_neptune_proxy.config import Settings

        patched = Settings(allowed_origins=["https://app.example.com:8443"])
        monkeypatch.setattr(app_module, "settings", patched)
        transport = ASGITransport(app=app_module.app)
        return AsyncClient(
            transport=transport,
            base_url="http://localhost",
            headers={"X-Requested-With": "nx-neptune"},
        )

    @pytest.mark.asyncio
    async def test_configured_full_origin_allowed(self, client_with_allowed_origin):
        resp = await client_with_allowed_origin.get(
            "/health", headers={"Origin": "https://app.example.com:8443"}
        )
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_same_host_wrong_port_rejected(self, client_with_allowed_origin):
        """A trusted hostname on a non-configured port is rejected — matching
        is on the full origin, not the hostname alone."""
        resp = await client_with_allowed_origin.get(
            "/health", headers={"Origin": "https://app.example.com:9999"}
        )
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_same_host_wrong_scheme_rejected(self, client_with_allowed_origin):
        resp = await client_with_allowed_origin.get(
            "/health", headers={"Origin": "http://app.example.com:8443"}
        )
        assert resp.status_code == 403
