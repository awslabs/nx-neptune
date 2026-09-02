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
    @pytest.mark.parametrize(
        "origin",
        [
            pytest.param("http://localhost:8080", id="localhost"),
            pytest.param("http://127.0.0.1:8080", id="127_0_0_1"),
            # urlparse correctly strips IPv6 brackets before comparison, unlike
            # Starlette's TrustedHostMiddleware, which cannot parse them at all.
            pytest.param("http://[::1]:8080", id="ipv6_loopback"),
        ],
    )
    async def test_loopback_origin_allowed(self, client, origin):
        resp = await client.get("/health", headers={"Origin": origin})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "origin",
        [
            pytest.param("http://evil.com", id="evil_host"),
            pytest.param("not-a-valid-url", id="malformed"),
        ],
    )
    async def test_untrusted_origin_rejected(self, client, origin):
        resp = await client.get("/health", headers={"Origin": origin})
        assert resp.status_code == 403
        assert resp.json()["error"] == "origin_rejected"

    @pytest.mark.asyncio
    async def test_no_origin_allowed(self, client):
        """Requests without an Origin header (e.g. non-browser callers) are
        not subject to this check."""
        resp = await client.get("/health")
        assert resp.status_code == 200


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
    @pytest.mark.parametrize(
        "origin, expected_status",
        [
            pytest.param(
                "https://app.example.com:8443", 200, id="configured_full_origin"
            ),
            # A trusted hostname on a non-configured port is rejected — matching
            # is on the full origin, not the hostname alone.
            pytest.param(
                "https://app.example.com:9999", 403, id="same_host_wrong_port"
            ),
            pytest.param(
                "http://app.example.com:8443", 403, id="same_host_wrong_scheme"
            ),
        ],
    )
    async def test_full_origin_matching(
        self, client_with_allowed_origin, origin, expected_status
    ):
        resp = await client_with_allowed_origin.get(
            "/health", headers={"Origin": origin}
        )
        assert resp.status_code == expected_status
