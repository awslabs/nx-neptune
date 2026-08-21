# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""TrustedHostMiddleware — blocks DNS rebinding via the Host header."""

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


class TestTrustedHostMiddleware:
    @pytest.mark.asyncio
    async def test_localhost_host_allowed(self, client):
        resp = await client.get("/health", headers={"Host": "localhost:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_127_0_0_1_host_allowed(self, client):
        resp = await client.get("/health", headers={"Host": "127.0.0.1:8080"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_untrusted_host_rejected(self, client):
        """Simulated DNS-rebinding: the request lands on this server, but
        claims to be addressed to an attacker-controlled hostname."""
        resp = await client.get("/health", headers={"Host": "attacker-controlled.com"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_ipv6_loopback_host_rejected(self, client):
        """IPv6 is deliberately excluded from allowed_hosts (see app.py),
        not just accidentally unmatched. Outcome is unchanged either way."""
        resp = await client.get("/health", headers={"Host": "[::1]:8080"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_random_ipv6_host_rejected(self, client):
        """A non-loopback IPv6 literal is rejected too — every IPv6 literal
        is rejected regardless of whether it's trusted, per the deliberate
        exclusion above."""
        resp = await client.get("/health", headers={"Host": "[2001:db8::1]:8080"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_ipv6_base_url_rejected_without_explicit_host_header(self):
        """Same rejection as above, but via a client whose base_url is
        itself IPv6 — no Host header is set explicitly here; httpx derives
        Host: [::1]:8080 from base_url on its own, same as a real client
        addressing this server by an IPv6 literal would."""
        transport = ASGITransport(app=app)
        ipv6_client = AsyncClient(
            transport=transport,
            base_url="http://[::1]:8080",
            headers={"X-Requested-With": "nx-neptune"},
        )
        resp = await ipv6_client.get("/health")
        assert resp.status_code == 400


class TestIpv6ExclusionScope:
    """The IPv6 exclusion above is specific to TrustedHostMiddleware's
    allowed_hosts, not to settings.trusted_hosts itself — origin_validation
    (which parses Origin via urlparse, unaffected by Starlette's bug) still
    supports IPv6 loopback."""

    @pytest.mark.asyncio
    async def test_origin_validation_still_allows_ipv6_loopback(self, client):
        # TrustedHostMiddleware still evaluates the Host header on this
        # request. It passes because the value is trusted and not an
        # IPv6 literal (localhost). This test isolates the Origin check
        # by controlling for Host, not by bypassing the Host check.
        resp = await client.get(
            "/health",
            headers={"Host": "localhost", "Origin": "http://[::1]:8080"},
        )
        assert resp.status_code == 200
