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
    async def test_ipv6_loopback_host_incorrectly_rejected(self, client):
        """Known Starlette limitation, not a regression in this change.

        TrustedHostMiddleware parses Host via `.split(":")[0]`, which turns
        any IPv6 literal like "[::1]:8080" into "[" — never a value that can
        match an allowlist entry. A legitimate IPv6 loopback client is
        rejected exactly like an untrusted one, purely due to this parsing
        bug (confirmed present in Starlette's latest release as of this
        writing). This test documents and pins down that behavior so a
        future Starlette upgrade that happens to fix it doesn't go
        unnoticed.
        """
        resp = await client.get("/health", headers={"Host": "[::1]:8080"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_random_ipv6_host_rejected(self, client):
        """A non-loopback IPv6 literal is correctly rejected too — though
        for this middleware, every IPv6 literal is rejected regardless of
        whether it's trusted, per the limitation above."""
        resp = await client.get("/health", headers={"Host": "[2001:db8::1]:8080"})
        assert resp.status_code == 400
