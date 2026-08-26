# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""TrustedHostMiddleware — blocks DNS rebinding via the Host header."""

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import UI_DIR, app


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


class TestHostHeaderEdgeCases:
    """Edge cases for the Host header check.

    These lock in the current behavior. Note that Starlette's
    TrustedHostMiddleware compares the incoming Host against allowed_hosts
    case-sensitively (it does not lowercase the header), so a Host that
    differs only in case is rejected. Matching is exact, not substring, so
    hostnames that merely share a prefix or suffix with a trusted host are
    also rejected.
    """

    @pytest.mark.asyncio
    async def test_uppercase_host_rejected(self, client):
        """Host matching is case-sensitive: an upper/mixed-case variant of a
        trusted host does not match the (lowercase) allowlist and is
        rejected."""
        resp = await client.get("/health", headers={"Host": "LOCALHOST:8080"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_mixed_case_host_rejected(self, client):
        resp = await client.get("/health", headers={"Host": "LoCalHost"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_empty_host_rejected(self, client):
        """An empty Host value is not on the allowlist and is rejected."""
        resp = await client.get("/health", headers={"Host": ""})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_whitespace_host_rejected(self, client):
        """A whitespace-only Host is not trimmed into a trusted value; it is
        rejected."""
        resp = await client.get("/health", headers={"Host": "   "})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_suffix_confusion_host_rejected(self, client):
        """A host that has a trusted name as a *prefix* label
        (localhost.attacker.com) must not be accepted — matching is exact,
        not substring/suffix."""
        resp = await client.get("/health", headers={"Host": "localhost.attacker.com"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_prefix_confusion_host_rejected(self, client):
        """A host that ends with a trusted name (attacker.com.localhost) must
        not be accepted either."""
        resp = await client.get("/health", headers={"Host": "attacker.com.localhost"})
        assert resp.status_code == 400


class TestHostCheckCoversCatchAllRoute:
    """The Host check must guard every route, not just /health — including
    the SPA catch-all (GET /{path:path}), which is the most exposed surface.
    """

    @pytest.mark.skipif(
        not UI_DIR.exists(),
        reason="ui/ directory not built — run 'make ui' first",
    )
    @pytest.mark.asyncio
    async def test_catch_all_root_allowed_with_trusted_host(self, client):
        resp = await client.get("/", headers={"Host": "localhost"})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_catch_all_root_rejected_with_untrusted_host(self, client):
        resp = await client.get("/", headers={"Host": "attacker-controlled.com"})
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_catch_all_spa_route_rejected_with_untrusted_host(self, client):
        """An arbitrary deep SPA path is still subject to the Host check."""
        resp = await client.get(
            "/some/spa/route", headers={"Host": "attacker-controlled.com"}
        )
        assert resp.status_code == 400
