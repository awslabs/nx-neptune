# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""client_ip_guard — reject non-loopback clients unless a non-loopback bind
was explicitly acknowledged (ALLOW_NON_LOOPBACK_BIND).

Unlike the Host/Origin/CORS checks (which inspect forgeable headers), this
inspects the TCP peer address (request.client.host), which a remote client
cannot forge. Tests drive it via ASGITransport's `client` argument, which
sets request.client.host.
"""

import dataclasses

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app


def _client(client_addr=("127.0.0.1", 12345)):
    # ASGITransport(client=(host, port)) sets request.client.host.
    transport = ASGITransport(app=app, client=client_addr)
    return AsyncClient(
        transport=transport,
        base_url="http://localhost",
        headers={"X-Requested-With": "nx-neptune"},
    )


class TestClientIpGuardDefault:
    """Default settings (allow_non_loopback_bind=False)."""

    @pytest.mark.asyncio
    async def test_loopback_ipv4_allowed(self):
        resp = await _client(("127.0.0.1", 12345)).get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_loopback_ipv6_allowed(self):
        resp = await _client(("::1", 12345)).get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_non_loopback_client_rejected(self):
        resp = await _client(("203.0.113.5", 40000)).get("/health")
        assert resp.status_code == 403
        assert resp.json()["error"] == "non_loopback_client"

    @pytest.mark.asyncio
    async def test_private_lan_client_rejected(self):
        resp = await _client(("192.168.1.20", 40000)).get("/health")
        assert resp.status_code == 403

    @pytest.mark.asyncio
    async def test_missing_client_rejected_fails_closed(self):
        # No client tuple -> request.client is None -> treated as non-loopback.
        resp = await _client(client_addr=None).get("/health")
        assert resp.status_code == 403


class TestClientIpGuardOptIn:
    """With allow_non_loopback_bind=True the guard is disabled (e.g. inside a
    container whose published ports are restricted)."""

    @pytest.fixture
    def opted_in(self, monkeypatch):
        # settings is a frozen dataclass, so swap in a modified copy.
        import nx_neptune_proxy.app as app_module

        patched = dataclasses.replace(app_module.settings, allow_non_loopback_bind=True)
        monkeypatch.setattr(app_module, "settings", patched)

    @pytest.mark.asyncio
    async def test_non_loopback_allowed_when_opted_in(self, opted_in):
        resp = await _client(("203.0.113.5", 40000)).get("/health")
        assert resp.status_code == 200
