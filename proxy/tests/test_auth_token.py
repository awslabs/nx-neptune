# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-run bearer token enforcement on /api/* routes."""

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.auth import get_token


def _client(headers=None):
    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport, base_url="http://localhost", headers=headers or {}
    )


class TestTokenRequiredOnApiRoutes:
    """Every /api/* route behind a router must require the bearer token."""

    @pytest.mark.asyncio
    async def test_missing_token_rejected(self):
        client = _client({"X-Requested-With": "nx-neptune"})
        resp = await client.get("/api/v0/project")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_wrong_token_rejected(self):
        client = _client(
            {"X-Requested-With": "nx-neptune", "Authorization": "Bearer not-the-token"}
        )
        resp = await client.get("/api/v0/project")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_malformed_auth_header_rejected(self):
        client = _client(
            {"X-Requested-With": "nx-neptune", "Authorization": get_token()}
        )
        resp = await client.get("/api/v0/project")
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_correct_token_accepted(self):
        client = _client(
            {"X-Requested-With": "nx-neptune", "Authorization": f"Bearer {get_token()}"}
        )
        resp = await client.get("/api/v0/project")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_correct_token_accepted_on_other_routers(self):
        client = _client(
            {"X-Requested-With": "nx-neptune", "Authorization": f"Bearer {get_token()}"}
        )
        resp = await client.get("/api/v0/projection")
        assert resp.status_code == 200
        resp = await client.get("/api/v0/metadata/config")
        assert resp.status_code in (
            200,
            500,
            502,
        )  # reaches the handler, not blocked by auth
        resp = await client.get("/api/v0/graphs/some-id/actions")
        assert resp.status_code != 401


class TestTokenExemptRoutes:
    """/health and /api/v0/info are not behind the router-level dependency."""

    @pytest.mark.asyncio
    async def test_health_does_not_require_token(self):
        client = _client({"X-Requested-With": "nx-neptune"})
        resp = await client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_info_does_not_require_token(self):
        client = _client({"X-Requested-With": "nx-neptune"})
        resp = await client.get("/api/v0/info")
        assert resp.status_code == 200


class TestTokenProperties:
    @pytest.mark.asyncio
    async def test_token_is_nonempty_and_high_entropy(self):
        token = get_token()
        assert isinstance(token, str)
        assert len(token) >= 32

    @pytest.mark.asyncio
    async def test_token_stable_within_process(self):
        assert get_token() == get_token()
