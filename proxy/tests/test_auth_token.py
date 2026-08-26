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

    @pytest.mark.asyncio
    async def test_non_ascii_token_rejected_not_500(self):
        """A bearer value with non-ASCII characters must fail cleanly with a
        401, not raise TypeError (which would surface as a 500).

        secrets.compare_digest raises TypeError on non-ASCII str inputs, so
        the dependency compares encoded bytes. This exercises the dependency
        directly because HTTP clients won't transport non-ASCII header bytes,
        so the str reaches require_token via header decoding, not the wire.
        """
        from fastapi import HTTPException

        from nx_neptune_proxy.auth import require_token

        with pytest.raises(HTTPException) as exc_info:
            await require_token(authorization="Bearer tökén-nön-ascii-\u00e9\u00e8")
        assert exc_info.value.status_code == 401


class TestTokenNotLeakedByUnauthenticatedRoutes:
    """The token must not be recoverable from any unauthenticated response.

    It is delivered only via the launch URL; the SPA shell (served by the
    unauthenticated catch-all) must not embed it, so reaching the port does
    not yield the token.
    """

    @pytest.mark.asyncio
    async def test_index_does_not_contain_token(self):
        client = _client({"X-Requested-With": "nx-neptune"})
        resp = await client.get("/")
        # Either the UI isn't bundled in this test env (404/other) or, if it
        # is served, the body must not contain the token nor the old meta tag.
        if resp.status_code == 200:
            body = resp.text
            assert get_token() not in body
            assert "nx-neptune-proxy-token" not in body
