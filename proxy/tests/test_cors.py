# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CORS origin restriction enforcement."""

import pytest


class TestCorsRejection:
    """CORS middleware must reject origins not in CORS_ALLOWED_ORIGINS."""

    @pytest.mark.asyncio
    async def test_disallowed_origin_no_cors_headers(self, client):
        """Preflight from an unknown origin must not get permissive CORS headers."""
        resp = await client.options(
            "/api/v0/projection",
            headers={
                "Origin": "http://evil.com",
                "Access-Control-Request-Method": "POST",
            },
        )
        allow_origin = resp.headers.get("access-control-allow-origin")
        # Must be absent — not the evil origin, not wildcard
        assert allow_origin is None

    @pytest.mark.asyncio
    async def test_disallowed_origin_get_request(self, client):
        """Regular request from disallowed origin must not get CORS headers."""
        resp = await client.get(
            "/health",
            headers={"Origin": "http://attacker.example.com"},
        )
        allow_origin = resp.headers.get("access-control-allow-origin")
        assert allow_origin is None

    @pytest.mark.asyncio
    async def test_no_wildcard_cors(self, client):
        """CORS must never return wildcard Access-Control-Allow-Origin."""
        resp = await client.options(
            "/api/v0/projection",
            headers={
                "Origin": "http://random.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        allow_origin = resp.headers.get("access-control-allow-origin")
        assert allow_origin != "*"
