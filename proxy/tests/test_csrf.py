# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CSRF protection via X-Requested-With header requirement."""

import pytest


class TestCsrfProtection:
    """State-changing requests must include X-Requested-With header."""

    # --- Blocked without header ---

    @pytest.mark.asyncio
    async def test_post_without_header_rejected(self, bare_client):
        """POST without X-Requested-With must be rejected."""
        resp = await bare_client.post(
            "/api/v0/projection", json={"database": "test", "graph_name": "g"}
        )
        assert resp.status_code == 403
        assert resp.json()["error"] == "csrf_rejected"

    @pytest.mark.asyncio
    async def test_put_without_header_rejected(self, bare_client):
        """PUT without X-Requested-With must be rejected."""
        resp = await bare_client.put(
            "/api/v0/projection/fake-id", json={"status": "done"}
        )
        assert resp.status_code == 403
        assert resp.json()["error"] == "csrf_rejected"

    @pytest.mark.asyncio
    async def test_delete_without_header_rejected(self, bare_client):
        """DELETE without X-Requested-With must be rejected."""
        resp = await bare_client.delete("/api/v0/projection/fake-id")
        assert resp.status_code == 403
        assert resp.json()["error"] == "csrf_rejected"

    # --- Allowed with header ---

    @pytest.mark.asyncio
    async def test_post_with_header_allowed(self, client):
        """POST with X-Requested-With must pass CSRF check."""
        resp = await client.post(
            "/api/v0/projection",
            json={"database": "test", "graph_name": "g"},
        )
        # Should pass CSRF check (may get 422/400 from validation, but not 403 csrf)
        assert resp.status_code != 403 or resp.json().get("error") != "csrf_rejected"

    @pytest.mark.asyncio
    async def test_delete_with_header_allowed(self, client):
        """DELETE with X-Requested-With must pass CSRF check."""
        resp = await client.delete("/api/v0/projection/fake-id")
        # Should pass CSRF check (may get 404, but not 403 csrf)
        assert resp.status_code != 403 or resp.json().get("error") != "csrf_rejected"

    # --- Safe methods unaffected ---

    @pytest.mark.asyncio
    async def test_get_without_header_allowed(self, bare_client):
        """GET must work without X-Requested-With."""
        resp = await bare_client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_options_without_header_allowed(self, bare_client):
        """OPTIONS (preflight) must work without X-Requested-With."""
        resp = await bare_client.options("/api/v0/projection")
        assert resp.status_code != 403
