# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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


@pytest.mark.asyncio
async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


@pytest.mark.asyncio
async def test_health_returns_request_id(client):
    resp = await client.get("/health")
    assert "x-request-id" in resp.headers


@pytest.mark.asyncio
async def test_custom_request_id_echoed(client):
    resp = await client.get("/health", headers={"x-request-id": "abc-123"})
    assert resp.headers["x-request-id"] == "abc-123"


@pytest.mark.asyncio
async def test_not_found_returns_404(client):
    resp = await client.get("/api/v0/nonexistent")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_docs_disabled(client):
    resp = await client.get("/docs")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_redoc_disabled(client):
    resp = await client.get("/redoc")
    assert resp.status_code == 404


@pytest.mark.skipif(
    not UI_DIR.exists(),
    reason="ui/ directory not built — run 'make ui' first",
)
class TestDocsDisabledWithUiBuilt:
    """With docs_url=None, /docs and /redoc would otherwise fall through to
    the SPA catch-all and be served as index.html (200). Only meaningful
    once the SPA fallback route actually exists, i.e. ui/ is built — the
    two tests above run with no SPA build present, where FastAPI's own
    router already 404s these paths regardless of this control."""

    @pytest.mark.asyncio
    async def test_docs_still_404_via_spa_fallback(self, client):
        resp = await client.get("/docs")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_redoc_still_404_via_spa_fallback(self, client):
        resp = await client.get("/redoc")
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_root_still_served_by_spa_fallback(self, client):
        """Sanity check: the fix doesn't collateral-damage real SPA routes."""
        resp = await client.get("/")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_client_side_route_still_served_by_spa_fallback(self, client):
        resp = await client.get("/projects/abc")
        assert resp.status_code == 200
