# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


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
async def test_openapi_docs_available(client):
    resp = await client.get("/docs")
    assert resp.status_code == 200
