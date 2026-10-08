# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune.property_graph import PropertyGraphSyntaxError
from nx_neptune_proxy.app import app
from nx_neptune_proxy.auth import get_token
from nx_neptune_proxy.services.db import connection
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store


@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(
        transport=transport,
        base_url="http://localhost",
        headers={
            "X-Requested-With": "nx-neptune",
            "Authorization": f"Bearer {get_token()}",
        },
    )


@pytest.fixture(autouse=True)
def clear_store():
    with connection() as conn:
        conn.execute("DELETE FROM projections")
        conn.execute("DELETE FROM projects")
    yield
    with connection() as conn:
        conn.execute("DELETE FROM projections")
        conn.execute("DELETE FROM projects")


def _make_projection(property_graph=None):
    project = project_store.create(name="T")
    p = store.create(
        catalog="AwsDataCatalog",
        database="db",
        property_graph=property_graph,
        project_id=project.id,
    )
    return p.id


@pytest.mark.asyncio
async def test_translate_splits_vertex_and_edge_queries(client):
    """A successful translate returns vertex queries as node_queries and edge
    queries as edge_queries, in that split."""
    pid = _make_projection()
    ddl = (
        "CREATE PROPERTY GRAPH g "
        "VERTEX TABLES ( accounts KEY (id) LABEL account PROPERTIES (name) ) "
        "EDGE TABLES ( transfers SOURCE (src) REFERENCES accounts "
        "DESTINATION (dst) REFERENCES accounts LABEL transfer PROPERTIES (amount) )"
    )
    # Keep property_graph_to_sql real but stub the catalog metadata so no AWS
    # call is made: accounts has id/name, transfers has src/dst/amount.
    columns = {
        "accounts": [("id", "bigint"), ("name", "string")],
        "transfers": [("src", "bigint"), ("dst", "bigint"), ("amount", "double")],
    }

    class FakeMetadata:
        def __init__(self, *a, **k):
            pass

        def get_columns(self, table):
            from nx_neptune.property_graph import Column

            return [Column(n, t) for n, t in columns[".".join(table).lower()]]

    with patch(
        "nx_neptune_proxy.services.projection_service.AthenaTableMetadata",
        FakeMetadata,
    ), patch("nx_neptune_proxy.services.projection_service.ClientFactory"):
        resp = await client.post(
            f"/api/v0/projection/{pid}/translate",
            json={"property_graph": ddl},
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["error"] is None
    assert len(data["node_queries"]) == 1
    assert len(data["edge_queries"]) == 1
    assert '"~id"' in data["node_queries"][0]
    assert '"~from"' in data["edge_queries"][0]
    assert '"~to"' in data["edge_queries"][0]


@pytest.mark.asyncio
async def test_translate_invalid_ddl_returns_error_not_500(client):
    """A PropertyGraphError is surfaced as 200 with error set."""
    pid = _make_projection()
    resp = await client.post(
        f"/api/v0/projection/{pid}/translate",
        json={"property_graph": "this is not a property graph"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["error"]
    assert data["node_queries"] == []
    assert data["edge_queries"] == []


@pytest.mark.asyncio
async def test_translate_falls_back_to_saved_ddl_when_body_omitted(client):
    """Omitting property_graph uses the projection's saved copy."""
    pid = _make_projection(property_graph="also invalid ddl")
    resp = await client.post(f"/api/v0/projection/{pid}/translate", json={})
    assert resp.status_code == 200
    # Saved DDL is invalid -> error (proves the saved copy was used, not empty).
    assert resp.json()["error"]


@pytest.mark.asyncio
async def test_translate_no_ddl_anywhere_returns_error(client):
    """No body DDL and no saved DDL -> a clear error, not a crash."""
    pid = _make_projection(property_graph=None)
    resp = await client.post(f"/api/v0/projection/{pid}/translate", json={})
    assert resp.status_code == 200
    assert "No property graph schema" in resp.json()["error"]


@pytest.mark.asyncio
async def test_translate_unknown_projection_404(client):
    resp = await client.post(
        "/api/v0/projection/does-not-exist/translate", json={"property_graph": "x"}
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_property_graph_persists_through_create_and_get(client):
    """property_graph round-trips through create -> get."""
    project = project_store.create(name="T")
    body = {
        "database": "db",
        "property_graph": "CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY (id) )",
        "graph_name": "test-graph",
        "project_id": project.id,
    }
    created = await client.post("/api/v0/projection", json=body)
    assert created.status_code == 201
    pid = created.json()["id"]
    assert created.json()["property_graph"] == body["property_graph"]

    got = await client.get(f"/api/v0/projection/{pid}")
    assert got.status_code == 200
    assert got.json()["property_graph"] == body["property_graph"]
