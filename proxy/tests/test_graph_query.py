# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the read-only graph-query path: ProjectionService.run_graph_query
and POST /api/v0/projection/{id}/graph-query."""

from unittest.mock import MagicMock, patch

import pytest

from nx_neptune_proxy.services.projection_service import (
    MAX_ROWS,
    ProjectionNotFound,
    ProjectionNotQueryable,
    ReadOnlyQueryViolation,
    projection_service,
)
from nx_neptune_proxy.services.projection_store import store

# Patch target: the symbol as imported into the service module.
_NA_CLIENT = "nx_neptune_proxy.services.projection_service.NeptuneAnalyticsClient"


def _queryable_projection(project_id):
    """Create a projection with a live graph in the queryable ('complete') state."""
    p = store.create(graph_name="g", project_id=project_id)
    store.update(p.id, status="complete", graph_id="g-abc123")
    return p


def _mock_na(results):
    """Patch NeptuneAnalyticsClient so execute_query returns ``results``."""
    instance = MagicMock()
    instance.execute_query.return_value = results
    ctx = patch(_NA_CLIENT, return_value=instance)
    return ctx, instance


# --------------------------------------------------------------------------
# Service: run_graph_query
# --------------------------------------------------------------------------


class TestRunGraphQueryService:
    def test_happy_path_scalars(self, test_project_id):
        p = _queryable_projection(test_project_id)
        results = [{"name": "Alice", "score": 1.5}, {"name": "Bob", "score": 2.0}]
        ctx, _ = _mock_na(results)
        with ctx:
            out = projection_service.run_graph_query(
                p.id, "MATCH (n) RETURN n.name, n.score"
            )
        assert out["columns"] == ["name", "score"]
        assert out["rows"] == [["Alice", 1.5], ["Bob", 2.0]]

    def test_limit_appended_when_missing(self, test_project_id):
        p = _queryable_projection(test_project_id)
        ctx, instance = _mock_na([{"x": 1}])
        with ctx:
            projection_service.run_graph_query(p.id, "MATCH (n) RETURN n", limit=25)
        executed = instance.execute_query.call_args[0][0]
        assert executed.strip().upper().endswith("LIMIT 25")

    def test_existing_limit_preserved(self, test_project_id):
        p = _queryable_projection(test_project_id)
        ctx, instance = _mock_na([{"x": 1}])
        with ctx:
            projection_service.run_graph_query(p.id, "MATCH (n) RETURN n LIMIT 3")
        executed = instance.execute_query.call_args[0][0]
        # No second LIMIT appended.
        assert executed.upper().count("LIMIT") == 1

    def test_limit_capped_at_max_rows(self, test_project_id):
        p = _queryable_projection(test_project_id)
        ctx, instance = _mock_na([{"x": 1}])
        with ctx:
            projection_service.run_graph_query(
                p.id, "MATCH (n) RETURN n", limit=MAX_ROWS + 500
            )
        executed = instance.execute_query.call_args[0][0]
        assert executed.strip().upper().endswith(f"LIMIT {MAX_ROWS}")

    def test_rows_bounded_by_ceiling(self, test_project_id):
        p = _queryable_projection(test_project_id)
        results = [{"x": i} for i in range(MAX_ROWS + 50)]
        ctx, _ = _mock_na(results)
        with ctx:
            out = projection_service.run_graph_query(p.id, "MATCH (n) RETURN n")
        assert len(out["rows"]) == MAX_ROWS

    def test_node_and_list_cells_stringified(self, test_project_id):
        p = _queryable_projection(test_project_id)
        results = [
            {
                "scalar": 42,
                "node": {"~id": "1", "~labels": ["Person"]},
                "members": [1, 2, 3],
            }
        ]
        ctx, _ = _mock_na(results)
        with ctx:
            out = projection_service.run_graph_query(p.id, "MATCH (n) RETURN n")
        row = out["rows"][0]
        assert row[0] == 42  # scalar passthrough
        assert isinstance(row[1], str) and "Person" in row[1]  # node -> str
        assert isinstance(row[2], str) and row[2] == "[1, 2, 3]"  # list -> str

    def test_empty_results(self, test_project_id):
        p = _queryable_projection(test_project_id)
        ctx, _ = _mock_na([])
        with ctx:
            out = projection_service.run_graph_query(p.id, "MATCH (n) RETURN n")
        assert out == {"columns": [], "rows": []}

    @pytest.mark.parametrize(
        "query",
        [
            "CREATE (n:Person)",
            "MATCH (n) SET n.x = 1",
            "MATCH (n) DELETE n",
            "MATCH (n) DETACH DELETE n",
            "MERGE (n:Person {id: 1})",
            "MATCH (n) REMOVE n.x",
            "match (n) delete n",  # case-insensitive
        ],
    )
    def test_denylist_rejects_mutations(self, query, test_project_id):
        p = _queryable_projection(test_project_id)
        with pytest.raises(ReadOnlyQueryViolation):
            projection_service.run_graph_query(p.id, query)

    def test_unknown_projection_raises_not_found(self, test_project_id):
        with pytest.raises(ProjectionNotFound):
            projection_service.run_graph_query("does-not-exist", "MATCH (n) RETURN n")

    def test_no_graph_id_not_queryable(self, test_project_id):
        p = store.create(
            graph_name="g", project_id=test_project_id
        )  # draft, no graph_id
        with pytest.raises(ProjectionNotQueryable):
            projection_service.run_graph_query(p.id, "MATCH (n) RETURN n")

    def test_status_not_complete_not_queryable(self, test_project_id):
        p = store.create(graph_name="g", project_id=test_project_id)
        store.update(p.id, status="importing", graph_id="g-abc123")
        with pytest.raises(ProjectionNotQueryable):
            projection_service.run_graph_query(p.id, "MATCH (n) RETURN n")


# --------------------------------------------------------------------------
# Endpoint: POST /api/v0/projection/{id}/graph-query
# --------------------------------------------------------------------------


class TestGraphQueryEndpoint:
    @pytest.mark.asyncio
    async def test_returns_columns_and_rows(self, client, test_project_id):
        p = _queryable_projection(test_project_id)
        ctx, _ = _mock_na([{"name": "Alice"}, {"name": "Bob"}])
        with ctx:
            resp = await client.post(
                f"/api/v0/projection/{p.id}/graph-query",
                json={"query": "MATCH (n) RETURN n.name"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["columns"] == ["name"]
        assert data["rows"] == [["Alice"], ["Bob"]]
        assert data["error"] is None

    @pytest.mark.asyncio
    async def test_unknown_projection_404(self, client):
        resp = await client.post(
            "/api/v0/projection/nope/graph-query",
            json={"query": "MATCH (n) RETURN n"},
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_non_queryable_400(self, client, test_project_id):
        p = store.create(graph_name="g", project_id=test_project_id)  # draft
        resp = await client.post(
            f"/api/v0/projection/{p.id}/graph-query",
            json={"query": "MATCH (n) RETURN n"},
        )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_mutation_rejected_400(self, client, test_project_id):
        p = _queryable_projection(test_project_id)
        resp = await client.post(
            f"/api/v0/projection/{p.id}/graph-query",
            json={"query": "MATCH (n) DETACH DELETE n"},
        )
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_execution_error_sanitized(self, client, test_project_id):
        p = _queryable_projection(test_project_id)
        instance = MagicMock()
        instance.execute_query.side_effect = Exception(
            "boom at arn:aws:iam::123456789012:user/dev"
        )
        with patch(_NA_CLIENT, return_value=instance):
            resp = await client.post(
                f"/api/v0/projection/{p.id}/graph-query",
                json={"query": "MATCH (n) RETURN n"},
            )
        # Not a 500 — a readable, sanitized error body.
        assert resp.status_code == 200
        err = resp.json()["error"]
        assert err is not None
        assert "123456789012" not in err["message"]
