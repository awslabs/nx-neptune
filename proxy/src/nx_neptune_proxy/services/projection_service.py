# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Application service that orchestrates projection + query persistence.

The web layer (routers) and the pipeline talk to this service instead of
reaching into ``projection_store`` and ``query_store`` directly. The service is
the single place where the two stores are coordinated, and the natural home for
any cross-store consistency logic (e.g. deleting a projection also removes its
queries).

It is intentionally HTTP-agnostic: it raises ``ProjectionNotFound`` rather than
an ``HTTPException`` so it can be reused outside the request path (pipeline,
tests). Routers translate ``ProjectionNotFound`` into a 404.
"""

from __future__ import annotations

from typing import List, Optional

from .projection_store import Projection, store as projection_store
from .query_store import EdgeQuery, NodeQuery, query_store


class ProjectionNotFound(Exception):
    """Raised when a projection id does not exist."""

    def __init__(self, projection_id: str):
        self.projection_id = projection_id
        super().__init__(f"Projection not found: {projection_id}")


class ProjectionService:
    """Coordinates projection metadata and its node/edge queries."""

    # --- Projection metadata ---

    def create(self, **data) -> Projection:
        return projection_store.create(**data)

    def get(self, projection_id: str) -> Optional[Projection]:
        return projection_store.get(projection_id)

    def get_or_raise(self, projection_id: str) -> Projection:
        projection = projection_store.get(projection_id)
        if projection is None:
            raise ProjectionNotFound(projection_id)
        return projection

    def list(self) -> List[Projection]:
        return projection_store.list()

    def list_by_project(self, project_id: str) -> List[Projection]:
        return projection_store.list_by_project(project_id)

    def update(self, projection_id: str, **data) -> Optional[Projection]:
        return projection_store.update(projection_id, **data)

    def delete(self, projection_id: str) -> bool:
        """Delete a projection and its associated node/edge queries."""
        # Remove child queries first so nothing is orphaned regardless of
        # whether SQLite FK cascade is enabled.
        query_store.save_node_queries(projection_id, [])
        query_store.save_edge_queries(projection_id, [])
        return projection_store.delete(projection_id)

    # --- Queries (child of a projection) ---

    def list_node_queries(self, projection_id: str) -> List[NodeQuery]:
        return query_store.list_node_queries(projection_id)

    def list_edge_queries(self, projection_id: str) -> List[EdgeQuery]:
        return query_store.list_edge_queries(projection_id)

    def list_query_sql(self, projection_id: str) -> List[str]:
        """All non-empty node+edge query SQL, node queries first."""
        node = [
            nq.sql for nq in self.list_node_queries(projection_id) if nq.sql.strip()
        ]
        edge = [
            eq.sql for eq in self.list_edge_queries(projection_id) if eq.sql.strip()
        ]
        return node + edge

    def list_labeled_queries(self, projection_id: str) -> List[tuple[str, str, str]]:
        """(label, sql, query_type) for each non-empty node/edge query."""
        labeled: List[tuple[str, str, str]] = []
        for i, nq in enumerate(self.list_node_queries(projection_id)):
            if nq.sql.strip():
                labeled.append((f"node_query_{i + 1}", nq.sql, "node"))
        for i, eq in enumerate(self.list_edge_queries(projection_id)):
            if eq.sql.strip():
                labeled.append((f"edge_query_{i + 1}", eq.sql, "edge"))
        return labeled

    def get_queries(self, projection_id: str) -> tuple[List[dict], List[dict]]:
        """Return (node_responses, edge_responses) as API-ready dicts."""
        return (
            query_store.list_node_responses(projection_id),
            query_store.list_edge_responses(projection_id),
        )

    def save_queries(
        self, projection_id: str, node_models: List, edge_models: List
    ) -> tuple[List[dict], List[dict]]:
        """Replace all node/edge queries, then return the stored responses."""
        query_store.save_node_from_payload(projection_id, node_models)
        query_store.save_edge_from_payload(projection_id, edge_models)
        return self.get_queries(projection_id)

    # --- Import / export helpers ---

    def get_query_sql_lists(self, projection_id: str) -> tuple[List[str], List[str]]:
        """Return (node_sql, edge_sql) lists verbatim (no filtering), for export."""
        node = [nq.sql for nq in self.list_node_queries(projection_id)]
        edge = [eq.sql for eq in self.list_edge_queries(projection_id)]
        return node, edge

    def list_projections_with_query_sql(
        self, project_id: str
    ) -> tuple[List[Projection], dict]:
        """Return (projections, {projection_id: (node_sql, edge_sql)}) for export."""
        projections = self.list_by_project(project_id)
        queries_by_projection = {
            pr.id: self.get_query_sql_lists(pr.id) for pr in projections
        }
        return projections, queries_by_projection

    def create_with_queries(
        self,
        projection_data: dict,
        node_sql: List[str],
        edge_sql: List[str],
    ) -> Projection:
        """Create a projection and its node/edge queries (for import)."""
        projection = projection_store.create(**projection_data)
        if node_sql:
            query_store.save_node_queries(
                projection.id, [{"sql": sql} for sql in node_sql]
            )
        if edge_sql:
            query_store.save_edge_queries(
                projection.id, [{"sql": sql} for sql in edge_sql]
            )
        return projection


# Singleton service instance
projection_service = ProjectionService()
