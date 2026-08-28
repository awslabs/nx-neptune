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


# Singleton service instance
projection_service = ProjectionService()
