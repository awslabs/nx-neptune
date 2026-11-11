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

from .projection_store import IN_FLIGHT_STATUSES, Projection, store as projection_store
from .query_store import EdgeQuery, NodeQuery, query_store
from .db import connection
from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.property_graph import (
    AthenaTableMetadata,
    parse_property_graph,
    property_graph_to_sql,
)


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

    def try_transition(
        self,
        projection_id: str,
        to_status: str,
        refuse_when: frozenset = IN_FLIGHT_STATUSES,
        **data,
    ) -> bool:
        """Atomic claim: see ``ProjectionStore.try_transition``."""
        return projection_store.try_transition(
            projection_id, to_status, refuse_when, **data
        )

    def purge(self, projection_id: str, force: bool = False) -> bool:
        """Atomically delete a projection and its queries if safe to purge.

        Returns False (and removes nothing) when the row is missing, has a
        graph attached, or is in flight without ``force``.
        """
        with connection() as conn:
            if not projection_store.purge_on(conn, projection_id, force):
                return False
            query_store.delete_node_and_edge_queries_on(conn, projection_id)
            return True

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

    def translate(
        self, projection_id: str, property_graph: Optional[str] = None
    ) -> tuple[List[str], List[str]]:
        """Translate a CREATE PROPERTY GRAPH statement into (node, edge) SQL.

        ``property_graph`` is translated when provided; otherwise the
        projection's saved ``property_graph`` is used. Pure — does not persist
        the generated queries (the caller does that via ``save_queries``).

        Raises:
            ValueError: If there is no DDL to translate.
            PropertyGraphError: If the DDL is invalid or does not match the
                source tables (surfaced to the client as an error message).
        """

        p = self.get_or_raise(projection_id)
        ddl = property_graph if property_graph is not None else p.property_graph
        if not ddl:
            raise ValueError("No property graph schema to translate.")

        graph = parse_property_graph(ddl)
        metadata = AthenaTableMetadata(
            ClientFactory().athena(), catalog=p.catalog, database=p.database
        )
        queries = property_graph_to_sql(graph, metadata)
        # property_graph_to_sql returns vertex queries first, then edge queries.
        n_vertex = len(graph.vertex_tables)
        return queries[:n_vertex], queries[n_vertex:]

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
