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

import re
from typing import Any, List, Optional

from nx_neptune.clients import NeptuneAnalyticsClient

from .projection_store import Projection
from .projection_store import store as projection_store
from .query_store import EdgeQuery, NodeQuery, query_store

# --- Read-only graph-query configuration ---

# Projection status at which the backing Neptune graph exists and is queryable.
QUERYABLE_STATUS = "complete"

# Default LIMIT appended when a query has none, and the hard ceiling on rows
# returned regardless of any user-supplied LIMIT. These are sensible defaults
# and are expected to be revisited based on client feedback.
DEFAULT_LIMIT = 100
MAX_ROWS = 1000

# Per-query timeout (seconds) applied to the Neptune Analytics client. The
# service converts this to queryTimeoutMilliseconds natively.
QUERY_TIMEOUT_SECONDS = 30

# Best-effort read-only guard. This is NOT a parser — IAM (a read-only scoped
# session) is the real control; this denylist is a coarse first line applied
# server-side before execution. Case-insensitive, substring match.
_MUTATION_KEYWORDS = (
    "DETACH DELETE",
    "CREATE",
    "MERGE",
    "DELETE",
    "SET",
    "REMOVE",
)

# Detect a trailing LIMIT clause so we only append one when missing.
_LIMIT_RE = re.compile(r"\blimit\b\s+\d+\s*;?\s*$", re.IGNORECASE)


# --- Exceptions ---


class ProjectionNotFound(Exception):
    """Raised when a projection id does not exist."""

    def __init__(self, projection_id: str):
        self.projection_id = projection_id
        super().__init__(f"Projection not found: {projection_id}")


class ProjectionNotQueryable(Exception):
    """Raised when a projection has no live, queryable graph.

    Carries a human-readable message so the router can return a 400 with a
    clear explanation rather than surfacing a 500.
    """

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class ReadOnlyQueryViolation(Exception):
    """Raised when a query contains a disallowed mutation keyword."""

    def __init__(self, keyword: str):
        self.keyword = keyword
        super().__init__(
            f"Only read-only queries are allowed; disallowed keyword: {keyword}"
        )


# --- Internal Utils ---


def _find_mutation_keyword(query: str) -> Optional[str]:
    """Return the first disallowed mutation keyword found, else None.

    This denylist is a best-effort, case-insensitive substring match — a coarse
    first line of defense applied server-side before execution. It is NOT the
    primary guard: the authoritative read-only enforcement is at the IAM level
    (a read-only-scoped session/role for the Neptune data plane), which cannot
    be bypassed by query phrasing. This check exists to fail obvious mutations
    fast with a clear error, not to be a substitute for that IAM scoping.

    ``DETACH DELETE`` is checked first so it is reported in preference to the
    bare ``DELETE`` it contains.
    """
    upper = query.upper()
    for keyword in _MUTATION_KEYWORDS:
        if keyword in upper:
            return keyword
    return None


def _ensure_limit(query: str, limit: int) -> str:
    """Append ``LIMIT <limit>`` when the query has no trailing LIMIT clause.

    A user-supplied LIMIT is left intact; the row ceiling is still enforced on
    the results after execution as a belt-and-suspenders bound.
    """
    stripped = query.rstrip().rstrip(";").rstrip()
    if _LIMIT_RE.search(query):
        return query
    return f"{stripped} LIMIT {limit}"


# --- Result normalization ---


def _normalize_cell(value: Any) -> Any:
    """Normalize a single openCypher result value for the tabular response.

    Only scalars pass through; everything else (nodes,
    relationships, lists, maps) is stringified. Proper structured node/list
    rendering is deferred; the outer ``{columns, rows}``
    shape does not change when that lands.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def graph_result_from_records(
    results: List[dict],
) -> tuple[list[str], list[list[Any]]]:
    """Build ``(columns, rows)`` from a list of openCypher record dicts.

    Column order is derived from the first record's key order (which reflects
    the query's RETURN order). An empty result set yields empty columns/rows.
    """
    if not results:
        return [], []
    columns = list(results[0].keys())
    rows = [[_normalize_cell(record.get(col)) for col in columns] for record in results]
    return columns, rows


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

    # --- Read-only graph query ---

    def run_graph_query(
        self, projection_id: str, query: str, limit: int = DEFAULT_LIMIT
    ) -> dict:
        """Run a read-only openCypher query against a projection's graph.

        Returns ``{"columns": [...], "rows": [[...], ...]}``.

        Raises:
            ProjectionNotFound: unknown projection id (router -> 404).
            ProjectionNotQueryable: no live/queryable graph (router -> 400).
            ReadOnlyQueryViolation: query contains a mutation keyword (-> 400).
            Exception: execution failures propagate for the router to sanitize.
        """
        projection = self.get_or_raise(projection_id)

        if not projection.graph_id:
            raise ProjectionNotQueryable(
                "This projection has no graph yet. Run the import pipeline first."
            )
        if projection.status != QUERYABLE_STATUS:
            raise ProjectionNotQueryable(
                f"Projection is not queryable (status: {projection.status}). "
                "The graph must finish importing before it can be queried."
            )

        keyword = _find_mutation_keyword(query)
        if keyword is not None:
            raise ReadOnlyQueryViolation(keyword)

        capped_limit = min(limit, MAX_ROWS)
        effective_query = _ensure_limit(query, capped_limit)

        na_client = NeptuneAnalyticsClient(
            graph_id=projection.graph_id,
            timeout_seconds=QUERY_TIMEOUT_SECONDS,
        )
        results = na_client.execute_query(effective_query)

        columns, rows = graph_result_from_records(results)
        # Bound rows even if a user-supplied LIMIT was
        # larger than the ceiling (or absent and ignored by the engine).
        rows = rows[:MAX_ROWS]
        return {"columns": columns, "rows": rows}

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
