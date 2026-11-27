# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from .db import connection

STATUS_DRAFT = "draft"
STATUS_EXECUTING = "executing"
STATUS_IMPORTING = "importing"
STATUS_DELETING = "deleting"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"
STATUS_ARCHIVED = "archived"

# States in which a long-running operation owns the projection.
IN_FLIGHT_STATUSES = frozenset({STATUS_EXECUTING, STATUS_IMPORTING, STATUS_DELETING})

_FIELDS = [
    "id",
    "status",
    "catalog",
    "database",
    "node_query",
    "edge_query",
    "property_graph",
    "graph_name",
    "graph_memory_gb",
    "s3_staging_bucket",
    "graph_id",
    "graph_endpoint",
    "project_id",
    "step",
    "step_label",
    "progress",
    "error",
    "created_at",
]


@dataclass
class Projection:
    """State for a single projection pipeline."""

    id: str
    status: str
    catalog: str = "AwsDataCatalog"
    database: Optional[str] = None
    node_query: Optional[str] = None
    edge_query: Optional[str] = None
    property_graph: Optional[str] = None
    graph_name: Optional[str] = None
    graph_memory_gb: int = 16
    s3_staging_bucket: Optional[str] = None
    graph_id: Optional[str] = None
    graph_endpoint: Optional[str] = None
    project_id: Optional[str] = None
    step: Optional[str] = None
    step_label: Optional[str] = None
    progress: float = 0
    error: Optional[str] = None
    # Which run owns the row. Rotated by every claim; see try_transition.
    owner_token: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectionStore:
    def create(
        self,
        catalog: str = "AwsDataCatalog",
        database: Optional[str] = None,
        node_query: Optional[str] = None,
        edge_query: Optional[str] = None,
        property_graph: Optional[str] = None,
        graph_name: Optional[str] = None,
        graph_memory_gb: int = 16,
        s3_staging_bucket: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> Projection:
        p = Projection(
            id=str(uuid.uuid4()),
            status=STATUS_DRAFT,
            catalog=catalog,
            database=database,
            node_query=node_query,
            edge_query=edge_query,
            property_graph=property_graph,
            graph_name=graph_name,
            graph_memory_gb=graph_memory_gb,
            s3_staging_bucket=s3_staging_bucket,
            project_id=project_id,
        )
        with connection() as conn:
            conn.execute(
                f"INSERT INTO projections ({', '.join(_FIELDS)}) VALUES ({', '.join('?' for _ in _FIELDS)})",
                [
                    getattr(p, f) if f != "created_at" else p.created_at.isoformat()
                    for f in _FIELDS
                ],
            )
        return p

    def get(self, projection_id: str) -> Optional[Projection]:
        with connection() as conn:
            row = conn.execute(
                "SELECT * FROM projections WHERE id = ?", (projection_id,)
            ).fetchone()
        return self._row_to_projection(row) if row else None

    def list(self) -> List[Projection]:
        with connection() as conn:
            rows = conn.execute(
                "SELECT * FROM projections ORDER BY created_at DESC"
            ).fetchall()
        return [self._row_to_projection(r) for r in rows]

    def list_by_project(self, project_id: str) -> List[Projection]:
        with connection() as conn:
            rows = conn.execute(
                "SELECT * FROM projections WHERE project_id = ? ORDER BY created_at DESC",
                (project_id,),
            ).fetchall()
        return [self._row_to_projection(r) for r in rows]

    _ALLOWED_UPDATE_COLUMNS = {
        "status",
        "catalog",
        "database",
        "node_query",
        "edge_query",
        "property_graph",
        "graph_name",
        "graph_memory_gb",
        "s3_staging_bucket",
        "graph_id",
        "graph_endpoint",
        "project_id",
        "step",
        "step_label",
        "progress",
        "error",
        "owner_token",
    }

    def update(self, projection_id: str, **kwargs) -> Optional[Projection]:
        if not kwargs:
            return self.get(projection_id)
        invalid = set(kwargs.keys()) - self._ALLOWED_UPDATE_COLUMNS
        if invalid:
            raise ValueError(f"Invalid column(s): {invalid}")
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [projection_id]
        with connection() as conn:
            conn.execute(f"UPDATE projections SET {sets} WHERE id = ?", vals)
        return self.get(projection_id)

    def try_transition(
        self,
        projection_id: str,
        to_status: str,
        refuse_when: frozenset = IN_FLIGHT_STATUSES,
        owner_token: Optional[str] = None,
        **fields,
    ) -> bool:
        """Atomically set ``status`` unless it is currently in ``refuse_when``.

        A single compare-and-set UPDATE, so concurrent callers cannot both win.
        Extra ``fields`` are written in the same statement. Returns True if the
        row was claimed, False if it is missing or in a refused status.

        **Every claim rotates ``owner_token``.** A status change is a new
        ownership epoch, and that is what lets a previous owner's later writes
        be rejected: matching on status alone cannot tell "still my run" from
        "a different run that reached the same status". A caller that intends
        to keep writing passes its own ``owner_token`` so it knows the value it
        now holds; otherwise a fresh one is generated and the old owner is
        simply locked out.
        """
        invalid = set(fields) - self._ALLOWED_UPDATE_COLUMNS
        if invalid:
            raise ValueError(f"Invalid column(s): {invalid}")
        columns = {
            "status": to_status,
            "owner_token": owner_token or str(uuid.uuid4()),
            **fields,
        }
        sets = ", ".join(f"{k} = ?" for k in columns)
        guard = ", ".join("?" for _ in refuse_when)
        with connection() as conn:
            cur = conn.execute(
                f"UPDATE projections SET {sets} WHERE id = ? AND status NOT IN ({guard})",
                [*columns.values(), projection_id, *refuse_when],
            )
            return cur.rowcount > 0

    def try_transition_if_graph(
        self,
        projection_id: str,
        to_status: str,
        expected_graph_id: Optional[str],
        refuse_when: frozenset = IN_FLIGHT_STATUSES,
        owner_token: Optional[str] = None,
        **fields,
    ) -> bool:
        """``try_transition``, additionally requiring ``graph_id`` to be unchanged.

        For a caller that read ``graph_id``, decided something about that graph,
        and now wants to act on the row. With ``force`` the status guard is
        dropped, so the status alone cannot rule out an in-flight run having
        recorded a *different* graph in the meantime — clearing the reference
        then would orphan a live, billing graph.
        """
        invalid = set(fields) - self._ALLOWED_UPDATE_COLUMNS
        if invalid:
            raise ValueError(f"Invalid column(s): {invalid}")
        columns = {
            "status": to_status,
            "owner_token": owner_token or str(uuid.uuid4()),
            **fields,
        }
        sets = ", ".join(f"{k} = ?" for k in columns)
        guard = ", ".join("?" for _ in refuse_when)
        with connection() as conn:
            cur = conn.execute(
                f"UPDATE projections SET {sets} WHERE id = ? "
                f"AND status NOT IN ({guard}) AND graph_id IS ?",
                [*columns.values(), projection_id, *refuse_when, expected_graph_id],
            )
            return cur.rowcount > 0

    def clear_graph_if(self, projection_id: str, expected_graph_id: str) -> bool:
        """Drop the graph reference only while it still names ``expected_graph_id``.

        For a caller that has just deleted a graph and wants to remove the
        reference to it. An unconditional clear is wrong here: a run that was
        taken over mid-import records *its* graph on the same row
        (``pipeline._record_graph``), so a clear keyed off the id the caller
        read earlier would wipe a reference to a different, live graph and leave
        it billing with nothing pointing at it.

        Returns True if the reference was still the expected one and was
        cleared. False means the row is gone or now names another graph, and the
        caller has to deal with that graph rather than assume it cleaned up.
        """
        if not expected_graph_id:
            raise ValueError("clear_graph_if needs the graph id it expects")
        with connection() as conn:
            cur = conn.execute(
                "UPDATE projections SET graph_id = NULL, graph_endpoint = NULL "
                "WHERE id = ? AND graph_id IS ?",
                [projection_id, expected_graph_id],
            )
            return cur.rowcount > 0

    def update_if_owner(self, projection_id: str, owner_token: str, **fields) -> bool:
        """Write ``fields`` only while ``owner_token`` still owns the row.

        The write half of a claim. ``update`` is unconditional, and matching on
        status is not enough: a run that was taken over and then re-claimed by a
        *new* run reaches the same status again, so the stale run's writes would
        land on the new run's row and mark it complete or failed underneath it.
        The token is rotated by every claim, so it distinguishes the two.

        Returns True if the row was still ours and the write landed.
        """
        invalid = set(fields) - self._ALLOWED_UPDATE_COLUMNS
        if invalid:
            raise ValueError(f"Invalid column(s): {invalid}")
        if not fields:
            raise ValueError("update_if_owner needs at least one field")
        if not owner_token:
            raise ValueError("update_if_owner needs an owner_token")
        sets = ", ".join(f"{k} = ?" for k in fields)
        with connection() as conn:
            cur = conn.execute(
                f"UPDATE projections SET {sets} WHERE id = ? AND owner_token = ?",
                [*fields.values(), projection_id, owner_token],
            )
            return cur.rowcount > 0

    def fail_orphaned_runs(self, error: str) -> List[str]:
        """Mark rows left mid-run by a process exit as ``failed``.

        An ``importing`` or ``executing`` row is owned by a background task in
        this process. If the proxy stops before that task finishes, nothing is
        left to advance the row: execute refuses it (the status is in flight)
        and the UI sends no ``force``, so the projection could never be run
        again. Called once at startup, before any request is served.

        ``graph_id`` is deliberately kept: a graph created by the dead run is
        still live and billing, and holding the reference is what lets
        delete-graph remove it. ``deleting`` is left alone - a forced
        delete-graph can already take that over.

        Returns the ids that were reset.
        """
        statuses = (STATUS_IMPORTING, STATUS_EXECUTING)
        guard = ", ".join("?" for _ in statuses)
        with connection() as conn:
            rows = conn.execute(
                f"SELECT id FROM projections WHERE status IN ({guard})",
                list(statuses),
            ).fetchall()
            ids = [row["id"] for row in rows]
            if ids:
                conn.execute(
                    f"UPDATE projections SET status = ?, error = ? "
                    f"WHERE status IN ({guard})",
                    [STATUS_FAILED, error, *statuses],
                )
        return ids

    @staticmethod
    def purge_on(conn, projection_id: str, force: bool = False) -> bool:
        """Atomically delete a row only if it is safe to purge.

        A single conditional DELETE: the row goes only when no graph is
        attached and, unless ``force``, the status is not in flight. ``force``
        relaxes the status check only, never the graph check. Runs on a
        caller-supplied connection so it can join a larger transaction.
        """
        sql = (
            "DELETE FROM projections WHERE id = ? "
            "AND (graph_id IS NULL OR graph_id = '')"
        )
        params: list = [projection_id]
        if not force:
            sql += f" AND status NOT IN ({', '.join('?' for _ in IN_FLIGHT_STATUSES)})"
            params += list(IN_FLIGHT_STATUSES)
        return conn.execute(sql, params).rowcount > 0

    @staticmethod
    def _row_to_projection(row) -> Projection:
        return Projection(
            id=row["id"],
            status=row["status"],
            catalog=row["catalog"],
            database=row["database"],
            node_query=row["node_query"],
            edge_query=row["edge_query"],
            property_graph=row["property_graph"],
            graph_name=row["graph_name"],
            graph_memory_gb=row["graph_memory_gb"] or 16,
            s3_staging_bucket=row["s3_staging_bucket"],
            graph_id=row["graph_id"],
            graph_endpoint=row["graph_endpoint"],
            project_id=row["project_id"],
            step=row["step"],
            step_label=row["step_label"],
            progress=row["progress"] or 0,
            error=row["error"],
            owner_token=row["owner_token"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )


# Singleton store instance
store = ProjectionStore()
