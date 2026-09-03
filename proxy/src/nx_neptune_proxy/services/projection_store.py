# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from .db import connection

_FIELDS = [
    "id",
    "status",
    "catalog",
    "database",
    "node_query",
    "edge_query",
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
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectionStore:
    def create(
        self,
        catalog: str = "AwsDataCatalog",
        database: Optional[str] = None,
        node_query: Optional[str] = None,
        edge_query: Optional[str] = None,
        graph_name: Optional[str] = None,
        graph_memory_gb: int = 16,
        s3_staging_bucket: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> Projection:
        p = Projection(
            id=str(uuid.uuid4()),
            status="draft",
            catalog=catalog,
            database=database,
            node_query=node_query,
            edge_query=edge_query,
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

    def delete(self, projection_id: str) -> bool:
        with connection() as conn:
            cur = conn.execute("DELETE FROM projections WHERE id = ?", (projection_id,))
            return cur.rowcount > 0

    @staticmethod
    def _row_to_projection(row) -> Projection:
        return Projection(
            id=row["id"],
            status=row["status"],
            catalog=row["catalog"],
            database=row["database"],
            node_query=row["node_query"],
            edge_query=row["edge_query"],
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
            created_at=datetime.fromisoformat(row["created_at"]),
        )


# Singleton store instance
store = ProjectionStore()
