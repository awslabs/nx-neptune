# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .db import connection


@dataclass
class Project:
    id: str
    name: str
    status: str = "active"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectStore:
    def create(self, name: str) -> Project:
        p = Project(id=str(uuid.uuid4()), name=name)
        with connection() as conn:
            conn.execute(
                "INSERT INTO projects (id, name, status, created_at) VALUES (?, ?, ?, ?)",
                (p.id, p.name, p.status, p.created_at.isoformat()),
            )
        return p

    def get(self, project_id: str) -> Optional[Project]:
        with connection() as conn:
            row = conn.execute(
                "SELECT * FROM projects WHERE id = ?", (project_id,)
            ).fetchone()
        return self._row_to_project(row) if row else None

    def list(self) -> list[Project]:
        with connection() as conn:
            rows = conn.execute(
                "SELECT * FROM projects ORDER BY created_at ASC"
            ).fetchall()
        return [self._row_to_project(r) for r in rows]

    _ALLOWED_UPDATE_COLUMNS = {"name", "status"}

    def update(self, project_id: str, **kwargs) -> Optional[Project]:
        if not kwargs:
            return self.get(project_id)
        invalid = set(kwargs.keys()) - self._ALLOWED_UPDATE_COLUMNS
        if invalid:
            raise ValueError(f"Invalid column(s): {invalid}")
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [project_id]
        with connection() as conn:
            conn.execute(f"UPDATE projects SET {sets} WHERE id = ?", vals)
        return self.get(project_id)

    def delete(self, project_id: str) -> bool:
        with connection() as conn:
            cur = conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
            return cur.rowcount > 0

    @staticmethod
    def _row_to_project(row) -> Project:
        return Project(
            id=row["id"],
            name=row["name"],
            status=row["status"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )


store = ProjectStore()
