# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .db import get_connection


@dataclass
class Project:
    id: str
    name: str
    status: str = "active"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectStore:
    def create(self, name: str) -> Project:
        p = Project(id=str(uuid.uuid4()), name=name)
        conn = get_connection()
        conn.execute(
            "INSERT INTO projects (id, name, status, created_at) VALUES (?, ?, ?, ?)",
            (p.id, p.name, p.status, p.created_at.isoformat()),
        )
        conn.commit()
        conn.close()
        return p

    def get(self, project_id: str) -> Optional[Project]:
        conn = get_connection()
        row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        conn.close()
        return self._row_to_project(row) if row else None

    def list(self) -> list[Project]:
        conn = get_connection()
        rows = conn.execute("SELECT * FROM projects ORDER BY created_at ASC").fetchall()
        conn.close()
        return [self._row_to_project(r) for r in rows]

    def update(self, project_id: str, **kwargs) -> Optional[Project]:
        if not kwargs:
            return self.get(project_id)
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [project_id]
        conn = get_connection()
        conn.execute(f"UPDATE projects SET {sets} WHERE id = ?", vals)
        conn.commit()
        conn.close()
        return self.get(project_id)

    def delete(self, project_id: str) -> bool:
        conn = get_connection()
        cur = conn.execute("DELETE FROM projects WHERE id = ?", (project_id,))
        conn.commit()
        conn.close()
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
