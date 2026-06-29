# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from .db import get_connection


@dataclass
class Workspace:
    id: str
    name: str
    status: str = "active"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class WorkspaceStore:
    def create(self, name: str) -> Workspace:
        ws = Workspace(id=str(uuid.uuid4()), name=name)
        conn = get_connection()
        conn.execute(
            "INSERT INTO workspaces (id, name, status, created_at) VALUES (?, ?, ?, ?)",
            (ws.id, ws.name, ws.status, ws.created_at.isoformat()),
        )
        conn.commit()
        conn.close()
        return ws

    def get(self, workspace_id: str) -> Optional[Workspace]:
        conn = get_connection()
        row = conn.execute("SELECT * FROM workspaces WHERE id = ?", (workspace_id,)).fetchone()
        conn.close()
        return self._row_to_workspace(row) if row else None

    def list(self) -> list[Workspace]:
        conn = get_connection()
        rows = conn.execute("SELECT * FROM workspaces ORDER BY created_at DESC").fetchall()
        conn.close()
        return [self._row_to_workspace(r) for r in rows]

    def update(self, workspace_id: str, **kwargs) -> Optional[Workspace]:
        if not kwargs:
            return self.get(workspace_id)
        sets = ", ".join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [workspace_id]
        conn = get_connection()
        conn.execute(f"UPDATE workspaces SET {sets} WHERE id = ?", vals)
        conn.commit()
        conn.close()
        return self.get(workspace_id)

    def delete(self, workspace_id: str) -> bool:
        conn = get_connection()
        cur = conn.execute("DELETE FROM workspaces WHERE id = ?", (workspace_id,))
        conn.commit()
        conn.close()
        return cur.rowcount > 0

    @staticmethod
    def _row_to_workspace(row) -> Workspace:
        return Workspace(
            id=row["id"],
            name=row["name"],
            status=row["status"],
            created_at=datetime.fromisoformat(row["created_at"]),
        )


store = WorkspaceStore()
