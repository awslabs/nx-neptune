# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Workspace:
    id: str
    name: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class WorkspaceStore:
    def __init__(self) -> None:
        self._workspaces: dict[str, Workspace] = {}

    def create(self, name: str) -> Workspace:
        ws = Workspace(id=str(uuid.uuid4()), name=name)
        self._workspaces[ws.id] = ws
        return ws

    def get(self, workspace_id: str) -> Optional[Workspace]:
        return self._workspaces.get(workspace_id)

    def list(self) -> list[Workspace]:
        return list(self._workspaces.values())

    def update(self, workspace_id: str, **kwargs) -> Optional[Workspace]:
        ws = self._workspaces.get(workspace_id)
        if ws is None:
            return None
        for key, value in kwargs.items():
            if hasattr(ws, key):
                setattr(ws, key, value)
        return ws

    def delete(self, workspace_id: str) -> bool:
        return self._workspaces.pop(workspace_id, None) is not None


store = WorkspaceStore()
