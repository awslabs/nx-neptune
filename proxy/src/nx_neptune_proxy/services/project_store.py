# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Project:
    id: str
    name: str
    status: str = "active"  # active | deleting
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectStore:
    def __init__(self) -> None:
        self._projects: dict[str, Project] = {}

    def create(self, name: str) -> Project:
        p = Project(id=str(uuid.uuid4()), name=name)
        self._projects[p.id] = p
        return p

    def get(self, project_id: str) -> Optional[Project]:
        return self._projects.get(project_id)

    def list(self) -> list[Project]:
        return list(self._projects.values())

    def update(self, project_id: str, **kwargs) -> Optional[Project]:
        p = self._projects.get(project_id)
        if p is None:
            return None
        for key, value in kwargs.items():
            if hasattr(p, key):
                setattr(p, key, value)
        return p

    def delete(self, project_id: str) -> bool:
        return self._projects.pop(project_id, None) is not None


store = ProjectStore()
