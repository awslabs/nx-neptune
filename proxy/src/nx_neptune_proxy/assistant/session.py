# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""In-memory assistant session store (spec §9.9).

Each session keeps the conversation history plus a **discovery cache** keyed by
``(catalog, database)`` — so a follow-up turn against the same database reuses
the discovered schema instead of re-reading Athena metadata, and discovery
re-runs only when the target database changes. In-memory is sufficient for the
prototype (single process); a durable store is a later concern.
"""

import uuid
from dataclasses import dataclass, field
from typing import Optional

from nx_neptune_proxy.assistant.schemas import DiscoveryResult


@dataclass
class Session:
    session_id: str
    history: list[dict] = field(default_factory=list)
    # (catalog, database) -> discovered schema for this session.
    discovery_cache: dict[tuple[str, str], DiscoveryResult] = field(
        default_factory=dict
    )

    def add_turn(self, role: str, text: str) -> None:
        self.history.append({"role": role, "text": text})

    def get_discovery(self, catalog: str, database: str) -> Optional[DiscoveryResult]:
        return self.discovery_cache.get((catalog, database))

    def cache_discovery(
        self, catalog: str, database: str, result: DiscoveryResult
    ) -> None:
        self.discovery_cache[(catalog, database)] = result


class SessionStore:
    """Process-local map of ``session_id`` → :class:`Session`."""

    def __init__(self):
        self._sessions: dict[str, Session] = {}

    def create(self) -> Session:
        session = Session(session_id=str(uuid.uuid4()))
        self._sessions[session.session_id] = session
        return session

    def get(self, session_id: str) -> Optional[Session]:
        return self._sessions.get(session_id)

    def get_or_create(self, session_id: Optional[str]) -> Session:
        if session_id and session_id in self._sessions:
            return self._sessions[session_id]
        session = Session(session_id=session_id or str(uuid.uuid4()))
        self._sessions[session.session_id] = session
        return session
