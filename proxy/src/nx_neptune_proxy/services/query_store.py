# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from dataclasses import dataclass

from .db import get_connection, query


@dataclass
class NodeQuery:
    id: str
    projection_id: str
    sql: str = ""
    position: int = 0

    @classmethod
    def from_row(cls, row) -> "NodeQuery":
        return cls(id=row["id"], projection_id=row["projection_id"], sql=row["sql"], position=row["position"])

    def to_response(self) -> dict:
        return {"id": self.id, "sql": self.sql, "position": self.position}


@dataclass
class EdgeQuery:
    id: str
    projection_id: str
    sql: str = ""
    position: int = 0

    @classmethod
    def from_row(cls, row) -> "EdgeQuery":
        return cls(id=row["id"], projection_id=row["projection_id"], sql=row["sql"], position=row["position"])

    def to_response(self) -> dict:
        return {"id": self.id, "sql": self.sql, "position": self.position}


class QueryStore:
    """Manages node and edge queries for projections."""

    # --- Node Queries ---

    def list_node_queries(self, projection_id: str) -> list[NodeQuery]:
        rows = query("SELECT * FROM node_queries WHERE projection_id = ? ORDER BY position", (projection_id,))
        return [NodeQuery.from_row(r) for r in rows]

    def list_node_responses(self, projection_id: str) -> list[dict]:
        """Same as list_node_queries but returns API-ready dicts (excludes projection_id)."""
        return [q.to_response() for q in self.list_node_queries(projection_id)]

    def save_node_queries(self, projection_id: str, queries: list[dict]) -> list[NodeQuery]:
        """Replace all node queries for a projection."""
        conn = get_connection()
        conn.execute("DELETE FROM node_queries WHERE projection_id = ?", (projection_id,))
        results = []
        for i, q in enumerate(queries):
            qid = q.get("id") or str(uuid.uuid4())
            sql = q.get("sql", "")
            conn.execute(
                "INSERT INTO node_queries (id, projection_id, sql, position) VALUES (?, ?, ?, ?)",
                (qid, projection_id, sql, i),
            )
            results.append(NodeQuery(id=qid, projection_id=projection_id, sql=sql, position=i))
        conn.commit()
        conn.close()
        return results

    def save_node_from_payload(self, projection_id: str, models: list) -> None:
        """Variant that accepts Pydantic models directly (calls model_dump internally)."""
        self.save_node_queries(projection_id, [m.model_dump() for m in models])

    # --- Edge Queries ---

    def list_edge_queries(self, projection_id: str) -> list[EdgeQuery]:
        rows = query("SELECT * FROM edge_queries WHERE projection_id = ? ORDER BY position", (projection_id,))
        return [EdgeQuery.from_row(r) for r in rows]

    def list_edge_responses(self, projection_id: str) -> list[dict]:
        """Same as list_edge_queries but returns API-ready dicts (excludes projection_id)."""
        return [q.to_response() for q in self.list_edge_queries(projection_id)]

    def save_edge_queries(self, projection_id: str, queries: list[dict]) -> list[EdgeQuery]:
        """Replace all edge queries for a projection."""
        conn = get_connection()
        conn.execute("DELETE FROM edge_queries WHERE projection_id = ?", (projection_id,))
        results = []
        for i, q in enumerate(queries):
            qid = q.get("id") or str(uuid.uuid4())
            sql = q.get("sql", "")
            conn.execute(
                "INSERT INTO edge_queries (id, projection_id, sql, position) VALUES (?, ?, ?, ?)",
                (qid, projection_id, sql, i),
            )
            results.append(EdgeQuery(id=qid, projection_id=projection_id, sql=sql, position=i))
        conn.commit()
        conn.close()
        return results

    def save_edge_from_payload(self, projection_id: str, models: list) -> None:
        """Variant that accepts Pydantic models directly (calls model_dump internally)."""
        self.save_edge_queries(projection_id, [m.model_dump() for m in models])


# Singleton
query_store = QueryStore()
