# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from dataclasses import dataclass
from typing import Optional

from .db import get_connection


@dataclass
class NodeQuery:
    id: str
    projection_id: str
    sql: str = ""
    position: int = 0

    def to_response(self) -> dict:
        return {"id": self.id, "sql": self.sql, "position": self.position}


@dataclass
class EdgeQuery:
    id: str
    projection_id: str
    sql: str = ""
    from_type: Optional[str] = None
    to_type: Optional[str] = None
    position: int = 0

    def to_response(self) -> dict:
        return {"id": self.id, "sql": self.sql, "from_type": self.from_type, "to_type": self.to_type, "position": self.position}


class QueryStore:
    """Manages node and edge queries for projections."""

    # --- Node Queries ---

    def list_node_queries(self, projection_id: str) -> list[NodeQuery]:
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM node_queries WHERE projection_id = ? ORDER BY position",
            (projection_id,),
        ).fetchall()
        conn.close()
        return [NodeQuery(id=r["id"], projection_id=r["projection_id"], sql=r["sql"], position=r["position"]) for r in rows]

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
        conn = get_connection()
        rows = conn.execute(
            "SELECT * FROM edge_queries WHERE projection_id = ? ORDER BY position",
            (projection_id,),
        ).fetchall()
        conn.close()
        return [
            EdgeQuery(
                id=r["id"], projection_id=r["projection_id"], sql=r["sql"],
                from_type=r["from_type"], to_type=r["to_type"], position=r["position"],
            )
            for r in rows
        ]

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
            from_type = q.get("from_type")
            to_type = q.get("to_type")
            conn.execute(
                "INSERT INTO edge_queries (id, projection_id, sql, from_type, to_type, position) VALUES (?, ?, ?, ?, ?, ?)",
                (qid, projection_id, sql, from_type, to_type, i),
            )
            results.append(EdgeQuery(id=qid, projection_id=projection_id, sql=sql, from_type=from_type, to_type=to_type, position=i))
        conn.commit()
        conn.close()
        return results

    def save_edge_from_payload(self, projection_id: str, models: list) -> None:
        """Variant that accepts Pydantic models directly (calls model_dump internally)."""
        self.save_edge_queries(projection_id, [m.model_dump() for m in models])


# Singleton
query_store = QueryStore()
