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


@dataclass
class EdgeQuery:
    id: str
    projection_id: str
    sql: str = ""
    from_type: Optional[str] = None
    to_type: Optional[str] = None
    position: int = 0


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


# Singleton
query_store = QueryStore()
