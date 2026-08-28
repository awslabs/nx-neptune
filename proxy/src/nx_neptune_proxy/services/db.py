# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DB_PATH = os.environ.get(
    "NX_NEPTUNE_DB_PATH", str(Path.home() / ".nx-neptune" / "proxy.db")
)


def get_connection() -> sqlite3.Connection:
    # TODO: Consider a connection pool or a singleton connection with thread-safety
    #       for improved performance under concurrent requests.
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def connection() -> Iterator[sqlite3.Connection]:
    """Yield a connection, committing on success and always closing.

    Using this instead of calling ``get_connection()`` directly guarantees the
    connection is closed even when a query raises (e.g. an IntegrityError),
    preventing leaked connections that hold SQLite locks.
    """
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def query(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    """Run a read query and return all rows, always closing the connection."""
    with connection() as conn:
        return conn.execute(sql, params).fetchall()


def init_db() -> None:
    with connection() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS projections (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'draft',
            catalog TEXT DEFAULT 'AwsDataCatalog',
            database TEXT,
            node_query TEXT,
            edge_query TEXT,
            graph_name TEXT,
            graph_memory_gb INTEGER DEFAULT 16,
            s3_staging_bucket TEXT,
            graph_id TEXT,
            graph_endpoint TEXT,
            project_id TEXT NOT NULL,
            step TEXT,
            step_label TEXT,
            progress REAL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        );
        CREATE TABLE IF NOT EXISTS node_queries (
            id TEXT PRIMARY KEY,
            projection_id TEXT NOT NULL,
            sql TEXT NOT NULL DEFAULT '',
            position INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (projection_id) REFERENCES projections(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS edge_queries (
            id TEXT PRIMARY KEY,
            projection_id TEXT NOT NULL,
            sql TEXT NOT NULL DEFAULT '',
            position INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (projection_id) REFERENCES projections(id) ON DELETE CASCADE
        );
    """)
