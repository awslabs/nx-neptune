# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sqlite3
from pathlib import Path

DB_PATH = os.environ.get("NX_NEPTUNE_DB_PATH", str(Path.home() / ".nx-neptune" / "proxy.db"))


def get_connection() -> sqlite3.Connection:
    # TODO: Consider a connection pool or a singleton connection with thread-safety
    #       for improved performance under concurrent requests.
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def query(sql: str, params: tuple = ()) -> list:
    """Execute a read query and return all rows. Handles connection lifecycle."""
    conn = get_connection()
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def init_db() -> None:
    conn = get_connection()
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
            graph_name TEXT,
            graph_memory_gb INTEGER DEFAULT 16,
            s3_staging_bucket TEXT,
            graph_id TEXT,
            graph_endpoint TEXT,
            project_id TEXT,
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
    conn.commit()
    conn.close()
