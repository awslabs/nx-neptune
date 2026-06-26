# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import sqlite3
from pathlib import Path

DB_PATH = os.environ.get("NX_NEPTUNE_DB_PATH", str(Path.home() / ".nx-neptune" / "proxy.db"))


def get_connection() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS workspaces (
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
            sql_query TEXT,
            node_query TEXT,
            edge_query TEXT,
            graph_name TEXT,
            graph_memory_gb INTEGER DEFAULT 16,
            s3_staging_bucket TEXT,
            graph_id TEXT,
            graph_endpoint TEXT,
            workspace_id TEXT,
            step TEXT,
            step_label TEXT,
            progress REAL DEFAULT 0,
            error TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (workspace_id) REFERENCES workspaces(id)
        );
    """)
    conn.commit()
    conn.close()
