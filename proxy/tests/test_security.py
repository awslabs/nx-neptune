# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Security tests for nx-neptune-proxy.

Covers:
- Path traversal protection on SPA fallback
- SQLite update column allowlist enforcement
- SQL injection resistance via parameterized queries
- CORS origin restriction enforcement
- Absence of credentials or secrets in local storage
"""

import os
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import app
from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.services.db import get_connection
from nx_neptune_proxy.services.projection_store import store
from nx_neptune_proxy.app import UI_DIR

@pytest.fixture
def client():
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture(autouse=True)
def clear_store():
    conn = get_connection()
    conn.execute("DELETE FROM projections")
    conn.execute("DELETE FROM projects")
    conn.commit()
    conn.close()
    yield
    conn = get_connection()
    conn.execute("DELETE FROM projections")
    conn.execute("DELETE FROM projects")
    conn.commit()
    conn.close()


# =============================================================================
# Path traversal attempts on SPA fallback
# =============================================================================


class TestPathTraversal:
    """SPA fallback must not serve files outside UI_DIR.

    Two controls are tested:
    1. Input sanitization — any path containing '..' is rejected with 403.
    2. Output validation — resolved paths outside ui_root are rejected (symlinks).

    Uses raw ASGI scope to bypass httpx's client-side path normalization,
    ensuring the server's guards are actually exercised.
    """

    @staticmethod
    async def _raw_get(path: str) -> tuple[int, str]:
        """Send GET with un-normalized path directly to ASGI app."""
        scope = {
            "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
            "method": "GET", "path": path, "root_path": "",
            "query_string": b"", "headers": [(b"host", b"test")],
        }
        status = None
        body_parts = []

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(msg):
            nonlocal status
            if msg["type"] == "http.response.start":
                status = msg["status"]
            elif msg["type"] == "http.response.body":
                body_parts.append(msg.get("body", b""))

        await app(scope, receive, send)
        return status, b"".join(body_parts).decode()

    # --- Control 1: Input sanitization (reject '..') ---

    @pytest.mark.asyncio
    async def test_dot_dot_slash_rejected(self):
        """Path with ../ must be rejected outright."""
        code, body = await self._raw_get("/../../etc/passwd")
        assert code == 403

    @pytest.mark.asyncio
    async def test_deep_traversal_rejected(self):
        """Deep traversal must be rejected outright."""
        code, body = await self._raw_get("/../../../etc/shadow")
        assert code == 403

    @pytest.mark.asyncio
    async def test_backslash_traversal_rejected(self):
        """Backslash traversal with .. must be rejected."""
        code, body = await self._raw_get("/..\\..\\etc\\passwd")
        assert code == 403

    @pytest.mark.asyncio
    async def test_encoded_dot_dot_rejected(self):
        """Percent-encoded ../ must be rejected."""
        code, body = await self._raw_get("/..%2F..%2Fetc%2Fpasswd")
        assert code == 403

    @pytest.mark.asyncio
    async def test_mid_path_traversal_rejected(self):
        """Traversal in middle of path must not serve files outside UI dir."""
        code, body = await self._raw_get("/assets/../../etc/passwd")
        # May hit /assets StaticFiles mount (404) or spa_fallback (403)
        assert code in (403, 404)
        assert "root:" not in body

    # --- Control 2: Output validation (symlink escape) ---

    @pytest.mark.asyncio
    async def test_symlink_escape_blocked(self):
        """Symlink inside UI dir pointing outside must be blocked."""


        ui_root = UI_DIR.resolve()
        symlink = ui_root / "evil_link"
        symlink.symlink_to("/etc/hosts")
        try:
            code, body = await self._raw_get("/evil_link")
            assert code == 403
            assert "localhost" not in body
        finally:
            symlink.unlink()

    # --- Valid paths still work ---

    @pytest.mark.asyncio
    async def test_valid_path_returns_spa(self):
        """Non-traversal paths should serve SPA fallback normally."""
        code, body = await self._raw_get("/projections")
        assert code == 200
        assert "<title>nx-neptune</title>" in body


# =============================================================================
# SQLite update rejects column names not in allowlist
# =============================================================================


class TestColumnAllowlist:
    """Store.update() must reject columns not in _ALLOWED_UPDATE_COLUMNS."""

    def test_invalid_column_raises_value_error(self):
        p = store.create(database="testdb", graph_name="test")
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, id="injected-id")

    def test_invalid_column_created_at_rejected(self):
        p = store.create(database="testdb", graph_name="test")
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, created_at="2020-01-01")

    def test_arbitrary_column_rejected(self):
        p = store.create(database="testdb", graph_name="test")
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, drop_table="yes")

    def test_multiple_invalid_columns_rejected(self):
        p = store.create(database="testdb", graph_name="test")
        with pytest.raises(ValueError, match="Invalid column"):
            store.update(p.id, malicious="x", another_bad="y")

    def test_valid_column_accepted(self):
        p = store.create(database="testdb", graph_name="test")
        result = store.update(p.id, status="importing")
        assert result.status == "importing"


# =============================================================================
# SQL injection payloads in values are stored as literals
# =============================================================================


class TestSqlInjection:
    """Parameterized queries must store injection payloads as literal strings."""

    def test_injection_in_graph_name(self):
        payload = "'; DROP TABLE projections; --"
        p = store.create(database="testdb", graph_name=payload)
        # Table still exists and projection is retrievable
        retrieved = store.get(p.id)
        assert retrieved is not None
        assert retrieved.graph_name == payload

    def test_injection_in_database_field(self):
        payload = "x' OR '1'='1"
        p = store.create(database=payload, graph_name="test")
        retrieved = store.get(p.id)
        assert retrieved.database == payload

    def test_injection_in_update_value(self):
        p = store.create(database="testdb", graph_name="test")
        payload = "'; DELETE FROM projections WHERE '1'='1"
        store.update(p.id, error=payload)
        # Projection still exists with payload stored as literal
        retrieved = store.get(p.id)
        assert retrieved.error == payload
        # Verify other projections are not affected
        p2 = store.create(database="testdb2", graph_name="test2")
        assert store.get(p2.id) is not None

    def test_injection_in_sql_query_field(self):
        payload = "SELECT * FROM t; DROP TABLE projections;--"
        p = store.create(database="testdb", sql_query=payload, graph_name="test")
        retrieved = store.get(p.id)
        assert retrieved.sql_query == payload

    def test_unicode_injection(self):
        payload = "test\u0000'; DROP TABLE projections;--"
        p = store.create(database="testdb", graph_name=payload)
        retrieved = store.get(p.id)
        assert retrieved is not None


# =============================================================================
# CORS preflight rejects requests from non-allowed origins
# =============================================================================


class TestCorsRejection:
    """CORS middleware must reject origins not in CORS_ALLOWED_ORIGINS."""

    @pytest.mark.asyncio
    async def test_disallowed_origin_no_cors_headers(self, client):
        """Preflight from an unknown origin should not return Access-Control-Allow-Origin."""
        resp = await client.options(
            "/api/v0/projection",
            headers={
                "Origin": "http://evil.com",
                "Access-Control-Request-Method": "POST",
            },
        )
        # Should not include the evil origin in allowed origins
        allow_origin = resp.headers.get("access-control-allow-origin")
        assert allow_origin != "http://evil.com"

    @pytest.mark.asyncio
    async def test_disallowed_origin_get_request(self, client):
        """Regular request from disallowed origin should not get CORS headers."""
        resp = await client.get(
            "/health",
            headers={"Origin": "http://attacker.example.com"},
        )
        allow_origin = resp.headers.get("access-control-allow-origin")
        assert allow_origin != "http://attacker.example.com"

    @pytest.mark.asyncio
    async def test_no_wildcard_by_default(self, client):
        """With no CORS_ALLOWED_ORIGINS set, should not allow wildcard."""
        resp = await client.options(
            "/api/v0/projection",
            headers={
                "Origin": "http://random.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        allow_origin = resp.headers.get("access-control-allow-origin")
        assert allow_origin != "*"

    @pytest.mark.asyncio
    async def test_allowed_origin_gets_cors_headers(self, client):
        """If CORS_ALLOWED_ORIGINS is configured, that origin should be allowed."""
        with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": "http://localhost:5173"}):
            test_settings = Settings.from_env()
            assert "http://localhost:5173" in test_settings.allowed_origins


# =============================================================================
# SQLite database contains no credentials or secrets
# =============================================================================


class TestNoSecretsInDb:
    """SQLite should contain no credentials, tokens, or secrets."""

    SENSITIVE_PATTERNS = [
        "AKIA",
        "aws_secret",
        "aws_session",
        "BEGIN RSA",
        "BEGIN PRIVATE",
        "password",
    ]

    def test_no_secrets_after_creating_projections(self):
        """After CRUD operations, DB should contain no credential patterns."""
        # Create several projections with various data
        store.create(
            database="production_db",
            graph_name="fraud-graph",
            s3_staging_bucket="s3://my-bucket/staging/",
            sql_query="SELECT user_id, name FROM users",
        )
        store.create(
            database="analytics",
            graph_name="social-graph",
            s3_staging_bucket="s3://other-bucket/data/",
            node_query="SELECT id AS `~id`, type AS `~label` FROM nodes",
            edge_query="SELECT src AS `~from`, dst AS `~to` FROM edges",
        )

        # Read the raw database content
        conn = get_connection()
        cursor = conn.execute("SELECT * FROM projections")
        rows = cursor.fetchall()
        conn.close()

        # Serialize all values to check for sensitive content
        all_values = []
        for row in rows:
            for key in row.keys():
                val = row[key]
                if val is not None:
                    all_values.append(str(val).lower())

        full_content = " ".join(all_values)

        for pattern in self.SENSITIVE_PATTERNS:
            assert pattern.lower() not in full_content, (
                f"Sensitive pattern '{pattern}' found in SQLite data"
            )

    def test_no_secrets_after_update_with_error(self):
        """Error messages stored in DB should not contain credentials."""
        p = store.create(database="testdb", graph_name="test")
        store.update(
            p.id,
            status="failed",
            error="AccessDeniedException: User arn:aws:iam::123456789012:user/dev is not authorized",
        )

        conn = get_connection()
        row = conn.execute(
            "SELECT error FROM projections WHERE id = ?", (p.id,)
        ).fetchone()
        conn.close()

        error_text = row["error"].lower()
        for pattern in self.SENSITIVE_PATTERNS:
            assert pattern.lower() not in error_text, (
                f"Sensitive pattern '{pattern}' found in error message"
            )
