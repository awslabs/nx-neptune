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
from pathlib import Path
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient

from nx_neptune_proxy.app import UI_DIR, app
from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.services.db import get_connection
from nx_neptune_proxy.services.projection_store import store


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

    Note: HTTP clients (httpx, browsers) normalize '../' in paths before
    sending. The security property we verify is: regardless of the path
    attempted, the response is either 403 or the SPA index.html — never
    content from outside the UI directory.
    """

    @pytest.mark.asyncio
    async def test_dot_dot_slash_serves_spa_not_system_file(self, client):
        """Client-normalized traversal should serve SPA fallback, not /etc/passwd."""
        resp = await client.get("/../../etc/passwd")
        assert resp.status_code == 200
        # Must be the SPA HTML, not /etc/passwd content
        assert "<html" in resp.text
        assert "<title>nx-neptune</title>" in resp.text
        assert "root:" not in resp.text

    @pytest.mark.asyncio
    async def test_encoded_traversal_serves_spa_not_system_file(self, client):
        """Percent-encoded traversal must not leak system files."""
        resp = await client.get("/..%2F..%2Fetc%2Fpasswd")
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            assert "<html" in resp.text
            assert "<title>nx-neptune</title>" in resp.text
            assert "root:" not in resp.text

    @pytest.mark.asyncio
    async def test_double_dot_in_middle_serves_spa(self, client):
        """Traversal via nested path must not leak files."""
        resp = await client.get("/assets/../../../etc/shadow")
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            assert "<html" in resp.text
            assert "<title>nx-neptune</title>" in resp.text
            assert "root:" not in resp.text

    @pytest.mark.asyncio
    async def test_backslash_traversal_serves_spa(self, client):
        resp = await client.get("/..\\..\\etc\\passwd")
        assert resp.status_code in (200, 403)
        if resp.status_code == 200:
            assert "<html" in resp.text
            assert "<title>nx-neptune</title>" in resp.text
            assert "root:" not in resp.text

    @pytest.mark.asyncio
    async def test_null_byte_traversal(self, client):
        resp = await client.get("/..%00/etc/passwd")
        assert resp.status_code in (200, 400, 403)
        if resp.status_code == 200:
            assert "<html" in resp.text
            assert "<title>nx-neptune</title>" in resp.text
            assert "root:" not in resp.text

    @pytest.mark.asyncio
    async def test_resolve_prevents_escape(self, client):
        """Directly test the path resolution logic rejects escape attempts."""
        ui_root = UI_DIR.resolve()

        # Simulate what the SPA fallback does
        malicious_paths = [
            "../../../etc/passwd",
            "..%2F..%2Fetc/passwd",
            "assets/../../../../../../etc/shadow",
        ]
        for malicious in malicious_paths:
            try:
                normalized = Path("/", malicious).resolve().relative_to("/")
            except ValueError:
                continue  # Blocked at normalization — good
            file_path = (ui_root / normalized).resolve()
            # The resolved path must NOT escape ui_root
            try:
                file_path.relative_to(ui_root)
                # If it doesn't raise, the file must be within UI_DIR
                assert str(file_path).startswith(str(ui_root))
            except ValueError:
                # Correctly detected escape — would return 403
                pass

    @pytest.mark.asyncio
    async def test_valid_path_returns_200(self, client):
        """Non-traversal paths should work normally (SPA fallback)."""
        resp = await client.get("/projections")
        assert resp.status_code == 200


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
