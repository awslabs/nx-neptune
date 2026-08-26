# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Path traversal protection on SPA fallback."""

import pytest

from nx_neptune_proxy.app import UI_DIR, app


@pytest.mark.skipif(
    not UI_DIR.exists(),
    reason="ui/ directory not built — run 'make ui' first",
)
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
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "path": path,
            "root_path": "",
            "query_string": b"",
            "headers": [(b"host", b"test")],
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
