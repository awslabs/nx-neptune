# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import socket
import stat
import time
from pathlib import Path
from unittest import mock

import pytest

from nx_neptune_proxy import browser
from nx_neptune_proxy.config import Settings

URL = "http://127.0.0.1:8080/?token=abc&x=<y>"


@pytest.fixture
def listening_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        s.listen()
        yield s.getsockname()[1]


@pytest.fixture
def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class TestOpenBrowserSetting:
    def test_disabled_by_default(self, monkeypatch):
        monkeypatch.delenv("OPEN_BROWSER", raising=False)
        assert Settings.from_env().open_browser is False

    @pytest.mark.parametrize("value", ["true", "TRUE", "1", "yes"])
    def test_enabled_values(self, monkeypatch, value):
        monkeypatch.setenv("OPEN_BROWSER", value)
        assert Settings.from_env().open_browser is True

    @pytest.mark.parametrize("value", ["false", "0", "no", "", "maybe"])
    def test_other_values_stay_disabled(self, monkeypatch, value):
        monkeypatch.setenv("OPEN_BROWSER", value)
        assert Settings.from_env().open_browser is False

    def test_dataclass_default_is_off(self):
        assert Settings().open_browser is False


class TestRedirectPage:
    def test_page_is_private_and_redirects_to_escaped_url(self):
        page = browser._write_redirect_page(URL)
        try:
            assert stat.S_IMODE(page.stat().st_mode) == 0o600
            body = page.read_text()
            assert 'content="0;url=http://127.0.0.1:8080/?token=abc&amp;x=&lt;y&gt;"' in body
            assert "<y>" not in body
        finally:
            page.unlink()


class TestOpenWhenReady:
    def test_opens_redirect_file_not_the_token_url(self, listening_port):
        with mock.patch.object(browser.webbrowser, "open", return_value=True) as opened:
            browser._open_when_ready(URL, "127.0.0.1", listening_port)
        opened.assert_called_once()
        target = opened.call_args.args[0]
        assert target.startswith("file://")
        assert "token" not in target

    def test_does_not_open_when_nothing_listens(self, free_port, monkeypatch):
        monkeypatch.setattr(browser, "_WAIT_SECONDS", 0.3)
        with mock.patch.object(browser.webbrowser, "open") as opened:
            browser._open_when_ready(URL, "127.0.0.1", free_port)
        opened.assert_not_called()

    def test_browser_failure_does_not_raise(self, listening_port):
        with mock.patch.object(browser.webbrowser, "open", side_effect=RuntimeError("x")):
            browser._open_when_ready(URL, "127.0.0.1", listening_port)


class TestRedirectPageCleanup:
    def _opened_path(self, opened):
        from urllib.parse import unquote, urlparse

        return Path(unquote(urlparse(opened.call_args.args[0]).path))

    def test_page_removed_after_delay(self, listening_port, monkeypatch):
        monkeypatch.setattr(browser, "_CLEANUP_SECONDS", 0.2)
        with mock.patch.object(browser.webbrowser, "open", return_value=True) as opened:
            browser._open_when_ready(URL, "127.0.0.1", listening_port)
        page = self._opened_path(opened)
        assert page.exists()
        deadline = time.monotonic() + 3
        while page.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert not page.exists()

    def test_page_removed_immediately_when_no_browser(self, listening_port):
        with mock.patch.object(browser.webbrowser, "open", return_value=False) as opened:
            browser._open_when_ready(URL, "127.0.0.1", listening_port)
        assert not self._opened_path(opened).exists()

    def test_page_removed_immediately_when_browser_raises(self, listening_port):
        written = []
        real_write = browser._write_redirect_page

        def spy(url):
            written.append(real_write(url))
            return written[-1]

        with mock.patch.object(browser, "_write_redirect_page", side_effect=spy), \
                mock.patch.object(browser.webbrowser, "open", side_effect=RuntimeError("x")):
            browser._open_when_ready(URL, "127.0.0.1", listening_port)
        assert written and not written[0].exists()
