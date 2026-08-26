# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Startup bind guard — refuse a non-loopback bind unless acknowledged."""

from unittest import mock

import pytest

from nx_neptune_proxy.__main__ import check_bind_allowed, main
from nx_neptune_proxy.config import Settings


class TestCheckBindAllowed:
    @pytest.mark.parametrize("host", ["127.0.0.1", "::1", "localhost"])
    def test_loopback_always_allowed(self, host):
        assert check_bind_allowed(host, allow_non_loopback=False) is None

    def test_non_loopback_refused_without_override(self):
        error = check_bind_allowed("0.0.0.0", allow_non_loopback=False)
        assert error is not None
        assert "Refusing to start" in error
        assert "ALLOW_NON_LOOPBACK_BIND" in error

    def test_non_loopback_allowed_with_override(self):
        assert check_bind_allowed("0.0.0.0", allow_non_loopback=True) is None

    def test_specific_external_ip_refused_without_override(self):
        assert check_bind_allowed("192.168.1.5", allow_non_loopback=False) is not None


class TestMain:
    def test_main_starts_uvicorn_on_loopback_default(self):
        """Default (no HOST set) is loopback, so uvicorn is launched."""
        with (
            mock.patch.dict("os.environ", {}, clear=True),
            mock.patch("nx_neptune_proxy.__main__.uvicorn.run") as run,
        ):
            main()
            run.assert_called_once()
            assert run.call_args.kwargs["host"] == "127.0.0.1"

    def test_main_refuses_non_loopback_without_override(self):
        with (
            mock.patch.dict("os.environ", {"HOST": "0.0.0.0"}, clear=True),
            mock.patch("nx_neptune_proxy.__main__.uvicorn.run") as run,
        ):
            with pytest.raises(SystemExit):
                main()
            run.assert_not_called()

    def test_main_starts_non_loopback_with_override(self):
        with (
            mock.patch.dict(
                "os.environ",
                {"HOST": "0.0.0.0", "ALLOW_NON_LOOPBACK_BIND": "1"},
                clear=True,
            ),
            mock.patch("nx_neptune_proxy.__main__.uvicorn.run") as run,
        ):
            main()
            run.assert_called_once()
            assert run.call_args.kwargs["host"] == "0.0.0.0"


class TestConfigBindFields:
    def test_defaults_to_loopback_no_override(self):
        settings = Settings()
        assert settings.host == "127.0.0.1"
        assert settings.allow_non_loopback_bind is False

    def test_from_env_reads_host(self, monkeypatch):
        monkeypatch.setenv("HOST", "0.0.0.0")
        assert Settings.from_env().host == "0.0.0.0"

    @pytest.mark.parametrize("value", ["1", "true", "True", "yes", "on"])
    def test_override_truthy_values(self, monkeypatch, value):
        monkeypatch.setenv("ALLOW_NON_LOOPBACK_BIND", value)
        assert Settings.from_env().allow_non_loopback_bind is True

    @pytest.mark.parametrize("value", ["", "0", "false", "False", "no"])
    def test_override_falsy_values(self, monkeypatch, value):
        monkeypatch.setenv("ALLOW_NON_LOOPBACK_BIND", value)
        assert Settings.from_env().allow_non_loopback_bind is False
