# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Settings.trusted_hosts — derived from the CORS origins list + loopback."""

from nx_neptune_proxy.config import Settings


class TestTrustedHosts:
    def test_defaults_to_loopback_only(self):
        settings = Settings()
        assert settings.trusted_hosts == {"127.0.0.1", "::1", "localhost"}

    def test_trusted_hosts_can_be_constructed_directly(self):
        settings = Settings(
            trusted_hosts=frozenset({"127.0.0.1", "::1", "localhost", "example.com"})
        )
        assert settings.trusted_hosts == {
            "127.0.0.1",
            "::1",
            "localhost",
            "example.com",
        }

    def test_trusted_hosts_derived_from_origins(self, monkeypatch):
        """The Host allowlist derives its hostnames from the configured
        origins, so a browser origin is trusted for the Host check with no
        separate configuration."""
        monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "https://example.com:888")
        settings = Settings.from_env()
        assert settings.trusted_hosts == {
            "127.0.0.1",
            "::1",
            "localhost",
            "example.com",
        }
        assert settings.allowed_origins == ["https://example.com:888"]

    def test_derived_host_is_lowercased(self, monkeypatch):
        """Hostnames are case-insensitive; a mixed-case origin host is
        normalized to lowercase so it matches the lowercase Host a client
        sends."""
        monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "https://Example.COM")
        settings = Settings.from_env()
        assert settings.trusted_hosts == {
            "127.0.0.1",
            "::1",
            "localhost",
            "example.com",
        }

    def test_origins_normalized_default_port_stripped(self, monkeypatch):
        """Default ports are dropped so https://x and https://x:443 are equal;
        scheme/host are lowercased."""
        monkeypatch.setenv(
            "CORS_ALLOWED_ORIGINS", "HTTPS://Example.COM:443, http://foo.local:80"
        )
        settings = Settings.from_env()
        assert settings.allowed_origins == [
            "https://example.com",
            "http://foo.local",
        ]

    def test_allowed_origins_defaults_to_empty(self):
        settings = Settings()
        assert settings.allowed_origins == []
