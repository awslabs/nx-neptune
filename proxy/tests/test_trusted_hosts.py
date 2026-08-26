# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Settings.trusted_hosts — independent of allowed_origins/CORS."""

from nx_neptune_proxy.config import Settings


class TestTrustedHosts:
    def test_defaults_to_loopback_only(self):
        settings = Settings()
        assert settings.trusted_hosts == {"127.0.0.1", "::1", "localhost"}

    def test_extra_trusted_hosts_is_additive(self):
        settings = Settings(extra_trusted_hosts=frozenset({"example.com"}))
        assert settings.trusted_hosts == {
            "127.0.0.1",
            "::1",
            "localhost",
            "example.com",
        }

    def test_from_env_reads_trusted_hosts(self, monkeypatch):
        monkeypatch.setenv("TRUSTED_HOSTS", "example.com, foo.local")
        settings = Settings.from_env()
        assert settings.extra_trusted_hosts == {"example.com", "foo.local"}
        assert settings.trusted_hosts == {
            "127.0.0.1",
            "::1",
            "localhost",
            "example.com",
            "foo.local",
        }

    def test_independent_of_cors_allowed_origins(self, monkeypatch):
        """trusted_hosts must not be affected by CORS_ALLOWED_ORIGINS in
        either direction — the two lists are unrelated."""
        monkeypatch.setenv("CORS_ALLOWED_ORIGINS", "https://example.com:888")
        settings = Settings.from_env()
        assert settings.trusted_hosts == {"127.0.0.1", "::1", "localhost"}
        assert settings.allowed_origins == ["https://example.com:888"]

    def test_allowed_origins_still_defaults_to_empty(self):
        """Unchanged pre-existing behaviour: no fallback/derivation."""
        settings = Settings()
        assert settings.allowed_origins == []
