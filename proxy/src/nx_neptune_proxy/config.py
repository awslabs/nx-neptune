# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from dataclasses import dataclass
from urllib.parse import urlparse

# Always trusted, regardless of configuration: requests claiming to be from
# here never leave the local machine.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

# Default ports that are implied by the scheme and should be dropped when
# normalizing an origin, so "https://x" and "https://x:443" compare equal.
_DEFAULT_PORTS = {"http": 80, "https": 443}


def normalize_origin(origin: str) -> str | None:
    """Normalize an origin to a canonical scheme://host[:port] form.

    Lowercases scheme and host, drops the default port for the scheme, and
    strips any path/query. Returns None if the value can't be parsed as an
    origin with both a scheme and a host.
    """
    try:
        parsed = urlparse(origin.strip())
    except Exception:
        return None
    scheme = (parsed.scheme or "").lower()
    host = (parsed.hostname or "").lower()
    if not scheme or not host:
        return None
    port = parsed.port
    if port is not None and _DEFAULT_PORTS.get(scheme) == port:
        port = None
    return f"{scheme}://{host}" if port is None else f"{scheme}://{host}:{port}"


@dataclass(frozen=True)
class Settings:
    """Application settings loaded from environment variables.

    ``allowed_origins`` and ``trusted_hosts`` are both derived once, in
    ``from_env``, from the single CORS origins list. ``allowed_origins``
    holds full origins (for CORS and the server-side Origin check);
    ``trusted_hosts`` holds the hostnames extracted from those origins plus
    the loopback set (for the Host check). Consumers read them as-is.
    """

    log_level: str = "INFO"
    allowed_origins: list[str] = None  # type: ignore[assignment]
    host: str = "127.0.0.1"
    trusted_hosts: frozenset[str] = None  # type: ignore[assignment]
    port: int = 8080
    allow_non_loopback_bind: bool = False
    extra_trusted_hosts: frozenset[str] = field(default_factory=frozenset)
    region: str = ""
    graph_prefix: str = "nxp-"

    def __post_init__(self) -> None:
        if self.allowed_origins is None:
            object.__setattr__(self, "allowed_origins", [])
        if self.trusted_hosts is None:
            object.__setattr__(self, "trusted_hosts", _LOOPBACK_HOSTS)

    @classmethod
    def from_env(cls) -> "Settings":
        # Single source of truth for browser origins. From this one list we
        # derive both outputs, once, here:
        #   - allowed_origins: normalized full origins (CORS + Origin check)
        #   - trusted_hosts:   the origins' hostnames + loopback (Host check)
        origins_raw = os.environ.get("CORS_ALLOWED_ORIGINS", "")
        origins: list[str] = []
        for o in origins_raw.split(","):
            normalized = normalize_origin(o)
            if normalized and normalized not in origins:
                origins.append(normalized)

        # Hostnames trusted in the Host header: those of the configured
        # origins, plus the loopback floor (this tool is intended to run on
        # localhost or in Docker, so loopback covers the normal case).
        origin_hosts = frozenset(
            h for h in (urlparse(o).hostname for o in origins) if h
        )
        trusted_hosts_raw = os.environ.get("TRUSTED_HOSTS", "")
        # Hostnames are case-insensitive; normalize to lowercase so a
        # mixed-case TRUSTED_HOSTS value (e.g. "Example.COM") matches the
        # conventionally lowercase Host/Origin a client sends.
        extra_trusted = frozenset(
            h.strip().lower() for h in trusted_hosts_raw.split(",") if h.strip()
        )
        allow_raw = os.environ.get("ALLOW_NON_LOOPBACK_BIND", "")
        trusted_hosts = _LOOPBACK_HOSTS | origin_hosts

        return cls(
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            allowed_origins=origins,
            host=os.environ.get("HOST", "127.0.0.1"),
            trusted_hosts=trusted_hosts,
            port=int(os.environ.get("PORT", "8080")),
            extra_trusted_hosts=extra_trusted,
            allow_non_loopback_bind=allow_raw.strip().lower()
            not in ("", "0", "false", "no"),
            region=os.environ.get(
                "AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "")
            ),
            graph_prefix=os.environ.get("GRAPH_PREFIX", "nxp-"),
        )

    def validate(self) -> None:
        """Validate settings at startup. Reports all problems at once.

        Raises SystemExit listing every configuration error found, so the
        user can fix them in one pass rather than one restart at a time.
        """
        errors: list[str] = []

        if not self.graph_prefix:
            errors.append(
                "GRAPH_PREFIX must not be empty. An empty prefix disables "
                "the managed-graph safety guard, allowing operations on any "
                "graph in the account."
            )

        if errors:
            raise SystemExit(
                "ERROR: invalid configuration:\n"
                + "\n".join(f"  - {e}" for e in errors)
            )


# Single validated Settings instance shared across the process. Built and
# validated once on first access, then reused, so the value the startup
# guard validates is the exact same instance every consumer (e.g. the
# managed-graph guard) reads — not an independent, unvalidated re-read.
_settings: "Settings | None" = None


def get_settings() -> "Settings":
    """Return the process-wide validated Settings instance.

    Loads from the environment and validates on first call, then caches.
    All runtime consumers should read configuration through this, rather
    than calling ``Settings.from_env()`` directly, so everyone shares the
    one instance that startup validation checked.
    """
    global _settings
    if _settings is None:
        settings = Settings.from_env()
        settings.validate()
        _settings = settings
    return _settings
