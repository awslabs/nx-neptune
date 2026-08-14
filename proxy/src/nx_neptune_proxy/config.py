# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from dataclasses import dataclass, field

# Always trusted, regardless of TRUSTED_HOSTS: requests claiming to be from
# here never leave the local machine.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})


@dataclass(frozen=True)
class Settings:
    """Application settings loaded from environment variables."""

    log_level: str = "INFO"
    allowed_origins: list[str] = None  # type: ignore[assignment]
    port: int = 8080
    extra_trusted_hosts: frozenset[str] = field(default_factory=frozenset)
    region: str = ""
    graph_prefix: str = "nxp-"

    def __post_init__(self) -> None:
        if self.allowed_origins is None:
            object.__setattr__(self, "allowed_origins", [])

    @property
    def trusted_hosts(self) -> frozenset[str]:
        """Hostnames this process accepts in a request's Host or Origin.

        Used by the origin_validation middleware to check the Origin header.
        Independent of allowed_origins/CORS — that list controls browser
        CORS response headers; this one controls whether the server accepts
        the request at all. TRUSTED_HOSTS (comma-separated) adds to the
        fixed loopback set; it's never removed or replaced.
        """
        return _LOOPBACK_HOSTS | self.extra_trusted_hosts

    @classmethod
    def from_env(cls) -> "Settings":
        origins_raw = os.environ.get("CORS_ALLOWED_ORIGINS", "")
        origins = (
            [o.strip() for o in origins_raw.split(",") if o.strip()]
            if origins_raw
            else []
        )
        trusted_hosts_raw = os.environ.get("TRUSTED_HOSTS", "")
        extra_trusted = frozenset(
            h.strip() for h in trusted_hosts_raw.split(",") if h.strip()
        )
        return cls(
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            allowed_origins=origins,
            port=int(os.environ.get("PORT", "8080")),
            extra_trusted_hosts=extra_trusted,
            region=os.environ.get(
                "AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "")
            ),
            graph_prefix=os.environ.get("GRAPH_PREFIX", "nxp-"),
        )

    def validate(self) -> None:
        """Validate settings at startup. Raises SystemExit on invalid config."""
        if not self.graph_prefix:
            raise SystemExit(
                "ERROR: GRAPH_PREFIX must not be empty. "
                "An empty prefix disables the managed-graph safety guard, "
                "allowing operations on any graph in the account."
            )
