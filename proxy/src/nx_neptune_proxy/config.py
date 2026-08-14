# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Application settings loaded from environment variables."""

    log_level: str = "INFO"
    allowed_origins: list[str] = None  # type: ignore[assignment]
    port: int = 8080
    host: str = "127.0.0.1"
    region: str = ""
    graph_prefix: str = "nxp-"

    def __post_init__(self) -> None:
        if self.allowed_origins is None:
            object.__setattr__(self, "allowed_origins", [])

    @classmethod
    def from_env(cls) -> "Settings":
        origins_raw = os.environ.get("CORS_ALLOWED_ORIGINS", "")
        origins = [o.strip() for o in origins_raw.split(",") if o.strip()] if origins_raw else []
        return cls(
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            allowed_origins=origins,
            port=int(os.environ.get("PORT", "8080")),
            host=os.environ.get("HOST", "127.0.0.1"),
            region=os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "")),
            graph_prefix=os.environ.get("GRAPH_PREFIX", "nxp-"),
        )

    def validate_host(self) -> None:
        """Refuse to start on non-loopback address."""
        loopback = {"127.0.0.1", "::1", "localhost"}
        if self.host not in loopback:
            raise SystemExit(
                f"ERROR: Refusing to bind to '{self.host}'. "
                f"This proxy is designed for local development only. "
                f"Set HOST=127.0.0.1 or remove the HOST override."
            )
