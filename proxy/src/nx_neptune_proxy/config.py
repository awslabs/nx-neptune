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
    region: str = ""

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
            region=os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "")),
        )
