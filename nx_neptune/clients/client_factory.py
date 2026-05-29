# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
from typing import Any, Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config

from .neptune_constants import (
    APP_ID_NX,
    SERVICE_ATHENA,
    SERVICE_IAM,
    SERVICE_NA,
    SERVICE_S3,
    SERVICE_STS,
)

__all__ = ["ClientFactory"]


class ClientFactory:
    """Centralized factory for creating and caching boto3 clients.

    Ensures consistent configuration (user agent, timeouts, region) across
    all AWS service clients used by the library.

    When called with no arguments, returns the shared singleton default instance.
    When called with custom configuration, creates a new instance.

    Args:
        region: AWS region name. If None, uses boto3 default.
        timeout_seconds: Read timeout for long-running operations (e.g., queries).
            Applied to the Neptune Analytics client.
    """

    _default: Optional["ClientFactory"] = None

    def __new__(
        cls,
        region: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ):
        if region is None and timeout_seconds is None:
            if cls._default is None:
                cls._default = super().__new__(cls)
                cls._default._initialized = False
            return cls._default
        return super().__new__(cls)

    def __init__(
        self,
        region: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ):
        if getattr(self, "_initialized", False):
            return
        self._region = region
        self._timeout_seconds = timeout_seconds
        self._clients: dict[str, BaseClient] = {}
        self._initialized = True

    def _base_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        if self._region:
            kwargs["region_name"] = self._region
        return kwargs

    def _na_config(self) -> Config:
        config_kwargs: dict[str, Any] = {"user_agent_appid": APP_ID_NX}
        if self._timeout_seconds:
            config_kwargs["read_timeout"] = self._timeout_seconds
        return Config(**config_kwargs)

    def neptune(self) -> BaseClient:
        """Get or create the Neptune Analytics client."""
        if SERVICE_NA not in self._clients:
            self._clients[SERVICE_NA] = boto3.client(
                service_name=SERVICE_NA, config=self._na_config(), **self._base_kwargs()
            )
        return self._clients[SERVICE_NA]

    def s3(self) -> BaseClient:
        """Get or create the S3 client."""
        if SERVICE_S3 not in self._clients:
            self._clients[SERVICE_S3] = boto3.client(
                service_name=SERVICE_S3, **self._base_kwargs()
            )
        return self._clients[SERVICE_S3]

    def athena(self) -> BaseClient:
        """Get or create the Athena client."""
        if SERVICE_ATHENA not in self._clients:
            self._clients[SERVICE_ATHENA] = boto3.client(
                service_name=SERVICE_ATHENA, **self._base_kwargs()
            )
        return self._clients[SERVICE_ATHENA]

    def sts(self) -> BaseClient:
        """Get or create the STS client."""
        if SERVICE_STS not in self._clients:
            self._clients[SERVICE_STS] = boto3.client(
                service_name=SERVICE_STS, **self._base_kwargs()
            )
        return self._clients[SERVICE_STS]

    def iam(self) -> BaseClient:
        """Get or create the IAM client."""
        if SERVICE_IAM not in self._clients:
            self._clients[SERVICE_IAM] = boto3.client(
                service_name=SERVICE_IAM, **self._base_kwargs()
            )
        return self._clients[SERVICE_IAM]
