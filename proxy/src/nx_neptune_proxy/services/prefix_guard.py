# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared prefix guard to prevent deletion of graphs not managed by this tool."""

from fastapi import HTTPException

from nx_neptune_proxy.config import Settings


def assert_managed_graph(graph_name: str | None) -> None:
    """Raise HTTP 403 if graph_name doesn't start with the configured prefix.

    Args:
        graph_name: The full (prefixed) graph name as it exists in Neptune.
                    If None or empty, the guard rejects the request.
    """
    prefix = Settings.from_env().graph_prefix
    if not graph_name or not graph_name.startswith(prefix):
        raise HTTPException(
            status_code=403,
            detail=f"Refusing to delete graph '{graph_name or ''}': not managed by this tool (expected prefix '{prefix}')",
        )
