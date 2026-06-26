# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import logging
import time

from botocore.exceptions import ClientError
from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune_proxy.services.projection_store import store as projection_store
from nx_neptune_proxy.services.workspace_store import store as workspace_store

logger = logging.getLogger(__name__)

POLL_INTERVAL = 10
TIMEOUT = 600  # 10 min max wait per graph


async def delete_workspace(workspace_id: str) -> None:
    """Delete all graphs for a workspace's projections, then remove records."""
    projections = [p for p in projection_store.list() if p.workspace_id == workspace_id]

    for p in projections:
        if p.graph_id:
            try:
                await _delete_graph(p.graph_id)
            except Exception as e:
                logger.warning(f"Failed to delete graph {p.graph_id} for projection {p.id}: {e}")
        projection_store.delete(p.id)

    workspace_store.delete(workspace_id)
    logger.info(f"Workspace {workspace_id} fully deleted")


async def _delete_graph(graph_id: str) -> None:
    """Delete a Neptune graph and wait until gone."""
    client = ClientFactory().neptune()
    try:
        client.delete_graph(graphIdentifier=graph_id, skipSnapshot=True)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return
        raise

    start = time.time()
    while time.time() - start < TIMEOUT:
        await asyncio.sleep(POLL_INTERVAL)
        try:
            client.get_graph(graphIdentifier=graph_id)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return
            raise
    logger.warning(f"Timeout waiting for graph {graph_id} deletion")
