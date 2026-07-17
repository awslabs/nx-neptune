# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import logging
import time

from botocore.exceptions import ClientError
from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune_proxy.services.projection_store import store as projection_store
from nx_neptune_proxy.services.project_store import store as project_store

logger = logging.getLogger(__name__)

POLL_INTERVAL = 10
TIMEOUT = 600  # 10 min max wait per graph


async def delete_project(project_id: str) -> None:
    """Delete all graphs for a project's projections, then remove records."""
    projections = [p for p in projection_store.list() if p.project_id == project_id]

    # Delete all graphs in parallel
    graphs_to_delete = [p for p in projections if p.graph_id]
    results = await asyncio.gather(
        *[_delete_graph(p.graph_id) for p in graphs_to_delete],
        return_exceptions=True,
    )

    # Only delete projection records where graph deletion succeeded
    failed_ids = set()
    for p, result in zip(graphs_to_delete, results):
        if isinstance(result, Exception):
            logger.warning(f"Failed to delete graph {p.graph_id} for projection {p.id}: {result}")
            failed_ids.add(p.id)

    for p in projections:
        if p.id not in failed_ids:
            projection_store.delete(p.id)

    # Only delete the project if all graphs were cleaned up
    if failed_ids:
        logger.warning(f"Project {project_id} has {len(failed_ids)} projection(s) with failed graph deletion, keeping project in deleting state")
        return

    project_store.delete(project_id)
    logger.info(f"Project {project_id} fully deleted")


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
