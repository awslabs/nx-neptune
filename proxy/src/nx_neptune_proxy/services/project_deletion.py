# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import logging
import time

from botocore.exceptions import ClientError
from nx_neptune.clients.client_factory import ClientFactory

from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.projection_store import (
    STATUS_DELETING,
    STATUS_EXECUTING,
    STATUS_IMPORTING,
)

logger = logging.getLogger(__name__)

POLL_INTERVAL = 10
TIMEOUT = 600  # 10 min max wait per graph
_RUNNING = frozenset({STATUS_IMPORTING, STATUS_EXECUTING})


async def delete_project(project_id: str, force: bool = False) -> None:
    """Delete a project's projections (and their graphs), then the project.

    Each child is claimed atomically (``deleting``) before its graph is
    touched; a child running an import/execute is retained unless ``force``.
    Rows are removed only through the guarded ``purge``.
    """
    projections = projection_service.list_by_project(project_id)
    refuse = frozenset() if force else _RUNNING

    claimed = []
    retained_busy = False
    for p in projections:
        if projection_service.try_transition(p.id, STATUS_DELETING, refuse_when=refuse):
            claimed.append(p)
        elif projection_service.get(p.id) is not None:
            retained_busy = True

    with_graph = [p for p in claimed if p.graph_id]
    results = await asyncio.gather(
        *[_delete_graph(p.graph_id) for p in with_graph],  # type: ignore[arg-type]
        return_exceptions=True,
    )
    failed_ids = set()
    for p, result in zip(with_graph, results):
        # BaseException, not Exception: gather(return_exceptions=True) can hand
        # back a CancelledError, which must count as a failed deletion so the
        # graph reference is kept.
        if isinstance(result, BaseException):
            logger.warning(
                f"Failed to delete graph {p.graph_id} for projection {p.id}: {result}"
            )
            failed_ids.add(p.id)
        else:
            projection_service.update(p.id, graph_id=None, graph_endpoint=None)

    # The claim holds the row (`deleting`) and graph_id is clear, so forcing the
    # purge is safe; the graph_id condition still protects a failed deletion.
    for p in claimed:
        if p.id not in failed_ids and not projection_service.purge(p.id, force=True):
            retained_busy = True

    if failed_ids:
        logger.warning(
            f"Project {project_id} has {len(failed_ids)} projection(s) "
            "with failed graph deletion, keeping project in deleting state"
        )
        return
    if retained_busy:
        logger.warning(f"Project {project_id} kept: a projection could not be removed")
        project_store.update(project_id, status="active")
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
    # Raising lets the caller count this as a failed deletion: the graph may
    # still be live, so its reference must be kept for the resume to retry.
    raise TimeoutError(f"Timeout waiting for graph {graph_id} deletion")
