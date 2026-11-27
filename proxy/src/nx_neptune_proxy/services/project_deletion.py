# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import asyncio
import logging
import time

from botocore.exceptions import ClientError
from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.instance_management import _delete_na_instance_task

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
            # Pinned to the graph that was actually deleted. An unconditional
            # clear would wipe a reference a taken-over run recorded for a
            # *different* graph (`pipeline._record_graph`), orphaning a live one.
            # A False here is not an error: the re-read below picks that graph up.
            # with_graph was built by filtering on graph_id, which mypy does not
            # narrow through the comprehension.
            projection_service.clear_graph_if(
                p.id, p.graph_id  # type: ignore[arg-type]
            )

    # Re-read before purging, and trust the row rather than the pre-claim
    # snapshot. A forced delete can claim a child mid-import, and that run's
    # `_record_graph` writes a graph_id while this job awaits a sibling's delete
    # — whether or not the snapshot had one. Every reference this job did delete
    # has just been cleared, so any graph_id still present names a graph nobody
    # has deleted: the purge would refuse the row on its graph_id condition, the
    # project would fall back to `active`, and that graph would keep billing with
    # nobody looking at it. So pick up whatever is there now and delete it too.
    late_graphs = []
    for p in claimed:
        if p.id in failed_ids:
            continue
        current = projection_service.get(p.id)
        if current is not None and current.graph_id:
            late_graphs.append(current)

    if late_graphs:
        logger.warning(
            f"Project {project_id}: {len(late_graphs)} projection(s) recorded a "
            "graph after being claimed for deletion; deleting those too"
        )
        late_results = await asyncio.gather(
            *[_delete_graph(p.graph_id) for p in late_graphs],  # type: ignore[arg-type]
            return_exceptions=True,
        )
        for p, result in zip(late_graphs, late_results):
            if isinstance(result, BaseException):
                logger.warning(
                    f"Failed to delete late-recorded graph {p.graph_id} for "
                    f"projection {p.id}: {result}"
                )
                failed_ids.add(p.id)
            else:
                # late_graphs only holds rows whose graph_id was set, which mypy
                # does not narrow through the loop.
                projection_service.clear_graph_if(
                    p.id, p.graph_id  # type: ignore[arg-type]
                )

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


def _issue_delete(client, graph_id: str) -> bool:
    """Ask for the delete. True once the graph is confirmed gone.

    Deletion protection is cleared first. Graphs are created with it on by
    default (``NETWORKX_DELETION_PROTECTION`` defaults to true), and a raw
    ``delete_graph`` fails with ConflictException on a protected graph, so
    ``_delete_na_instance_task`` is used for the same reason
    ``pipeline._delete_orphaned_graph`` uses it — the two teardown paths must
    not disagree about whether protection has to be cleared.

    False means "not gone yet": either the delete was accepted, or it was
    refused by a transient graph state and the caller should poll and re-ask.
    """
    try:
        _delete_na_instance_task(client, graph_id)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "ResourceNotFoundException":
            return True
        # ConflictException now means a transient state rather than protection:
        # either a delete is already under way, or the graph is still CREATING
        # and nothing can be deleted yet. Both resolve by waiting.
        if code != "ConflictException":
            raise
    return False


async def _delete_graph(graph_id: str) -> None:
    """Delete a Neptune graph and wait until gone."""
    client = ClientFactory().neptune()
    if _issue_delete(client, graph_id):
        return

    start = time.time()
    while time.time() - start < TIMEOUT:
        await asyncio.sleep(POLL_INTERVAL)
        try:
            client.get_graph(graphIdentifier=graph_id)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return
            raise
        # Still there. Re-ask rather than polling out the full timeout on a
        # graph nothing ever accepted a delete for: a CREATING graph refuses the
        # first attempt and would otherwise reach AVAILABLE and simply stay.
        if _issue_delete(client, graph_id):
            return
    # Raising lets the caller count this as a failed deletion: the graph may
    # still be live, so its reference must be kept for the resume to retry.
    raise TimeoutError(f"Timeout waiting for graph {graph_id} deletion")
