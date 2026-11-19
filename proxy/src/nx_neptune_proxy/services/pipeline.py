# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from functools import partial

from botocore.exceptions import ClientError

from nx_neptune_proxy.config import get_settings
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.projection_store import (
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_IMPORTING,
    Projection,
    store,
)
from nx_neptune_proxy.utils.sanitize import sanitize_error_message

logger = logging.getLogger(__name__)


def _assert_managed(graph_name: str | None) -> None:
    """Raise if graph_name isn't managed by this tool (doesn't carry the prefix).

    Mirrors aws_helper.assert_managed_graph, but raises a plain exception rather
    than HTTPException because the pipeline runs as a background task where an
    HTTP response cannot be returned.
    """
    prefix = get_settings().graph_prefix
    if not graph_name or not graph_name.startswith(prefix):
        raise RuntimeError(
            f"Refusing to reset graph '{graph_name or ''}': not managed by this tool "
            f"(expected prefix '{prefix}')"
        )


async def run_pipeline(projection: Projection) -> None:
    """Execute the full pipeline: create graph → Athena → import."""
    from nx_neptune.clients.client_factory import ClientFactory
    from nx_neptune.session_manager import SessionManager

    graph_name = f"{get_settings().graph_prefix}{projection.graph_name}"
    s3_location = projection.s3_staging_bucket.rstrip("/") + f"/{projection.id}/"  # type: ignore[union-attr]  # noqa: E501

    try:
        # No status write here: execute_projection already claimed the row as
        # `importing` before scheduling this task. Re-asserting it would undo a
        # takeover that happened in between and let two owners run at once.

        # Step 1: Create or reuse graph
        _update(
            projection.id,
            step="graph_creation",
            label="Creating Neptune Analytics graph",
            progress=5,
        )
        sm = SessionManager(session_name=graph_name)

        # Only reuse a graph we previously recorded for THIS projection, matched by
        # exact graph_id — never adopt an arbitrary graph by name prefix (a short or
        # empty graph_name would otherwise collide with, and reset, an unrelated graph).
        existing = None
        if projection.graph_id:
            neptune = ClientFactory().neptune()
            try:
                resp = neptune.get_graph(graphIdentifier=projection.graph_id)
            except ClientError as e:
                # Only a missing graph means a fresh run. Any other error (throttling,
                # access denied) is raised, so the recorded graph is not orphaned.
                if e.response["Error"]["Code"] != "ResourceNotFoundException":
                    raise
                logger.warning(
                    "Recorded graph_id %s not retrievable; creating a new graph",
                    projection.graph_id,
                )
                resp = None
            if resp is not None and resp.get("status") == "AVAILABLE":
                # Guard: confirm the recorded graph is one this tool manages before reset.
                _assert_managed(resp.get("name"))
                existing = sm.get_graph(projection.graph_id)
            elif resp is not None and resp.get("status") != "DELETING":
                raise RuntimeError(
                    f"Graph {projection.graph_id} is {resp.get('status')}. "
                    "Retry when it is available, or delete it."
                )

        if existing:
            # Reuse existing graph (retry scenario) — reset data
            graph = existing
            _record_graph(projection.id, graph.graph_id)
            _update(
                projection.id,
                step="graph_reset",
                label="Resetting graph data",
                progress=25,
            )
            await sm.reset_graph(graph.name)
        else:
            # Brand new graph — no reset needed
            graph = await sm.create_graph(
                config={"provisionedMemory": projection.graph_memory_gb},
                on_created=partial(_record_graph, projection.id),
            )

        # Step 2: Athena query + CSV import
        _update(
            projection.id,
            step="athena_import",
            label="Running Athena query and importing data",
            progress=45,
        )
        sql_queries = projection_service.list_query_sql(projection.id)
        await sm.import_from_table(
            graph=graph,
            s3_location=s3_location,
            sql_queries=sql_queries,
            catalog=projection.catalog,
            database=projection.database,
            remove_buckets=True,
        )
        _update(
            projection.id, step="athena_import", label="Import complete", progress=90
        )

        # Step 3: Done
        _update(projection.id, step="ready", label="Graph ready", progress=100)
        # Conditional: if the row was taken over mid-run, `complete` would
        # overwrite the new owner's status and hide its operation.
        projection_service.update_if_status(
            projection.id, frozenset({STATUS_IMPORTING}), status=STATUS_COMPLETE
        )

    except Exception as e:
        logger.exception("Pipeline failed")
        if hasattr(e, "response"):
            err = e.response.get("Error", {})
            error_msg = f"{err.get('Code', 'Error')}: {err.get('Message', str(e))}"
        else:
            error_msg = str(e)

        # Sanitize before persisting — strip ARNs, account IDs.
        # Conditional for the same reason as the success path, and because the
        # row may already be gone (purged mid-run), in which case there is
        # nothing to record the failure on.
        projection_service.update_if_status(
            projection.id,
            frozenset({STATUS_IMPORTING}),
            status=STATUS_FAILED,
            error=sanitize_error_message(error_msg),
        )


def _record_graph(projection_id: str, graph_id: str) -> None:
    """Save the graph id and endpoint, or delete the graph if the row is gone.

    Conditional on this run still owning the row. A forced purge or a takeover
    can remove or re-claim it between the create call and this write; an
    unconditional ``update`` would then write nothing at all and leave the graph
    live, untracked and billing, with no reference left anywhere to delete it
    by. If the row is no longer ours the graph is deleted here instead.
    """
    from nx_neptune.clients.client_factory import ClientFactory

    recorded = projection_service.update_if_status(
        projection_id,
        frozenset({STATUS_IMPORTING}),
        graph_id=graph_id,
        graph_endpoint=f"https://{graph_id}.neptune-graph.amazonaws.com",
    )
    if recorded:
        return

    if projection_service.get(projection_id) is not None:
        # The row still exists but was re-claimed by another operation. Record
        # the reference anyway so whatever owns it now has something to delete;
        # that owner, not this dead run, decides the graph's fate.
        projection_service.update(
            projection_id,
            graph_id=graph_id,
            graph_endpoint=f"https://{graph_id}.neptune-graph.amazonaws.com",
        )
        raise RuntimeError(
            f"Projection {projection_id} was taken over mid-import; the graph "
            f"created for it ({graph_id}) is recorded for the new owner"
        )

    # The row is gone, so nothing can ever reference this graph again. Deleting
    # it here is the only thing standing between a forced purge and a graph that
    # bills forever with no record of its existence.
    logger.warning(
        "Projection %s was purged mid-import; deleting graph %s rather than "
        "leaving it untracked and billed",
        projection_id,
        graph_id,
    )
    try:
        ClientFactory().neptune().delete_graph(
            graphIdentifier=graph_id, skipSnapshot=True
        )
    except ClientError:
        logger.exception(
            "Could not delete untracked graph %s; it is still billing and must "
            "be removed manually",
            graph_id,
        )
    raise RuntimeError(
        f"Projection {projection_id} was purged mid-import; the graph created "
        f"for it ({graph_id}) has been deleted"
    )


def _update(projection_id: str, step: str, label: str, progress: float) -> None:
    projection_service.update(
        projection_id, step=step, step_label=label, progress=progress
    )
