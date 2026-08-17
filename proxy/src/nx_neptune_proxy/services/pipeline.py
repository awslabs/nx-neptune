# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from nx_neptune_proxy.config import get_settings
from nx_neptune_proxy.services.projection_store import Projection, store
from nx_neptune_proxy.utils.sanitize import sanitize_error_message

logger = logging.getLogger(__name__)


def _assert_managed(graph_name: str | None) -> None:
    """Raise if graph_name isn't managed by this tool (doesn't carry the prefix).

    Mirrors aws_helper.assert_managed_graph, but raises a plain exception rather
    than HTTPException because the pipeline runs as a background task where an
    HTTP response cannot be returned.
    """
    prefix = Settings.from_env().graph_prefix
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
    s3_location = projection.s3_staging_bucket.rstrip("/") + f"/{projection.id}/"

    try:
        store.update(projection.id, status="importing")

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
            except Exception:
                # Recorded graph no longer exists (or is unreachable): treat as a
                # fresh run and create a new graph below rather than adopting one.
                logger.warning(
                    "Recorded graph_id %s not retrievable; creating a new graph",
                    projection.graph_id,
                )
                resp = None
            if resp is not None and resp.get("status") == "AVAILABLE":
                # Guard: confirm the recorded graph is one this tool manages before reset.
                _assert_managed(resp.get("name"))
                existing = sm.get_graph(projection.graph_id)

        if existing:
            # Reuse existing graph (retry scenario) — reset data
            graph = existing
            store.update(
                projection.id,
                graph_id=graph.graph_id,
                graph_endpoint=f"https://{graph.graph_id}.neptune-graph.amazonaws.com",
            )
            _update(
                projection.id,
                step="graph_reset",
                label="Resetting graph data",
                progress=25,
            )
            await sm.reset_graph(graph.name)
        else:
            # Brand new graph — no reset needed
            graph = await sm.get_or_create_graph(
                config={"provisionedMemory": projection.graph_memory_gb}
            )
            store.update(
                projection.id,
                graph_id=graph.graph_id,
                graph_endpoint=f"https://{graph.graph_id}.neptune-graph.amazonaws.com",
            )

        # Step 2: Athena query + CSV import
        _update(
            projection.id,
            step="athena_import",
            label="Running Athena query and importing data",
            progress=45,
        )
        sql_queries = [q for q in [projection.node_query, projection.edge_query] if q]
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
        store.update(projection.id, status="complete")

    except Exception as e:
        logger.exception("Pipeline failed")
        if hasattr(e, "response"):
            err = e.response.get("Error", {})
            error_msg = f"{err.get('Code', 'Error')}: {err.get('Message', str(e))}"
        else:
            error_msg = str(e)

        # Sanitize before persisting — strip ARNs, account IDs
        store.update(
            projection.id, status="failed", error=sanitize_error_message(error_msg)
        )


def _update(projection_id: str, step: str, label: str, progress: float) -> None:
    store.update(projection_id, step=step, step_label=label, progress=progress)
