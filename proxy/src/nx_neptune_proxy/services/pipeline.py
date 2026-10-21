# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import uuid

from nx_neptune_proxy.config import get_settings
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.projection_store import Projection, store
from nx_neptune_proxy.utils.sanitize import sanitize_error_message

logger = logging.getLogger(__name__)


def _build_staging_prefix(bucket: str | None, projection_id: str, run_id: str) -> str:
    """Build a unique per-run staging prefix: <bucket>/<projection-id>/<run-id>/.

    Every pipeline invocation writes to its own prefix so CSVs left behind by a
    failed run are never picked up by a retry (StartImportTask imports every
    object under the prefix, and duplicate edge files are not deduplicated).
    """
    if not bucket or not bucket.strip():
        raise ValueError("s3_staging_bucket is required to stage projection data")
    if not projection_id or not projection_id.strip("/"):
        raise ValueError("projection id is required to stage projection data")
    if not run_id or not run_id.strip("/"):
        raise ValueError("run id is required to stage projection data")
    return f"{bucket.rstrip('/')}/{projection_id.strip('/')}/{run_id.strip('/')}/"


def _has_non_empty_key(s3_location: str) -> bool:
    """Return True if s3_location addresses a key path inside a bucket."""
    if not s3_location or not s3_location.startswith("s3://"):
        return False
    bucket, sep, key = s3_location[len("s3://") :].partition("/")
    return bool(bucket) and bool(sep) and bool(key.strip("/"))


def _cleanup_staging_prefix(s3_location: str) -> None:
    """Best-effort removal of a single run's staging prefix.

    Never raises: cleanup runs in a ``finally`` and must not mask the pipeline's
    original outcome. The key-path guard is essential because
    ``empty_s3_bucket`` treats an empty path as the whole bucket.
    """
    from nx_neptune import instance_management

    if not _has_non_empty_key(s3_location):
        logger.warning(
            "Refusing to empty S3 location without a key path: %r", s3_location
        )
        return

    try:
        instance_management.empty_s3_bucket(s3_location)
    except Exception:
        logger.exception("Failed to clean up staging prefix %s", s3_location)


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
    s3_location = _build_staging_prefix(
        projection.s3_staging_bucket, projection.id, uuid.uuid4().hex
    )

    try:
        projection_service.update(projection.id, status="importing")

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
            projection_service.update(
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
            projection_service.update(
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
        projection_service.update(projection.id, status="complete")

    except Exception as e:
        logger.exception("Pipeline failed")
        if hasattr(e, "response"):
            err = e.response.get("Error", {})
            error_msg = f"{err.get('Code', 'Error')}: {err.get('Message', str(e))}"
        else:
            error_msg = str(e)

        # Sanitize before persisting — strip ARNs, account IDs
        projection_service.update(
            projection.id, status="failed", error=sanitize_error_message(error_msg)
        )
    finally:
        # Remove only this run's staging prefix. Leftover CSVs from a failed
        # run would otherwise be re-imported by the next run (duplicate edges).
        _cleanup_staging_prefix(s3_location)


def _update(projection_id: str, step: str, label: str, progress: float) -> None:
    projection_service.update(
        projection_id, step=step, step_label=label, progress=progress
    )
