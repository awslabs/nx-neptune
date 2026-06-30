# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from nx_neptune_proxy.services.projection_store import GRAPH_PREFIX, Projection, store

logger = logging.getLogger(__name__)


async def run_pipeline(projection: Projection) -> None:
    """Execute the full pipeline: create graph → Athena → import."""
    from nx_neptune.session_manager import SessionManager

    graph_name = f"{GRAPH_PREFIX}{projection.graph_name}"
    s3_location = projection.s3_staging_bucket.rstrip("/") + f"/{projection.id}/"

    try:
        store.update(projection.id, status="importing")

        # Step 1: Create graph
        _update(projection.id, step="graph_creation", label="Creating Neptune Analytics graph", progress=5)
        sm = SessionManager(session_name=graph_name)
        graph = await sm.get_or_create_graph(
            config={"provisionedMemory": projection.graph_memory_gb}
        )
        store.update(projection.id, graph_id=graph.graph_id,
                     graph_endpoint=f"https://{graph.graph_id}.neptune-graph.amazonaws.com")
        _update(projection.id, step="graph_creation", label="Graph available", progress=20)

        # Step 1b: Reset graph to ensure it's empty
        _update(projection.id, step="graph_reset", label="Resetting graph data", progress=25)
        await sm.reset_graph(graph.name)
        _update(projection.id, step="graph_reset", label="Graph ready for import", progress=40)

        # Step 2: Athena query + CSV import
        _update(projection.id, step="athena_import", label="Running Athena query and importing data", progress=45)
        sql_queries = [q for q in [projection.node_query, projection.edge_query] if q]
        await sm.import_from_table(
            graph=graph,
            s3_location=s3_location,
            sql_queries=sql_queries,
            catalog=projection.catalog,
            database=projection.database,
            remove_buckets=True,
        )
        _update(projection.id, step="athena_import", label="Import complete", progress=90)

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
        store.update(projection.id, status="failed", error=error_msg)


def _update(projection_id: str, step: str, label: str, progress: float) -> None:
    store.update(projection_id, step=step, step_label=label, progress=progress)
