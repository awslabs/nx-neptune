# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from nx_neptune_proxy.services.projection_store import GRAPH_PREFIX, Projection

logger = logging.getLogger(__name__)


async def run_pipeline(projection: Projection) -> None:
    """Execute the full pipeline: create graph → Athena → import."""
    from nx_neptune.session_manager import SessionManager

    graph_name = f"{GRAPH_PREFIX}{projection.graph_name}"
    s3_location = projection.s3_staging_bucket.rstrip("/") + f"/{projection.id}/"

    try:
        projection.status = "executing"

        # Step 1: Create graph
        _update(projection, step="graph_creation", label="Creating Neptune Analytics graph", progress=5)
        sm = SessionManager(session_name=graph_name)
        graph = await sm.get_or_create_graph(
            config={"provisionedMemory": projection.graph_memory_gb}
        )
        projection.graph_id = graph.graph_id
        projection.graph_endpoint = f"https://{graph.graph_id}.neptune-graph.amazonaws.com"
        _update(projection, step="graph_creation", label="Graph available", progress=40)

        # Step 2: Athena query + CSV import
        _update(projection, step="athena_import", label="Running Athena query and importing data", progress=45)
        sql_queries = [q.strip() for q in projection.sql_query.split(";") if q.strip()]
        await sm.import_from_table(
            graph=graph,
            s3_location=s3_location,
            sql_queries=sql_queries,
            catalog=projection.catalog,
            database=projection.database,
            remove_buckets=True,
        )
        _update(projection, step="athena_import", label="Import complete", progress=90)

        # Step 3: Done
        _update(projection, step="ready", label="Graph ready", progress=100)
        projection.status = "complete"

    except Exception as e:
        logger.exception("Pipeline failed")
        projection.status = "failed"
        projection.error = str(e)


def _update(projection: Projection, step: str, label: str, progress: float) -> None:
    projection.step = step
    projection.step_label = label
    projection.progress = progress
