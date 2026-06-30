# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import time

from dataclasses import asdict
from botocore.exceptions import ClientError
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.clients.response_utils import get_query_failure_reason, get_query_state
from nx_neptune.instance_management import _execute_athena_query, get_athena_query_results
from nx_neptune.utils.task_future import TaskType, wait_until_all_complete
from nx_neptune.validators import check_athena_query, validate_resources, wrap_with_limit
from nx_neptune_proxy.routers.schemas import (
    PreviewResponse,
    ProjectionCreate,
    ProjectionResponse,
    ProjectionStatus,
    ProjectionUpdate,
    ValidateResponse,
)
from nx_neptune_proxy.services.pipeline import run_pipeline
from nx_neptune_proxy.services.projection_store import store
from nx_neptune_proxy.utils import unpack_query_results

router = APIRouter(prefix="/api/v0/projection", tags=["projection"])


def _get_projection_or_404(projection_id: str):
    projection = store.get(projection_id)
    if projection is None:
        raise HTTPException(status_code=404, detail="Projection not found")
    return projection


@router.post("", summary="Create a new projection", response_model=ProjectionResponse, status_code=201)
def create_projection(body: ProjectionCreate):
    """Create a new projection in draft state."""
    projection = store.create(**body.model_dump())
    return asdict(projection)


@router.get("", summary="List all projections", response_model=list[ProjectionResponse])
def list_projections():
    """List all projections."""
    return [asdict(p) for p in store.list()]


@router.get("/{projection_id}", summary="Get projection state", response_model=ProjectionResponse)
def get_projection(projection_id: str):
    """Get full projection state including progress."""
    return asdict(_get_projection_or_404(projection_id))


@router.put("/{projection_id}", summary="Update projection", response_model=ProjectionResponse)
def update_projection(projection_id: str, body: ProjectionUpdate):
    _get_projection_or_404(projection_id)
    projection = store.update(projection_id, **body.model_dump(exclude_unset=True))
    return asdict(projection)


@router.get("/{projection_id}/status", summary="Get pipeline progress", response_model=ProjectionStatus)
def get_projection_status(projection_id: str):
    """Get pipeline progress (subset of full state)."""
    p = _get_projection_or_404(projection_id)
    return {
        "id": p.id,
        "status": p.status,
        "step": p.step,
        "step_label": p.step_label,
        "progress": p.progress,
        "error": p.error,
        "graph_endpoint": p.graph_endpoint,
    }


@router.post("/{projection_id}/validate", summary="Validate all resources", response_model=ValidateResponse)
def validate_projection(projection_id: str):
    """Run all validators against the projection's configuration."""
    p = _get_projection_or_404(projection_id)
    checks = validate_resources(
        s3_staging_bucket=p.s3_staging_bucket,
        athena_catalog=p.catalog,
        athena_database=p.database
    )
    return {"valid": all(c["passed"] for c in checks), "checks": checks}


@router.post("/{projection_id}/validate-query", summary="Validate query only", response_model=ValidateResponse)
def validate_query(projection_id: str):
    """Validate node and edge queries individually"""
    p = _get_projection_or_404(projection_id)
    checks = []
    for label, query in [("node_query", p.node_query), ("edge_query", p.edge_query)]:
        if not query:
            continue
        result = check_athena_query(
            sql_query=query,
            catalog=p.catalog,
            database=p.database,
            output_location=p.s3_staging_bucket,
        )
        checks.append({"check": label, "passed": result.passed, "message": result.message})
    valid = all(c["passed"] for c in checks) if checks else False
    return {"valid": valid, "checks": checks}


@router.post("/{projection_id}/preview", summary="Preview first N rows", response_model=PreviewResponse)
def preview_projection(projection_id: str, limit: int = Query(10, ge=1, le=1000)):
    """Run the query with a LIMIT and return preview rows."""
    p = _get_projection_or_404(projection_id)
    client = ClientFactory().athena()

    queries = [q for q in [p.node_query, p.edge_query] if q]
    if not queries and p.sql_query:
        queries = [q.strip() for q in p.sql_query.split(";") if q.strip()]
    all_results = []

    for q in queries:
        limited = wrap_with_limit(q, limit)

        exec_id = _execute_athena_query(client, limited, p.s3_staging_bucket, catalog=p.catalog, database=p.database)

        asyncio.run(wait_until_all_complete([exec_id], TaskType.EXPORT_ATHENA_TABLE, client, polling_interval=5))

        resp = client.get_query_execution(QueryExecutionId=exec_id)
        state = get_query_state(resp)
        if state != "SUCCEEDED":
            return {"error": get_query_failure_reason(resp), "results": all_results}

        rows = get_athena_query_results(query_execution_id=exec_id, client=client)
        all_results.append(unpack_query_results(rows))

    return {"error": None, "results": all_results}


@router.post("/{projection_id}/execute", summary="Start import pipeline", status_code=202)
def execute_projection(projection_id: str, background_tasks: BackgroundTasks):
    """Kick off the full import pipeline as a background task."""
    p = _get_projection_or_404(projection_id)
    if p.status == "executing":
        raise HTTPException(status_code=409, detail="Pipeline already running")
    background_tasks.add_task(asyncio.run, run_pipeline(p))
    return {"id": p.id, "status": "accepted"}


@router.delete("/{projection_id}", summary="Delete projection and its graph", status_code=202)
def delete_projection(projection_id: str, background_tasks: BackgroundTasks):
    """Set status to deleting, delete graph in background, then remove record."""
    p = _get_projection_or_404(projection_id)
    if p.status == "deleting":
        raise HTTPException(status_code=409, detail="Already deleting")
    store.update(projection_id, status="deleting", step="graph_delete", step_label="Deleting graph")

    async def _delete():
        if p.graph_id:
            client = ClientFactory().neptune()
            try:
                client.delete_graph(graphIdentifier=p.graph_id, skipSnapshot=True)
            except ClientError as e:
                if e.response["Error"]["Code"] != "ResourceNotFoundException":
                    store.update(projection_id, status="failed", error=str(e))
                    return
            # Poll until gone
            for _ in range(60):
                await asyncio.sleep(10)
                try:
                    client.get_graph(graphIdentifier=p.graph_id)
                except ClientError:
                    break
        store.delete(projection_id)

    background_tasks.add_task(asyncio.run, _delete())
    return {"id": p.id, "status": "deleting"}
