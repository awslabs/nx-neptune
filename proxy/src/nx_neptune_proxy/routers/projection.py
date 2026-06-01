# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import re

from dataclasses import asdict
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.instance_management import execute_athena_query, get_athena_query_results
from nx_neptune.validators import check_athena_query, validate_resources
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
    }


@router.post("/{projection_id}/validate", summary="Validate all resources", response_model=ValidateResponse)
def validate_projection(projection_id: str):
    """Run all validators against the projection's configuration."""
    p = _get_projection_or_404(projection_id)
    checks = validate_resources(
        s3_staging_bucket=p.s3_staging_bucket,
        athena_catalog=p.catalog,
        athena_database=p.database,
        sql_query=p.sql_query,
    )
    return {"valid": all(c["passed"] for c in checks), "checks": checks}


@router.post("/{projection_id}/validate-query", summary="Validate query only", response_model=ValidateResponse)
def validate_query(projection_id: str):
    """Validate only the Athena query."""
    p = _get_projection_or_404(projection_id)
    result = check_athena_query(
        sql_query=p.sql_query,
        catalog=p.catalog,
        database=p.database,
        output_location=p.s3_staging_bucket,
    )
    return {"valid": result.passed, "checks": [result.to_dict()]}


@router.post("/{projection_id}/preview", summary="Preview first N rows", response_model=PreviewResponse)
def preview_projection(projection_id: str, limit: int = Query(10, ge=1, le=1000)):
    """Run the query with a LIMIT and return preview rows."""
    p = _get_projection_or_404(projection_id)
    client = ClientFactory().athena()

    queries = [q.strip() for q in p.sql_query.split(";") if q.strip()]
    all_results = []

    for q in queries:
        stripped = re.sub(r"\s+LIMIT\s+\d+\s*$", "", q.rstrip(), flags=re.IGNORECASE)
        limited = f"{stripped} LIMIT {limit}"

        exec_ids = execute_athena_query(
            sql_statement=limited,
            output_location=p.s3_staging_bucket,
            catalog=p.catalog,
            database=p.database,
            client=client,
        )
        exec_id = exec_ids[0] if isinstance(exec_ids, list) else exec_ids

        rows = get_athena_query_results(query_execution_id=exec_id, client=client)
        columns = rows[0] if rows else []
        data_rows = rows[1:] if len(rows) > 1 else []
        all_results.append({"columns": columns, "rows": data_rows})

    return {"error": None, "results": all_results}


@router.post("/{projection_id}/execute", summary="Start import pipeline", status_code=202)
def execute_projection(projection_id: str, background_tasks: BackgroundTasks):
    """Kick off the full import pipeline as a background task."""
    p = _get_projection_or_404(projection_id)
    if p.status == "executing":
        raise HTTPException(status_code=409, detail="Pipeline already running")
    background_tasks.add_task(asyncio.run, run_pipeline(p))
    return {"id": p.id, "status": "accepted"}
