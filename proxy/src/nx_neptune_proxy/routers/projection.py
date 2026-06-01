# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import re

from dataclasses import asdict
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from nx_neptune.clients.client_factory import ClientFactory
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
        athena_database=p.database
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

        rows, columns = _run_athena_preview(client, limited, p.catalog, p.database, p.s3_staging_bucket)
        if columns is None:
            return {"error": rows, "results": all_results}
        all_results.append({"columns": columns, "rows": rows})

    return {"error": None, "results": all_results}


def _run_athena_preview(client, query: str, catalog: str, database: str, output_location: str):
    """Run a query and return (rows, columns) or (error_string, None)."""
    import time

    exec_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Catalog": catalog, "Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )["QueryExecutionId"]

    while True:
        resp = client.get_query_execution(QueryExecutionId=exec_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            return resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error"), None
        time.sleep(1)

    results = client.get_query_results(QueryExecutionId=exec_id)
    columns = [c["Name"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [[d.get("VarCharValue", "") for d in row["Data"]] for row in results["ResultSet"]["Rows"][1:]]
    return rows, columns


@router.post("/{projection_id}/execute", summary="Start import pipeline", status_code=202)
def execute_projection(projection_id: str, background_tasks: BackgroundTasks):
    """Kick off the full import pipeline as a background task."""
    p = _get_projection_or_404(projection_id)
    if p.status == "executing":
        raise HTTPException(status_code=409, detail="Pipeline already running")
    background_tasks.add_task(asyncio.run, run_pipeline(p))
    return {"id": p.id, "status": "accepted"}
