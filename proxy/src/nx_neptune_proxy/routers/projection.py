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
from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.utils.sanitize import sanitize_error_message
from nx_neptune_proxy.routers.schemas import (
    PreviewResponse,
    ProjectionCreate,
    ProjectionResponse,
    ProjectionStatus,
    ProjectionUpdate,
    QueriesPayload,
    QueriesResponse,
    ValidateResponse,
)
from nx_neptune_proxy.services.pipeline import run_pipeline
from nx_neptune_proxy.services.projection_store import store
from nx_neptune_proxy.services.query_store import query_store
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

    # Try new multi-query tables first, fall back to legacy single fields
    node_queries_list = query_store.list_node_queries(projection_id)
    edge_queries_list = query_store.list_edge_queries(projection_id)

    queries_to_validate = []
    if node_queries_list:
        for i, nq in enumerate(node_queries_list):
            if nq.sql.strip():
                queries_to_validate.append((f"node_query_{i+1}", nq.sql, "node"))
    elif p.node_query:
        queries_to_validate.append(("node_query", p.node_query, "node"))

    if edge_queries_list:
        for i, eq in enumerate(edge_queries_list):
            if eq.sql.strip():
                queries_to_validate.append((f"edge_query_{i+1}", eq.sql, "edge"))
    elif p.edge_query:
        queries_to_validate.append(("edge_query", p.edge_query, "edge"))

    for label, query, query_type in queries_to_validate:
        result = check_athena_query(
            sql_query=query,
            catalog=p.catalog,
            database=p.database,
            output_location=p.s3_staging_bucket,
            query_type=query_type,
        )
        checks.append({"check": label, "passed": result.passed, "message": result.message})
    valid = all(c["passed"] for c in checks) if checks else False
    return {"valid": valid, "checks": checks}


@router.post("/{projection_id}/preview", summary="Preview first N rows", response_model=PreviewResponse)
async def preview_projection(projection_id: str, limit: int = Query(10, ge=1, le=1000)):
    """Run the query with a LIMIT and return preview rows."""
    p = _get_projection_or_404(projection_id)
    client = ClientFactory().athena()

    # Try new multi-query tables first, fall back to legacy single fields
    node_queries_list = query_store.list_node_queries(projection_id)
    edge_queries_list = query_store.list_edge_queries(projection_id)

    queries = []
    if node_queries_list or edge_queries_list:
        queries = [nq.sql for nq in node_queries_list if nq.sql.strip()]
        queries += [eq.sql for eq in edge_queries_list if eq.sql.strip()]
    else:
        queries = [q for q in [p.node_query, p.edge_query] if q]
        if not queries and p.sql_query:
            queries = [q.strip() for q in p.sql_query.split(";") if q.strip()]
    all_results = []

    for q in queries:
        limited = wrap_with_limit(q, limit)

        exec_id = _execute_athena_query(client, limited, p.s3_staging_bucket, catalog=p.catalog, database=p.database)

        await wait_until_all_complete([exec_id], TaskType.EXPORT_ATHENA_TABLE, client, polling_interval=5)

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
    background_tasks.add_task(run_pipeline, p)
    return {"id": p.id, "status": "accepted"}


@router.delete("/{projection_id}", summary="Delete projection record", status_code=200)
def delete_projection(projection_id: str):
    """Permanently remove the projection record from the database."""
    p = _get_projection_or_404(projection_id)
    if p.status == "deleting":
        raise HTTPException(status_code=409, detail="Graph deletion in progress, cannot purge yet")
    store.delete(projection_id)
    return {"id": p.id, "status": "deleted"}


@router.post("/{projection_id}/delete-graph", summary="Delete associated graph and archive projection", status_code=202)
def delete_projection_graph(projection_id: str, background_tasks: BackgroundTasks):
    """Delete the Neptune graph in background, then mark projection as archived."""
    p = _get_projection_or_404(projection_id)
    if not p.graph_id:
        raise HTTPException(status_code=409, detail="No graph associated with this projection")
    if p.status == "deleting":
        raise HTTPException(status_code=409, detail="Already deleting")
    store.update(projection_id, status="deleting", step="graph_delete", step_label="Deleting graph")

    async def _delete_graph():
        client = ClientFactory().neptune()
        try:
            client.delete_graph(graphIdentifier=p.graph_id, skipSnapshot=True)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                store.update(projection_id, status="failed", error=sanitize_error_message(str(e)))
                return
        # Poll until gone
        for _ in range(60):
            await asyncio.sleep(10)
            try:
                client.get_graph(graphIdentifier=p.graph_id)
            except ClientError:
                break
        else:
            store.update(projection_id, status="failed", error="Timeout waiting for graph deletion")
            return
        store.update(projection_id, status="archived", graph_id=None, graph_endpoint=None,
                     step=None, step_label=None, progress=0)

    background_tasks.add_task(_delete_graph)
    return {"id": p.id, "status": "deleting"}


@router.get(
    "/{projection_id}/queries",
    summary="Get node and edge queries for a projection",
    response_model=QueriesResponse,
)
def get_queries(projection_id: str):
    """Return all node and edge queries for a projection."""
    _get_projection_or_404(projection_id)
    node_queries = query_store.list_node_queries(projection_id)
    edge_queries = query_store.list_edge_queries(projection_id)
    return QueriesResponse(
        node_queries=[{"id": q.id, "sql": q.sql, "position": q.position} for q in node_queries],
        edge_queries=[
            {"id": q.id, "sql": q.sql, "from_type": q.from_type, "to_type": q.to_type, "position": q.position}
            for q in edge_queries
        ],
    )


@router.put(
    "/{projection_id}/queries",
    summary="Save node and edge queries for a projection",
    response_model=QueriesResponse,
)
def save_queries(projection_id: str, body: QueriesPayload):
    """Replace all node and edge queries for a projection."""
    _get_projection_or_404(projection_id)
    node_queries = query_store.save_node_queries(projection_id, [q.model_dump() for q in body.node_queries])
    edge_queries = query_store.save_edge_queries(projection_id, [q.model_dump() for q in body.edge_queries])
    return QueriesResponse(
        node_queries=[{"id": q.id, "sql": q.sql, "position": q.position} for q in node_queries],
        edge_queries=[
            {"id": q.id, "sql": q.sql, "from_type": q.from_type, "to_type": q.to_type, "position": q.position}
            for q in edge_queries
        ],
    )
