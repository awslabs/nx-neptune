# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
import time
from dataclasses import asdict

from botocore.exceptions import ClientError
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.clients.na_client import NeptuneAnalyticsClient
from nx_neptune.validators import (
    check_athena_query,
    validate_resources,
)

from nx_neptune_proxy.routers.schemas import (
    ExplainQueryResponse,
    ExplainQueryResult,
    PreviewResponse,
    ProjectionCreate,
    ProjectionResponse,
    ProjectionStatus,
    ProjectionUpdate,
    GraphQueriesPayload,
    GraphQueriesResponse,
    QueriesPayload,
    QueriesResponse,
    RunQueryPayload,
    RunQueryResponse,
    ValidateResponse,
)
from nx_neptune_proxy.services.athena_query import AthenaQueryError, execute_query_rows
from nx_neptune_proxy.services.pipeline import run_pipeline
from nx_neptune_proxy.services.projection_service import (
    ProjectionNotFound,
    projection_service,
)
from nx_neptune_proxy.utils.aws_helper import (
    assert_managed_graph,
    get_graph_or_exception,
)
from nx_neptune_proxy.utils.sanitize import sanitize_error_message

router = APIRouter(prefix="/api/v0/projection", tags=["projection"])


def _get_projection_or_404(projection_id: str):
    try:
        return projection_service.get_or_raise(projection_id)
    except ProjectionNotFound:
        raise HTTPException(status_code=404, detail="Projection not found")


@router.post(
    "",
    summary="Create a new projection",
    response_model=ProjectionResponse,
    status_code=201,
)
def create_projection(body: ProjectionCreate):
    """Create a new projection in draft state."""
    projection = projection_service.create(**body.model_dump())
    return asdict(projection)


@router.get("", summary="List all projections", response_model=list[ProjectionResponse])
def list_projections():
    """List all projections."""
    return [asdict(p) for p in projection_service.list()]


@router.get(
    "/{projection_id}",
    summary="Get projection state",
    response_model=ProjectionResponse,
)
def get_projection(projection_id: str):
    """Get full projection state including progress."""
    return asdict(_get_projection_or_404(projection_id))


@router.put(
    "/{projection_id}", summary="Update projection", response_model=ProjectionResponse
)
def update_projection(projection_id: str, body: ProjectionUpdate):
    _get_projection_or_404(projection_id)
    projection = projection_service.update(
        projection_id, **body.model_dump(exclude_unset=True)
    )
    return asdict(projection)  # type: ignore[arg-type]


@router.get(
    "/{projection_id}/status",
    summary="Get pipeline progress",
    response_model=ProjectionStatus,
)
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


@router.post(
    "/{projection_id}/validate",
    summary="Validate all resources",
    response_model=ValidateResponse,
)
def validate_projection(projection_id: str):
    """Run all validators against the projection's configuration."""
    p = _get_projection_or_404(projection_id)
    checks = validate_resources(
        s3_staging_bucket=p.s3_staging_bucket,
        athena_catalog=p.catalog,
        athena_database=p.database,
    )
    return {"valid": all(c["passed"] for c in checks), "checks": checks}


@router.post(
    "/{projection_id}/validate-query",
    summary="Validate query only",
    response_model=ValidateResponse,
)
def validate_query(projection_id: str):
    """Validate node and edge queries individually"""
    p = _get_projection_or_404(projection_id)
    checks = []

    queries_to_validate = projection_service.list_labeled_queries(projection_id)

    for label, query, query_type in queries_to_validate:
        result = check_athena_query(
            sql_query=query,
            catalog=p.catalog,
            database=p.database,
            output_location=p.s3_staging_bucket,
            query_type=query_type,
        )
        checks.append(
            {"check": label, "passed": result.passed, "message": result.message}
        )
    valid = all(c["passed"] for c in checks) if checks else False
    return {"valid": valid, "checks": checks}


@router.post(
    "/{projection_id}/preview",
    summary="Preview first N rows",
    response_model=PreviewResponse,
)
async def preview_projection(projection_id: str, limit: int = Query(10, ge=1, le=1000)):
    """Run the query with a LIMIT and return preview rows."""
    p = _get_projection_or_404(projection_id)
    client = ClientFactory().athena()

    queries = projection_service.list_query_sql(projection_id)

    all_results: list = []

    for q in queries:
        try:
            all_results.append(
                await execute_query_rows(
                    client,
                    q,
                    p.s3_staging_bucket,
                    catalog=p.catalog,
                    database=p.database,
                    limit=limit,
                )
            )
        except AthenaQueryError as e:
            return {"error": e.reason, "results": all_results}

    return {"error": None, "results": all_results}


@router.post(
    "/{projection_id}/execute", summary="Start import pipeline", status_code=202
)
def execute_projection(projection_id: str, background_tasks: BackgroundTasks):
    """Kick off the full import pipeline as a background task."""
    p = _get_projection_or_404(projection_id)
    if p.status == "executing":
        raise HTTPException(status_code=409, detail="Pipeline already running")
    background_tasks.add_task(run_pipeline, p)
    return {"id": p.id, "status": "accepted"}


@router.post(
    "/{projection_id}/run-query",
    summary="Run openCypher queries against the projection's graph",
    response_model=RunQueryResponse,
)
def run_query(projection_id: str, body: RunQueryPayload):
    """Execute the supplied openCypher queries in sequence against the
    projection's Neptune Analytics graph and return each query's results.

    Stops at the first failing query, returning any results gathered so far
    alongside the error message.
    """
    p = _get_projection_or_404(projection_id)
    if not p.graph_id:
        raise HTTPException(
            status_code=409,
            detail="No graph associated with this projection — run the import first.",
        )

    na_client = NeptuneAnalyticsClient(graph_id=p.graph_id)
    results: list = []
    for cypher in body.queries:
        if not cypher.strip():
            continue
        try:
            results.append(na_client.execute_query(cypher))
        except ClientError as e:
            return RunQueryResponse(
                error=sanitize_error_message(str(e)), results=results
            )
    return RunQueryResponse(error=None, results=results)


@router.post(
    "/{projection_id}/explain-query",
    summary="Validate openCypher query syntax via Neptune Analytics EXPLAIN",
    response_model=ExplainQueryResponse,
)
def explain_query(projection_id: str, body: RunQueryPayload):
    """Validate each openCypher query against the projection's graph using
    Neptune Analytics ``EXPLAIN`` mode.

    ``EXPLAIN`` runs on the real engine but is read-only and cheap: it plans the
    query without executing it, so it authoritatively catches syntax errors,
    unknown procedures, and bad algorithm parameters. Unlike ``run-query``, every
    query is checked independently — validation does not stop at the first
    invalid query — so the caller gets a per-query verdict.
    """
    p = _get_projection_or_404(projection_id)
    if not p.graph_id:
        raise HTTPException(
            status_code=409,
            detail="No graph associated with this projection — run the import first.",
        )

    na_client = NeptuneAnalyticsClient(graph_id=p.graph_id)
    results: list[ExplainQueryResult] = []
    for cypher in body.queries:
        if not cypher.strip():
            continue
        # Prefix EXPLAIN unless the user already did. A ClientError is the
        # engine rejecting the query (bad syntax/procedure/param) — the signal
        # we want; any success means the query planned cleanly.
        stmt = cypher.strip()
        if not stmt.upper().startswith("EXPLAIN"):
            stmt = f"EXPLAIN {stmt}"
        try:
            na_client.execute_query(stmt)
            results.append(ExplainQueryResult(valid=True))
        except ClientError as e:
            results.append(
                ExplainQueryResult(valid=False, error=sanitize_error_message(str(e)))
            )
    return ExplainQueryResponse(results=results)


@router.delete("/{projection_id}", summary="Delete projection record", status_code=200)
def delete_projection(projection_id: str):
    """Permanently remove the projection record from the database."""
    p = _get_projection_or_404(projection_id)
    if p.status == "deleting":
        raise HTTPException(
            status_code=409, detail="Graph deletion in progress, cannot purge yet"
        )
    projection_service.delete(projection_id)
    return {"id": p.id, "status": "deleted"}


@router.post(
    "/{projection_id}/delete-graph",
    summary="Delete associated graph and archive projection",
    status_code=202,
)
def delete_projection_graph(projection_id: str, background_tasks: BackgroundTasks):
    """Delete the Neptune graph in background, then mark projection as archived."""
    p = _get_projection_or_404(projection_id)
    if not p.graph_id:
        raise HTTPException(
            status_code=409, detail="No graph associated with this projection"
        )
    if p.status == "deleting":
        raise HTTPException(status_code=409, detail="Already deleting")

    # Prefix guard: only delete graphs managed by this tool
    client = ClientFactory().neptune()
    resp = get_graph_or_exception(client, p.graph_id)
    assert_managed_graph(resp.get("name"))

    projection_service.update(
        projection_id,
        status="deleting",
        step="graph_delete",
        step_label="Deleting graph",
    )

    async def _delete_graph():
        client = ClientFactory().neptune()
        try:
            client.delete_graph(graphIdentifier=p.graph_id, skipSnapshot=True)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                projection_service.update(
                    projection_id, status="failed", error=sanitize_error_message(str(e))
                )
                return
        # Poll until gone
        for _ in range(60):
            await asyncio.sleep(10)
            try:
                client.get_graph(graphIdentifier=p.graph_id)
            except ClientError:
                break
        else:
            projection_service.update(
                projection_id,
                status="failed",
                error="Timeout waiting for graph deletion",
            )
            return
        projection_service.update(
            projection_id,
            status="archived",
            graph_id=None,
            graph_endpoint=None,
            step=None,
            step_label=None,
            progress=0,
        )

    background_tasks.add_task(_delete_graph)
    return {"id": p.id, "status": "deleting"}


@router.get(
    "/{projection_id}/queries",
    summary="Get node and edge queries for a projection",
    response_model=QueriesResponse,
)
def get_queries(projection_id: str):
    """Return all node, edge, and graph queries for a projection."""
    _get_projection_or_404(projection_id)
    node_queries, edge_queries, graph_queries = projection_service.get_queries(
        projection_id
    )
    return QueriesResponse(
        node_queries=node_queries,  # type: ignore[arg-type]
        edge_queries=edge_queries,  # type: ignore[arg-type]
        graph_queries=graph_queries,  # type: ignore[arg-type]
    )


@router.put(
    "/{projection_id}/queries",
    summary="Save node and edge queries for a projection",
    response_model=QueriesResponse,
)
def save_queries(projection_id: str, body: QueriesPayload):
    """Replace all node, edge, and graph queries for a projection.

    Graph queries persist their openCypher text only — never their results.
    """
    _get_projection_or_404(projection_id)
    node_queries, edge_queries, graph_queries = projection_service.save_queries(
        projection_id, body.node_queries, body.edge_queries, body.graph_queries
    )
    return QueriesResponse(
        node_queries=node_queries,  # type: ignore[arg-type]
        edge_queries=edge_queries,  # type: ignore[arg-type]
        graph_queries=graph_queries,  # type: ignore[arg-type]
    )


@router.put(
    "/{projection_id}/graph-queries",
    summary="Save only the openCypher graph queries for a projection",
    response_model=GraphQueriesResponse,
)
def save_graph_queries(projection_id: str, body: GraphQueriesPayload):
    """Replace the projection's openCypher graph queries, leaving node/edge
    queries untouched. Persists query text only — never query results."""
    _get_projection_or_404(projection_id)
    graph_queries = projection_service.save_graph_queries(
        projection_id, body.graph_queries
    )
    return GraphQueriesResponse(graph_queries=graph_queries)  # type: ignore[arg-type]
