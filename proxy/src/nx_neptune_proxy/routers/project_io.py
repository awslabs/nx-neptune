# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project import/export endpoints."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store as projection_store

router = APIRouter(prefix="/api/v0/project", tags=["project-io"])

MAX_IMPORT_SIZE = 5 * 1024 * 1024  # 5 MB


# --- Export/Import JSON schema ---


class ProjectionExport(BaseModel):
    """Projection config fields only — no runtime state."""

    catalog: str = "AwsDataCatalog"
    database: Optional[str] = None
    node_query: Optional[str] = None
    edge_query: Optional[str] = None
    graph_name: Optional[str] = None
    graph_memory_gb: int = 16
    s3_staging_bucket: Optional[str] = None

    model_config = {"extra": "forbid"}


class ProjectExportPayload(BaseModel):
    """Top-level export format for a single project."""

    version: str = "1.0"
    project: dict
    projections: list[ProjectionExport] = []

    model_config = {"extra": "forbid"}


# --- Export endpoint ---


@router.get("/{project_id}/export", summary="Export a project")
def export_project(project_id: str):
    """Export a project and its projections as JSON."""
    p = project_store.get(project_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")

    projections = [
        pr for pr in projection_store.list() if pr.project_id == project_id
    ]

    payload = {
        "version": "1.0",
        "project": {"name": p.name},
        "projections": [
            ProjectionExport(
                catalog=pr.catalog,
                database=pr.database,
                node_query=pr.node_query,
                edge_query=pr.edge_query,
                graph_name=pr.graph_name,
                graph_memory_gb=pr.graph_memory_gb,
                s3_staging_bucket=pr.s3_staging_bucket,
            ).model_dump()
            for pr in projections
        ],
    }

    return JSONResponse(
        content=payload,
        headers={"Content-Disposition": f'attachment; filename="{p.name}.json"'},
    )


# --- Import endpoint ---


@router.post("/import", summary="Import a project from JSON", status_code=201)
async def import_project(request: Request):
    """Import a project and its projections from a JSON payload."""
    # Size check
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > MAX_IMPORT_SIZE:
        raise HTTPException(status_code=413, detail="Payload too large (max 5 MB)")

    contents = await request.body()
    if len(contents) > MAX_IMPORT_SIZE:
        raise HTTPException(status_code=413, detail="Payload too large (max 5 MB)")

    # Parse and validate
    try:
        payload = ProjectExportPayload.model_validate_json(contents)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    # Create project
    name = payload.project.get("name", "Imported Project")
    p = project_store.create(name=name)

    # Create projections
    for pr_data in payload.projections:
        projection_store.create(
            catalog=pr_data.catalog,
            database=pr_data.database,
            node_query=pr_data.node_query,
            edge_query=pr_data.edge_query,
            graph_name=pr_data.graph_name,
            graph_memory_gb=pr_data.graph_memory_gb,
            s3_staging_bucket=pr_data.s3_staging_bucket,
            project_id=p.id,
        )

    return {"imported": {"id": p.id, "name": p.name}}
