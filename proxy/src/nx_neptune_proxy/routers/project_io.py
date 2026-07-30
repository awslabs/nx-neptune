# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project import/export endpoints."""

from __future__ import annotations

from dataclasses import asdict
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store as projection_store

router = APIRouter(prefix="/api/v0/project", tags=["project-io"])

MAX_IMPORT_SIZE = 5 * 1024 * 1024  # 5 MB
MAX_PROJECTS_PER_IMPORT = 50


# --- Export/Import JSON schema ---


class ProjectionExport(BaseModel):
    """Projection config fields only — no runtime state."""

    catalog: str = "AwsDataCatalog"
    database: Optional[str] = None
    sql_query: Optional[str] = None
    node_query: Optional[str] = None
    edge_query: Optional[str] = None
    graph_name: Optional[str] = None
    graph_memory_gb: int = 16
    s3_staging_bucket: Optional[str] = None

    model_config = {"extra": "forbid"}


class ProjectExportEntry(BaseModel):
    """A single project with its projections."""

    project: dict = Field(description="Project metadata")
    projections: list[ProjectionExport] = []

    model_config = {"extra": "forbid"}


class ProjectExportPayload(BaseModel):
    """Top-level export format. Supports single or multi-project."""

    version: str = "1.0"
    # Single project export
    project: Optional[dict] = None
    projections: Optional[list[ProjectionExport]] = None
    # Multi-project export
    projects: Optional[list[ProjectExportEntry]] = None

    model_config = {"extra": "forbid"}

    @model_validator(mode="after")
    def check_structure(self):
        has_single = self.project is not None
        has_multi = self.projects is not None
        if not has_single and not has_multi:
            raise ValueError("Must provide either 'project' + 'projections' or 'projects'")
        if has_single and has_multi:
            raise ValueError("Cannot provide both 'project' and 'projects'")
        return self


# --- Export endpoints ---


@router.get("/{project_id}/export", summary="Export a single project")
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
                sql_query=pr.sql_query,
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


@router.get("/export", summary="Export all projects")
def export_all_projects():
    """Export all projects and their projections as JSON."""
    all_projects = project_store.list()
    all_projections = projection_store.list()

    entries = []
    for p in all_projects:
        projections = [pr for pr in all_projections if pr.project_id == p.id]
        entries.append({
            "project": {"name": p.name},
            "projections": [
                ProjectionExport(
                    catalog=pr.catalog,
                    database=pr.database,
                    sql_query=pr.sql_query,
                    node_query=pr.node_query,
                    edge_query=pr.edge_query,
                    graph_name=pr.graph_name,
                    graph_memory_gb=pr.graph_memory_gb,
                    s3_staging_bucket=pr.s3_staging_bucket,
                ).model_dump()
                for pr in projections
            ],
        })

    payload = {"version": "1.0", "projects": entries}

    return JSONResponse(
        content=payload,
        headers={"Content-Disposition": 'attachment; filename="projects-export.json"'},
    )


# --- Import endpoint ---


@router.post("/import", summary="Import project(s) from JSON", status_code=201)
async def import_projects(request: Request):
    """Import one or more projects from a JSON payload."""
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

    # Normalize to list of entries
    entries: list[tuple[dict, list[ProjectionExport]]] = []
    if payload.projects:
        if len(payload.projects) > MAX_PROJECTS_PER_IMPORT:
            raise HTTPException(
                status_code=400,
                detail=f"Too many projects (max {MAX_PROJECTS_PER_IMPORT})",
            )
        for entry in payload.projects:
            entries.append((entry.project, entry.projections))
    else:
        entries.append((payload.project, payload.projections or []))

    # Create projects and projections
    created = []
    for project_data, projections_data in entries:
        name = project_data.get("name", "Imported Project")
        p = project_store.create(name=name)

        for pr_data in projections_data:
            projection_store.create(
                catalog=pr_data.catalog,
                database=pr_data.database,
                sql_query=pr_data.sql_query,
                node_query=pr_data.node_query,
                edge_query=pr_data.edge_query,
                graph_name=pr_data.graph_name,
                graph_memory_gb=pr_data.graph_memory_gb,
                s3_staging_bucket=pr_data.s3_staging_bucket,
                project_id=p.id,
            )

        created.append({"id": p.id, "name": p.name})

    return {"imported": created}
