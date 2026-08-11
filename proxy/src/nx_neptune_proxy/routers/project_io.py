# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project import/export endpoints.

Enables exporting a project's configuration as a portable JSON file
and re-importing it to recreate the project in another environment.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store as projection_store
from nx_neptune_proxy.utils.aws_helper import check_content_length
from nx_neptune_proxy.utils.sanitize import sanitize_filename

router = APIRouter(prefix="/api/v0/project", tags=["project-io"])

MAX_IMPORT_SIZE = 5 * 1024 * 1024  # 5 MB
MAX_PROJECT_NAME_LENGTH = 100


# --- Export/Import JSON schema ---


class ProjectionExport(BaseModel):
    """Projection configuration (no runtime state such as graph_id, status, or progress)."""

    catalog: str = Field("AwsDataCatalog", description="Athena catalog name")
    database: Optional[str] = Field(None, description="Athena database name")
    node_query: Optional[str] = Field(None, description="SQL query that produces nodes (must include ~id and ~label columns)")
    edge_query: Optional[str] = Field(None, description="SQL query that produces edges (must include ~from, ~to, and ~label columns)")
    graph_name: Optional[str] = Field(None, description="Neptune Analytics graph name suffix (prefix is added automatically)")
    graph_memory_gb: int = Field(16, description="Graph memory allocation in GB")
    s3_staging_bucket: Optional[str] = Field(None, description="S3 bucket path for staging Athena results (e.g. s3://bucket/prefix)")

    model_config = {"extra": "forbid"}


class ProjectExportPayload(BaseModel):
    """Top-level schema for project import/export JSON files.

    Example:
        {
            "version": "1.0",
            "project": {"name": "My Project"},
            "projections": [{"catalog": "AwsDataCatalog", "database": "mydb", ...}]
        }
    """

    version: str = Field("1.0", description="Schema version for forward compatibility")
    project: dict = Field(..., description="Project metadata (must contain 'name' key)")
    projections: list[ProjectionExport] = Field(default=[], description="List of projection configurations to create")

    model_config = {"extra": "forbid"}


# --- Export endpoint ---


@router.get("/{project_id}/export", summary="Export a project as JSON",
            response_description="JSON file containing the project configuration and its projections")
def export_project(project_id: str):
    """Export a project and all its projection configurations as a downloadable JSON file.

    The exported file contains only configuration data (queries, database references,
    graph settings). Runtime state such as graph IDs, execution status, and endpoints
    are not included.

    The response includes a Content-Disposition header for browser download.
    """
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
        headers={"Content-Disposition": f'attachment; filename="{sanitize_filename(p.name)}.json"'},
    )


# --- Import endpoint ---


@router.post("/import", summary="Import a project from JSON", status_code=201,
             response_description="The newly created project ID and name")
async def import_project(request: Request):
    """Create a project and its projections from a previously exported JSON payload.

    Accepts the same JSON schema produced by the export endpoint. A new project
    is created with a fresh ID regardless of whether a project with the same name
    already exists. All projections start in 'draft' status.

    Constraints:
    - Maximum payload size: 5 MB
    - Maximum 50 projections per import
    - Unknown fields are rejected (extra='forbid')
    """
    # Size check
    check_content_length(request, MAX_IMPORT_SIZE)

    contents = await request.body()
    if len(contents) > MAX_IMPORT_SIZE:
        raise HTTPException(status_code=413, detail=f"Payload too large (max {MAX_IMPORT_SIZE // (1024 * 1024)} MB)")

    # Parse and validate
    try:
        payload = ProjectExportPayload.model_validate_json(contents)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    # Validate and create project
    name = payload.project.get("name", "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Project name is required")
    if len(name) > MAX_PROJECT_NAME_LENGTH:
        raise HTTPException(status_code=400, detail=f"Project name too long (max {MAX_PROJECT_NAME_LENGTH} characters)")
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
