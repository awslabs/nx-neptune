# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project import/export endpoints.

Enables exporting a project's configuration as a portable JSON file
and re-importing it to recreate the project in another environment.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from nx_neptune_proxy.routers.schemas import ProjectExportPayload
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.utils.aws_helper import (
    check_body_size,
    check_content_length,
    require_name,
)
from nx_neptune_proxy.utils.sanitize import sanitize_filename

router = APIRouter(prefix="/api/v0/project", tags=["project-io"])

MAX_IMPORT_SIZE = 5 * 1024 * 1024  # 5 MB
MAX_PROJECT_NAME_LENGTH = 100


# --- Helpers ---


def _build_export_payload(project_id: str) -> tuple[dict, str]:
    """Build the export JSON payload for a project. Returns (payload_dict, project_name)."""
    p = project_store.get(project_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")

    projections, queries_by_projection = (
        projection_service.list_projections_with_query_sql(project_id)
    )
    payload = ProjectExportPayload.from_project(p, projections, queries_by_projection)
    return payload.model_dump(), p.name


def _parse_payload(contents: bytes) -> ProjectExportPayload:
    """Parse and validate export JSON. Raises 400 on failure."""
    try:
        return ProjectExportPayload.model_validate_json(contents)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")


def _import_from_payload(payload: ProjectExportPayload) -> dict:
    """Create project and projections from validated payload. Returns {id, name}."""
    name = require_name(payload.project.get("name"), max_length=MAX_PROJECT_NAME_LENGTH)

    p = project_store.create(name=name)

    for pr_data in payload.projections:
        pr_dict = pr_data.model_dump(exclude={"node_queries", "edge_queries"})
        pr_dict["project_id"] = p.id
        projection_service.create_with_queries(
            pr_dict, pr_data.node_queries, pr_data.edge_queries
        )

    return {"id": p.id, "name": p.name}


# --- Export endpoints ---


@router.get(
    "/{project_id}/export",
    summary="Export a project as JSON",
    response_description="JSON file containing the project configuration and its projections",
)
def export_project(project_id: str):
    """Export a project and all its projection configurations as a downloadable JSON file.

    The exported file contains only configuration data (queries, database references,
    graph settings). Runtime state such as graph IDs, execution status, and endpoints
    are not included.

    The response includes a Content-Disposition header for browser download.
    """
    payload, name = _build_export_payload(project_id)

    return JSONResponse(
        content=payload,
        headers={
            "Content-Disposition": f'attachment; filename="{sanitize_filename(name)}.json"'
        },
    )


# --- Import endpoints ---


@router.post("/import", summary="Import a project from JSON", status_code=201)
async def import_project(request: Request):
    """Import a project and its projections from a JSON payload."""
    check_content_length(request, MAX_IMPORT_SIZE)
    contents = await request.body()
    check_body_size(contents, MAX_IMPORT_SIZE)

    payload = _parse_payload(contents)
    result = _import_from_payload(payload)
    return {"imported": result}
