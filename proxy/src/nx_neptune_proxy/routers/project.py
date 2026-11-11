# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import asdict
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

from nx_neptune_proxy.services.project_deletion import delete_project
from nx_neptune_proxy.services.project_store import store
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.projection_store import (
    IN_FLIGHT_STATUSES,
    STATUS_EXECUTING,
    STATUS_IMPORTING,
)

_RUNNING = frozenset({STATUS_IMPORTING, STATUS_EXECUTING})

router = APIRouter(prefix="/api/v0/project", tags=["project"])


class ProjectCreate(BaseModel):
    """Request body for creating a new project."""

    name: str = Field(
        min_length=1, max_length=255, description="Display name for the project"
    )


class ProjectUpdate(BaseModel):
    """Request body for updating an existing project."""

    name: Optional[str] = Field(
        default=None, min_length=1, max_length=255, description="New display name"
    )


class ProjectResponse(BaseModel):
    """Response model representing a project."""

    id: str
    name: str
    status: str
    created_at: datetime


@router.post("", response_model=ProjectResponse, status_code=201)
def create_project(body: ProjectCreate):
    return asdict(store.create(name=body.name))


@router.get("", response_model=list[ProjectResponse])
def list_projects():
    return [asdict(p) for p in store.list()]


@router.get("/{project_id}", response_model=ProjectResponse)
def get_project(project_id: str):
    p = store.get(project_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return asdict(p)


@router.put("/{project_id}", response_model=ProjectResponse)
def update_project(project_id: str, body: ProjectUpdate):
    p = store.update(project_id, **body.model_dump(exclude_unset=True))
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return asdict(p)


@router.delete("/{project_id}", status_code=202)
def delete_project_endpoint(
    project_id: str,
    background_tasks: BackgroundTasks,
    force: bool = Query(False),
):
    """Delete a project and its projections.

    Refuses (409) while a child is importing or executing unless ``force``.
    Children are removed only through the guarded purge, either here (when
    none holds a graph) or by the async job, which claims each one first.
    """
    p = store.get(project_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if p.status == "deleting":
        raise HTTPException(status_code=409, detail="Already deleting")

    projections = projection_service.list_by_project(project_id)
    if not force and any(pr.status in _RUNNING for pr in projections):
        raise HTTPException(
            status_code=409,
            detail="A projection is importing or executing; wait or use force",
        )

    # Fast path: nothing to tear down in AWS and nothing owned by an operation.
    needs_job = any(
        pr.graph_id or pr.status in IN_FLIGHT_STATUSES for pr in projections
    )
    if not needs_job:
        for pr in projections:
            if not projection_service.purge(pr.id):
                # Lost to a concurrent claim (e.g. execute) after the read.
                raise HTTPException(
                    status_code=409,
                    detail="A projection became busy; retry the delete",
                )
        store.delete(project_id)
        return Response(status_code=204)

    store.update(project_id, status="deleting")
    background_tasks.add_task(delete_project, project_id, force)
    return {"id": p.id, "status": "deleting"}
