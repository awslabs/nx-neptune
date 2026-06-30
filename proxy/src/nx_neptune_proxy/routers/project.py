# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from dataclasses import asdict
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field

from nx_neptune_proxy.services.project_deletion import delete_project
from nx_neptune_proxy.services.project_store import store
from nx_neptune_proxy.services.projection_store import store as projection_store

router = APIRouter(prefix="/api/v0/project", tags=["project"])


class ProjectCreate(BaseModel):
    """Request body for creating a new project."""

    name: str = Field(min_length=1, max_length=255, description="Display name for the project")


class ProjectUpdate(BaseModel):
    """Request body for updating an existing project."""

    name: Optional[str] = Field(default=None, min_length=1, max_length=255, description="New display name")


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
def delete_project_endpoint(project_id: str, background_tasks: BackgroundTasks):
    p = store.get(project_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")
    if p.status == "deleting":
        raise HTTPException(status_code=409, detail="Already deleting")

    # If no graphs to delete, do it immediately
    projections = [pr for pr in projection_store.list() if pr.project_id == project_id]
    has_graphs = any(pr.graph_id for pr in projections)

    if not has_graphs:
        for pr in projections:
            projection_store.delete(pr.id)
        store.delete(project_id)
        return Response(status_code=204)

    # Has graphs — async deletion
    store.update(project_id, status="deleting")
    background_tasks.add_task(delete_project, project_id)
    return {"id": p.id, "status": "deleting"}
