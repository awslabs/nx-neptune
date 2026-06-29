# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
from dataclasses import asdict
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from nx_neptune_proxy.services.project_deletion import delete_project
from nx_neptune_proxy.services.project_store import store

router = APIRouter(prefix="/api/v0/project", tags=["project"])


class ProjectCreate(BaseModel):
    name: str


class ProjectUpdate(BaseModel):
    name: Optional[str] = None


class ProjectResponse(BaseModel):
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
    p.status = "deleting"
    background_tasks.add_task(asyncio.run, delete_project(project_id))
    return {"id": p.id, "status": "deleting"}
