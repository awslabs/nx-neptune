# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import asyncio
from dataclasses import asdict
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from nx_neptune_proxy.services.workspace_deletion import delete_workspace
from nx_neptune_proxy.services.workspace_store import store

router = APIRouter(prefix="/api/v0/workspace", tags=["workspace"])


class WorkspaceCreate(BaseModel):
    name: str


class WorkspaceUpdate(BaseModel):
    name: Optional[str] = None


class WorkspaceResponse(BaseModel):
    id: str
    name: str
    status: str
    created_at: datetime


@router.post("", response_model=WorkspaceResponse, status_code=201)
def create_workspace(body: WorkspaceCreate):
    return asdict(store.create(name=body.name))


@router.get("", response_model=list[WorkspaceResponse])
def list_workspaces():
    return [asdict(ws) for ws in store.list()]


@router.get("/{workspace_id}", response_model=WorkspaceResponse)
def get_workspace(workspace_id: str):
    ws = store.get(workspace_id)
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return asdict(ws)


@router.put("/{workspace_id}", response_model=WorkspaceResponse)
def update_workspace(workspace_id: str, body: WorkspaceUpdate):
    ws = store.update(workspace_id, **body.model_dump(exclude_unset=True))
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return asdict(ws)


@router.delete("/{workspace_id}", status_code=202)
def delete_workspace_endpoint(workspace_id: str, background_tasks: BackgroundTasks):
    ws = store.get(workspace_id)
    if ws is None:
        raise HTTPException(status_code=404, detail="Workspace not found")
    if ws.status == "deleting":
        raise HTTPException(status_code=409, detail="Already deleting")
    ws.status = "deleting"
    background_tasks.add_task(asyncio.run, delete_workspace(workspace_id))
    return {"id": ws.id, "status": "deleting"}
