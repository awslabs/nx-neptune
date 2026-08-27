# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project import/export endpoints.

Enables exporting a project's configuration as a portable JSON file
and re-importing it to recreate the project in another environment.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path
from pydantic import BaseModel, Field

from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.routers.schemas import ProjectExportPayload, ProjectionExport
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store as projection_store
from nx_neptune_proxy.utils.aws_helper import (
    check_body_size,
    check_content_length,
    check_key_not_exists,
    friendly_s3_error,
    list_s3_json_objects,
    require_name,
)
from nx_neptune_proxy.utils.sanitize import sanitize_filename, sanitize_s3_key_name

router = APIRouter(prefix="/api/v0/project", tags=["project-io"])

MAX_IMPORT_SIZE = 5 * 1024 * 1024  # 5 MB
MAX_PROJECT_NAME_LENGTH = 100


# --- Helpers ---


def _get_export_bucket_tuple() -> tuple:
    """Return (bucket, prefix) or raise 404 if not configured."""
    settings = Settings.from_env()
    if not settings.config_bucket:
        raise HTTPException(status_code=404, detail="S3 export bucket not configured")
    bucket, prefix = split_s3_arn_to_bucket_and_path(settings.config_bucket)
    return bucket, prefix.rstrip("/")


def _s3_object_to_entry(obj: dict) -> dict:
    """Convert an S3 object dict to an API response entry."""
    return {
        "key": obj["Key"],
        "filename": obj["Key"].rsplit("/", 1)[-1],
        "last_modified": obj["LastModified"].isoformat(),
    }


def _build_export_payload(project_id: str) -> tuple[dict, str]:
    """Build the export JSON payload for a project. Returns (payload_dict, project_name)."""
    p = project_store.get(project_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")

    projections = projection_store.list_by_project(project_id)
    payload = ProjectExportPayload.from_project(p, projections)
    return payload.model_dump(), p.name


def _build_s3_key(name: str, prefix: str) -> tuple[str, str]:
    """Build S3 key from project name and prefix. Returns (key, filename)."""
    sanitized = sanitize_s3_key_name(name) or "project"
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S") + f"{now.microsecond // 1000:03d}"
    filename = f"{sanitized}_{timestamp}.json"
    key = f"{prefix}/{filename}" if prefix else filename
    return key, filename


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
        projection_store.create(**pr_data.model_dump(), project_id=p.id)

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


@router.post("/{project_id}/export/s3", summary="Export a project to S3")
def export_project_to_s3(project_id: str):
    """Export a project and its projections to the configured S3 bucket."""
    bucket, prefix = _get_export_bucket_tuple()
    payload, name = _build_export_payload(project_id)
    key, filename = _build_s3_key(name, prefix)

    s3 = ClientFactory().s3()
    try:
        check_key_not_exists(s3, bucket, key)
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, indent=2),
            ContentType="application/json",
            Tagging="graph_studio=true",
        )
    except HTTPException:
        raise
    except ClientError as e:
        raise HTTPException(status_code=502, detail=friendly_s3_error(e))

    return {"filename": filename, "key": key}


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


@router.get("/import/s3/list", summary="List available exports from S3")
def list_s3_exports():
    """List the last 10 export files from S3 (filtered by prefix)."""
    bucket, prefix = _get_export_bucket_tuple()
    s3 = ClientFactory().s3()

    try:
        json_objects = list_s3_json_objects(s3, bucket, prefix)

        # Sort by last modified descending, take last 10
        json_objects.sort(key=lambda o: o["LastModified"], reverse=True)
        recent = json_objects[:10]

        return {"files": [_s3_object_to_entry(obj) for obj in recent]}
    except ClientError as e:
        raise HTTPException(status_code=502, detail=friendly_s3_error(e))


@router.post("/import/s3", summary="Import a project from S3", status_code=201)
def import_project_from_s3(request_body: dict):
    """Import a project from a specific S3 file."""
    key = request_body.get("key")
    if not key:
        raise HTTPException(status_code=400, detail="Missing 'key' in request body")

    bucket, _ = _get_export_bucket_tuple()
    s3 = ClientFactory().s3()

    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
        contents = resp["Body"].read()
    except ClientError as e:
        raise HTTPException(status_code=502, detail=friendly_s3_error(e))

    check_body_size(contents, MAX_IMPORT_SIZE)
    payload = _parse_payload(contents)
    result = _import_from_payload(payload)
    return {"imported": result}
