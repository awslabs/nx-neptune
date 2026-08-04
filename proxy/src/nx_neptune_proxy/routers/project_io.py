# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Project import/export endpoints."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.services.project_store import store as project_store
from nx_neptune_proxy.services.projection_store import store as projection_store
from nx_neptune_proxy.utils.aws_helper import friendly_s3_error, check_content_length, check_body_size, check_key_not_exists, list_s3_json_objects
from nx_neptune_proxy.utils.sanitize import sanitize_s3_key_name

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


# --- Helpers ---


def _parse_bucket_config(export_bucket: str) -> tuple[str, str]:
    """Parse 'bucket' or 'bucket/prefix' into (bucket, prefix)."""
    parts = export_bucket.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix


def _require_export_bucket() -> tuple[str, str]:
    """Return (bucket, prefix) or raise 404 if not configured."""
    settings = Settings.from_env()
    if not settings.export_bucket:
        raise HTTPException(status_code=404, detail="S3 export bucket not configured")
    return _parse_bucket_config(settings.export_bucket)


def _build_export_payload(project_id: str) -> tuple[dict, str]:
    """Build the export JSON payload for a project. Returns (payload_dict, project_name)."""
    p = project_store.get(project_id)
    if p is None:
        raise HTTPException(status_code=404, detail="Project not found")

    projections = [pr for pr in projection_store.list() if pr.project_id == project_id]

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
    return payload, p.name


def _build_s3_key(name: str, prefix: str) -> tuple[str, str]:
    """Build S3 key from project name and prefix. Returns (key, filename)."""
    sanitized = sanitize_s3_key_name(name)
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
    name = payload.project.get("name", "Imported Project")
    p = project_store.create(name=name)

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

    return {"id": p.id, "name": p.name}



# --- Export endpoints ---


@router.get("/{project_id}/export", summary="Export a project")
def export_project(project_id: str):
    """Export a project and its projections as JSON (download)."""
    payload, name = _build_export_payload(project_id)

    return JSONResponse(
        content=payload,
        headers={"Content-Disposition": f'attachment; filename="{name}.json"'},
    )


@router.post("/{project_id}/export/s3", summary="Export a project to S3")
def export_project_to_s3(project_id: str):
    """Export a project and its projections to the configured S3 bucket."""
    bucket, prefix = _require_export_bucket()
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
    bucket, prefix = _require_export_bucket()
    s3 = ClientFactory().s3()

    try:
        json_objects = list_s3_json_objects(s3, bucket, prefix)

        # Sort by last modified descending, take last 10
        json_objects.sort(key=lambda o: o["LastModified"], reverse=True)
        recent = json_objects[:10]

        return {
            "files": [
                {
                    "key": obj["Key"],
                    "filename": obj["Key"].rsplit("/", 1)[-1],
                    "last_modified": obj["LastModified"].isoformat(),
                }
                for obj in recent
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=502, detail=friendly_s3_error(e))


@router.post("/import/s3", summary="Import a project from S3", status_code=201)
def import_project_from_s3(request_body: dict):
    """Import a project from a specific S3 file."""
    key = request_body.get("key")
    if not key:
        raise HTTPException(status_code=400, detail="Missing 'key' in request body")

    bucket, _ = _require_export_bucket()
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
