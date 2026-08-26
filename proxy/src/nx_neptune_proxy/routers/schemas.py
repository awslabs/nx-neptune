# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

# --- graph_name validation ---

# Neptune Analytics graph names in this tool are formed as `{prefix}{graph_name}`.
# Constrain the user-supplied portion so it cannot be empty/short and collide with
# the managed prefix (which previously allowed adopting/resetting an unrelated
# graph). First character must be alphanumeric; remaining may include
# hyphen/underscore. Upper- and lower-case are both allowed.
GRAPH_NAME_MIN_LENGTH = 3
GRAPH_NAME_MAX_LENGTH = 63
GRAPH_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_-]{2,62}$"


# --- Metadata responses ---


class DatabasesResponse(BaseModel):
    databases: list[str]


class TablesResponse(BaseModel):
    tables: list[str]


class Column(BaseModel):
    name: str
    type: str


class ColumnsResponse(BaseModel):
    columns: list[Column]


class BucketsResponse(BaseModel):
    buckets: list[str]


class Catalog(BaseModel):
    name: str
    status: str


class CatalogsResponse(BaseModel):
    catalogs: list[Catalog]


class NeptuneAnalyticsGraph(BaseModel):
    id: str
    name: str
    status: str


class NeptuneAnalyticsGraphsResponse(BaseModel):
    graphs: list[NeptuneAnalyticsGraph]


# --- Projection ---


class ProjectionCreate(BaseModel):
    catalog: str = "AwsDataCatalog"
    database: Optional[str] = None
    sql_query: Optional[str] = None
    node_query: Optional[str] = None
    edge_query: Optional[str] = None
    graph_name: Optional[str] = Field(
        default=None,
        min_length=GRAPH_NAME_MIN_LENGTH,
        max_length=GRAPH_NAME_MAX_LENGTH,
        pattern=GRAPH_NAME_PATTERN,
    )
    graph_memory_gb: int = 16
    s3_staging_bucket: Optional[str] = None
    project_id: Optional[str] = None


class ProjectionUpdate(BaseModel):
    catalog: Optional[str] = None
    database: Optional[str] = None
    sql_query: Optional[str] = None
    node_query: Optional[str] = None
    edge_query: Optional[str] = None
    graph_name: Optional[str] = Field(
        default=None,
        min_length=GRAPH_NAME_MIN_LENGTH,
        max_length=GRAPH_NAME_MAX_LENGTH,
        pattern=GRAPH_NAME_PATTERN,
    )
    graph_memory_gb: Optional[int] = None
    s3_staging_bucket: Optional[str] = None


class CheckResult(BaseModel):
    check: str
    passed: bool
    message: Optional[str] = None


class ValidateResponse(BaseModel):
    valid: bool
    checks: list[CheckResult]


class PreviewQueryResult(BaseModel):
    columns: list[str]
    rows: list[list[str]]


class PreviewResponse(BaseModel):
    error: Optional[str] = None
    results: list[PreviewQueryResult]


class ProjectionStatus(BaseModel):
    id: str
    status: str
    step: Optional[str] = None
    step_label: Optional[str] = None
    progress: float = 0
    error: Optional[str] = None
    graph_endpoint: Optional[str] = None


class ProjectionResponse(BaseModel):
    id: str
    status: str
    catalog: str
    database: Optional[str] = None
    sql_query: Optional[str] = None
    node_query: Optional[str] = None
    edge_query: Optional[str] = None
    graph_name: Optional[str] = None
    graph_id: Optional[str] = None
    graph_endpoint: Optional[str] = None
    graph_memory_gb: int
    s3_staging_bucket: Optional[str] = None
    project_id: Optional[str] = None
    step: Optional[str] = None
    step_label: Optional[str] = None
    progress: float = 0
    error: Optional[str] = None
    created_at: datetime
