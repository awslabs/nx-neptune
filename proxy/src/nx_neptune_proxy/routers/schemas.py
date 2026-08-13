# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


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
    graph_name: Optional[str] = None
    graph_memory_gb: int = 16
    s3_staging_bucket: Optional[str] = None
    project_id: Optional[str] = None


class ProjectionUpdate(BaseModel):
    catalog: Optional[str] = None
    database: Optional[str] = None
    sql_query: Optional[str] = None
    node_query: Optional[str] = None
    edge_query: Optional[str] = None
    graph_name: Optional[str] = None
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


# --- Schema Preview ---


class SchemaNode(BaseModel):
    id: str
    label: str


class SchemaEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str


class SchemaPreviewResponse(BaseModel):
    nodes: list[SchemaNode]
    edges: list[SchemaEdge]


class DiscoverLabelsResponse(BaseModel):
    node_labels: list[str]
    edge_labels: list[str]


# --- Multi-Query ---


class NodeQueryInput(BaseModel):
    id: Optional[str] = None
    sql: str = ""


class EdgeQueryInput(BaseModel):
    id: Optional[str] = None
    sql: str = ""
    from_type: Optional[str] = None
    to_type: Optional[str] = None


class NodeQueryResponse(BaseModel):
    id: str
    sql: str
    position: int


class EdgeQueryResponse(BaseModel):
    id: str
    sql: str
    from_type: Optional[str] = None
    to_type: Optional[str] = None
    position: int


class QueriesPayload(BaseModel):
    node_queries: list[NodeQueryInput] = []
    edge_queries: list[EdgeQueryInput] = []


class QueriesResponse(BaseModel):
    node_queries: list[NodeQueryResponse]
    edge_queries: list[EdgeQueryResponse]
