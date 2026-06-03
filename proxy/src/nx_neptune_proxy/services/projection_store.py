# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

GRAPH_PREFIX = "nxp-"


@dataclass
class Projection:
    """State for a single projection pipeline."""

    id: str
    status: str  # draft | validating | validated | executing | complete | failed
    catalog: str = "AwsDataCatalog"
    database: Optional[str] = None
    sql_query: Optional[str] = None
    graph_name: Optional[str] = None
    graph_memory_gb: int = 16
    s3_staging_bucket: Optional[str] = None
    graph_id: Optional[str] = None
    graph_endpoint: Optional[str] = None
    step: Optional[str] = None
    step_label: Optional[str] = None
    progress: float = 0
    error: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ProjectionStore:
    """In-memory projection state store."""

    def __init__(self) -> None:
        self._projections: dict[str, Projection] = {}

    def create(self, catalog: str = "AwsDataCatalog", database: str = None,
               sql_query: str = None, graph_name: str = None,
               graph_memory_gb: int = 16, s3_staging_bucket: str = None) -> Projection:
        projection = Projection(
            id=str(uuid.uuid4()),
            status="draft",
            catalog=catalog,
            database=database,
            sql_query=sql_query,
            graph_name=graph_name,
            graph_memory_gb=graph_memory_gb,
            s3_staging_bucket=s3_staging_bucket,
        )
        self._projections[projection.id] = projection
        return projection

    def get(self, projection_id: str) -> Optional[Projection]:
        return self._projections.get(projection_id)

    def list(self) -> list[Projection]:
        return list(self._projections.values())

    def update(self, projection_id: str, **kwargs) -> Optional[Projection]:
        projection = self._projections.get(projection_id)
        if projection is None:
            return None
        for key, value in kwargs.items():
            if value is not None and hasattr(projection, key):
                setattr(projection, key, value)
        return projection

    def delete(self, projection_id: str) -> bool:
        return self._projections.pop(projection_id, None) is not None


# Singleton store instance
store = ProjectionStore()
