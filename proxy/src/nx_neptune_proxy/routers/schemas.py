# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pydantic import BaseModel


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


class CatalogsResponse(BaseModel):
    catalogs: list[str]


class NeptuneAnalyticsGraph(BaseModel):
    id: str
    name: str
    status: str


class NeptuneAnalyticsGraphsResponse(BaseModel):
    graphs: list[NeptuneAnalyticsGraph]
