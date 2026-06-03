# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, Query

from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune_proxy.config import Settings
from nx_neptune_proxy.routers.schemas import (
    BucketsResponse,
    CatalogsResponse,
    ColumnsResponse,
    DatabasesResponse,
    NeptuneAnalyticsGraphsResponse,
    TablesResponse,
)
from nx_neptune_proxy.utils import paginate_aws

router = APIRouter(prefix="/api/v0/metadata", tags=["metadata"])


@router.get("/athena/catalogs", summary="List Athena data catalogs", response_model=CatalogsResponse)
def list_athena_catalogs():
    """List all Athena data catalogs"""
    client = ClientFactory().athena()
    items = paginate_aws(client.list_data_catalogs, "DataCatalogsSummary")
    return {"catalogs": [c["CatalogName"] for c in items]}


@router.get("/athena/databases", summary="List Athena databases", response_model=DatabasesResponse)
def list_athena_databases(catalog: str = Query("AwsDataCatalog", description="Athena catalog name")):
    """List all databases in the specified Athena catalog"""
    client = ClientFactory().athena()
    items = paginate_aws(client.list_databases, "DatabaseList", CatalogName=catalog)
    return {"databases": [db["Name"] for db in items]}


@router.get("/athena/tables", summary="List Athena tables", response_model=TablesResponse)
def list_athena_tables(
    database: str = Query(..., description="Database name"),
    catalog: str = Query("AwsDataCatalog", description="Athena catalog name"),
):
    """List all tables in the specified Athena database"""
    client = ClientFactory().athena()
    items = paginate_aws(client.list_table_metadata, "TableMetadataList", CatalogName=catalog, DatabaseName=database)
    return {"tables": [t["Name"] for t in items]}


@router.get("/athena/columns", summary="List Athena table columns", response_model=ColumnsResponse)
def list_athena_columns(
    database: str = Query(..., description="Database name"),
    table: str = Query(..., description="Table name"),
    catalog: str = Query("AwsDataCatalog", description="Athena catalog name"),
):
    """List columns and their types for the specified Athena table"""
    client = ClientFactory().athena()
    resp = client.get_table_metadata(CatalogName=catalog, DatabaseName=database, TableName=table)
    columns = resp["TableMetadata"].get("Columns", [])
    return {"columns": [{"name": c["Name"], "type": c["Type"]} for c in columns]}


@router.get("/s3/buckets", summary="List S3 buckets", response_model=BucketsResponse)
def list_s3_buckets():
    """List S3 buckets in the configured region"""
    client = ClientFactory().s3()
    filter_region = Settings.from_env().region
    kwargs = {}
    if filter_region:
        kwargs["BucketRegion"] = filter_region
    buckets = [b["Name"] for b in client.list_buckets(**kwargs).get("Buckets", [])]
    return {"buckets": buckets}


@router.get("/neptune/graph-analytics", summary="List Neptune Analytics graphs", response_model=NeptuneAnalyticsGraphsResponse)
def list_neptune_graphs():
    """List all Neptune Analytics graphs in the configured region"""
    client = ClientFactory().neptune()
    items = paginate_aws(client.list_graphs, "graphs")
    return {"graphs": [{"id": g["id"], "name": g["name"], "status": g["status"]} for g in items]}
