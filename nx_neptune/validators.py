# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Resource validation for nx-neptune migrations.

Provides upfront checks for S3, Athena, and Neptune Analytics resources
so configuration errors surface immediately — not minutes into a pipeline.
"""

import asyncio
import logging
import re
from dataclasses import dataclass
from typing import Optional

from botocore.exceptions import ClientError

from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.clients.response_utils import (
    get_caller_arn,
    get_graph_names,
    get_kms_key_id,
    get_object_count,
    get_query_failure_reason,
    get_query_result_columns,
    get_query_state,
    get_table_columns,
    is_access_denied,
    is_entity_not_found,
    is_kms_encrypted,
    is_not_found,
    is_versioning_enabled,
)
from nx_neptune.instance_management import _execute_athena_query
from nx_neptune.utils.task_future import TaskType, wait_until_all_complete

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    check_name: str
    passed: bool
    message: str

    @classmethod
    def ok(cls, check: str, msg: str) -> "CheckResult":
        return cls(check, True, msg)

    @classmethod
    def fail(cls, check: str, msg: str) -> "CheckResult":
        return cls(check, False, msg)

    def to_dict(self) -> dict:
        return {
            "check": self.check_name,
            "passed": self.passed,
            "message": self.message,
        }


def _parse_bucket(s3_uri: str) -> str:
    """Extract bucket name from s3://bucket/path/."""
    return s3_uri.replace("s3://", "").split("/")[0]


def _parse_prefix(s3_uri: str) -> str:
    """Extract prefix from s3://bucket/prefix/."""
    parts = s3_uri.replace("s3://", "").split("/", 1)
    return parts[1] if len(parts) > 1 else ""


# --- S3 checks ---


def check_bucket_exists(s3_uri: str) -> CheckResult:
    """Check that the S3 bucket exists and is accessible."""
    bucket = _parse_bucket(s3_uri)
    try:
        ClientFactory().s3().head_bucket(Bucket=bucket)
        return CheckResult.ok("s3_bucket_exists", f"Bucket '{bucket}' exists")
    except ClientError as e:
        if is_not_found(e):
            return CheckResult.fail("s3_bucket_exists", f"Bucket '{bucket}' not found")
        if is_access_denied(e):
            return CheckResult.fail(
                "s3_bucket_exists", f"Access denied to bucket '{bucket}'"
            )
        return CheckResult.fail("s3_bucket_exists", str(e))


def check_bucket_region(s3_uri: str, expected_region: str) -> CheckResult:
    """Check that the bucket is in the expected region."""
    bucket = _parse_bucket(s3_uri)
    try:
        resp = ClientFactory().s3().head_bucket(Bucket=bucket)
    except ClientError as e:
        return CheckResult.fail("s3_bucket_region", str(e))

    location = resp["BucketRegion"]
    if location == expected_region:
        return CheckResult.ok(
            "s3_bucket_region", f"Bucket '{bucket}' is in {expected_region}"
        )
    return CheckResult.fail(
        "s3_bucket_region",
        f"Bucket '{bucket}' is in {location}, expected {expected_region}",
    )


def check_bucket_encryption(s3_uri: str) -> CheckResult:
    """Check that the bucket has KMS encryption configured."""
    bucket = _parse_bucket(s3_uri)
    try:
        resp = ClientFactory().s3().get_bucket_encryption(Bucket=bucket)
    except ClientError as e:
        if "ServerSideEncryptionConfigurationNotFoundError" in str(e):
            return CheckResult.fail(
                "s3_bucket_encryption", f"No encryption configured on bucket '{bucket}'"
            )
        return CheckResult.fail("s3_bucket_encryption", str(e))

    if is_kms_encrypted(resp):
        return CheckResult.ok(
            "s3_bucket_encryption", f"KMS encryption: {get_kms_key_id(resp)}"
        )
    return CheckResult.fail(
        "s3_bucket_encryption", f"Bucket '{bucket}' does not use KMS encryption"
    )


def check_bucket_versioning(s3_uri: str) -> CheckResult:
    """Check that the bucket has versioning enabled."""
    bucket = _parse_bucket(s3_uri)
    try:
        resp = ClientFactory().s3().get_bucket_versioning(Bucket=bucket)
    except ClientError as e:
        return CheckResult.fail("s3_bucket_versioning", str(e))

    if is_versioning_enabled(resp):
        return CheckResult.ok(
            "s3_bucket_versioning", f"Versioning enabled on '{bucket}'"
        )
    return CheckResult.fail(
        "s3_bucket_versioning", f"Versioning not enabled on '{bucket}'"
    )


def check_path_empty(s3_uri: str) -> CheckResult:
    """Check that the S3 path has no existing objects."""
    bucket = _parse_bucket(s3_uri)
    prefix = _parse_prefix(s3_uri)
    try:
        resp = (
            ClientFactory()
            .s3()
            .list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        )
    except ClientError as e:
        return CheckResult.fail("s3_path_empty", str(e))

    if get_object_count(resp) == 0:
        return CheckResult.ok("s3_path_empty", f"Path '{s3_uri}' is empty")
    return CheckResult.fail(
        "s3_path_empty",
        f"Path '{s3_uri}' is not empty — leftover files may cause issues",
    )


# --- Athena checks ---


def check_athena_database(
    database: str, catalog: str = "AwsDataCatalog"
) -> CheckResult:
    """Check that the Athena database exists."""
    try:
        ClientFactory().athena().get_database(
            CatalogName=catalog, DatabaseName=database
        )
        return CheckResult.ok(
            "athena_database", f"Database '{database}' found in catalog '{catalog}'"
        )
    except ClientError as e:
        if is_entity_not_found(e):
            return CheckResult.fail(
                "athena_database",
                f"Database '{database}' not found in catalog '{catalog}'",
            )
        return CheckResult.fail("athena_database", str(e))


def check_athena_table(
    database: str, table: str, catalog: str = "AwsDataCatalog"
) -> CheckResult:
    """Check that the Athena table exists and return its columns."""
    try:
        resp = (
            ClientFactory()
            .athena()
            .get_table_metadata(
                CatalogName=catalog, DatabaseName=database, TableName=table
            )
        )
    except ClientError as e:
        if is_entity_not_found(e):
            return CheckResult.fail(
                "athena_table", f"Table '{table}' not found in '{database}'"
            )
        return CheckResult.fail("athena_table", str(e))

    col_names = get_table_columns(resp)
    return CheckResult.ok(
        "athena_table",
        f"Table '{table}' found with {len(col_names)} columns: {', '.join(col_names)}",
    )


def wrap_with_limit(query: str, limit: int) -> str:
    """Strip any existing LIMIT clause and append a new one."""
    stripped = re.sub(r"\s+LIMIT\s+\d+\s*$", "", query.rstrip(), flags=re.IGNORECASE)
    return f"{stripped} LIMIT {limit}"


def check_athena_query(
    sql_query: str, database: str, output_location: str, catalog: str = "AwsDataCatalog"
) -> CheckResult:
    """Validate SQL query by running with LIMIT 0 and checking required columns."""
    queries = [q.strip() for q in sql_query.split(";") if q.strip()]
    all_columns: list[list[str]] = []

    try:
        athena = ClientFactory().athena()

        for i, q in enumerate(queries):
            wrapped = wrap_with_limit(q, 0)

            exec_id = _execute_athena_query(
                athena, wrapped, output_location, catalog=catalog, database=database
            )

            asyncio.run(
                wait_until_all_complete(
                    [exec_id], TaskType.EXPORT_ATHENA_TABLE, athena, polling_interval=3
                )
            )

            resp = athena.get_query_execution(QueryExecutionId=exec_id)
            state = get_query_state(resp)
            if state != "SUCCEEDED":
                return CheckResult.fail(
                    "athena_query",
                    f"Query {i+1} failed: {get_query_failure_reason(resp)}",
                )

            results = athena.get_query_results(QueryExecutionId=exec_id, MaxResults=1)
            all_columns.append(get_query_result_columns(results))

        for i, columns in enumerate(all_columns):
            if "~id" not in columns:
                return CheckResult.fail(
                    "athena_query",
                    f"Query {i+1} missing required '~id' column. Got: {', '.join(columns)}",
                )

        combined = [c for cols in all_columns for c in cols]
        return CheckResult.ok(
            "athena_query",
            f"{len(queries)} query(ies) valid. Columns: {', '.join(set(combined))}",
        )
    except ClientError as e:
        return CheckResult.fail("athena_query", str(e))


# --- Neptune Analytics checks ---


def check_graph_name_available(graph_name: str) -> CheckResult:
    """Check that no existing graph uses this name."""
    try:
        resp = ClientFactory().neptune().list_graphs()
        for g in get_graph_names(resp):
            if g["name"] == graph_name:
                return CheckResult.fail(
                    "graph_name_available",
                    f"Graph '{g['name']}' ({g['id']}) already exists",
                )
        return CheckResult.ok(
            "graph_name_available", f"No existing graph named '{graph_name}'"
        )
    except ClientError as e:
        return CheckResult.fail("graph_name_available", str(e))


# --- Credentials check ---


def check_credentials() -> CheckResult:
    """Check that AWS credentials are valid."""
    try:
        resp = ClientFactory().sts().get_caller_identity()
        return CheckResult.ok("credentials", f"Resolved as {get_caller_arn(resp)}")
    except Exception as e:
        return CheckResult.fail("credentials", f"Cannot resolve credentials: {e}")


# --- Orchestrator ---


def validate_resources(
    s3_staging_bucket: Optional[str] = None,
    athena_output_bucket: Optional[str] = None,
    athena_catalog: str = "AwsDataCatalog",
    athena_database: Optional[str] = None,
    athena_table: Optional[str] = None,
    sql_query: Optional[str] = None,
    graph_name: Optional[str] = None,
    expected_region: Optional[str] = None,
) -> list[dict]:
    """Run all applicable validation checks and return results.

    Only validates resources that are provided — no false errors for unused features.
    """
    results: list[CheckResult] = []

    # Always check credentials first
    cred_check = check_credentials()
    results.append(cred_check)
    if not cred_check.passed:
        return [r.to_dict() for r in results]

    # S3 staging bucket
    if s3_staging_bucket:
        results.append(check_bucket_exists(s3_staging_bucket))
        if expected_region:
            results.append(check_bucket_region(s3_staging_bucket, expected_region))
        results.append(check_bucket_versioning(s3_staging_bucket))

    # Athena output bucket
    if athena_output_bucket:
        results.append(check_bucket_exists(athena_output_bucket))
        if expected_region:
            results.append(check_bucket_region(athena_output_bucket, expected_region))
        results.append(check_path_empty(athena_output_bucket))

    # Athena database/table
    if athena_database:
        results.append(check_athena_database(athena_database, athena_catalog))
        if athena_table:
            results.append(
                check_athena_table(athena_database, athena_table, athena_catalog)
            )

    # Athena query validation
    if sql_query and athena_database and s3_staging_bucket:
        results.append(
            check_athena_query(
                sql_query, athena_database, s3_staging_bucket, athena_catalog
            )
        )

    # Graph name
    if graph_name:
        results.append(check_graph_name_available(graph_name))

    return [r.to_dict() for r in results]
