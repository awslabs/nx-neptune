# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Resource validation for nx-neptune migrations.

Provides upfront checks for S3, Athena, and Neptune Analytics resources
so configuration errors surface immediately — not minutes into a pipeline.
"""

import logging
from dataclasses import dataclass
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    check: str
    passed: bool
    message: str

    def to_dict(self) -> dict:
        return {"check": self.check, "passed": self.passed, "message": self.message}


def _parse_bucket(s3_uri: str) -> str:
    """Extract bucket name from s3://bucket/path/."""
    return s3_uri.replace("s3://", "").split("/")[0]


# --- S3 checks ---


def check_bucket_exists(s3_uri: str, region: str) -> CheckResult:
    """Check that the S3 bucket exists and is accessible."""
    bucket = _parse_bucket(s3_uri)
    try:
        s3 = boto3.client("s3", region_name=region)
        s3.head_bucket(Bucket=bucket)
        return CheckResult("s3_bucket_exists", True, f"Bucket '{bucket}' exists")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            return CheckResult("s3_bucket_exists", False, f"Bucket '{bucket}' not found")
        if code == "403":
            return CheckResult("s3_bucket_exists", False, f"Access denied to bucket '{bucket}'")
        return CheckResult("s3_bucket_exists", False, str(e))


def check_bucket_region(s3_uri: str, expected_region: str) -> CheckResult:
    """Check that the bucket is in the expected region."""
    bucket = _parse_bucket(s3_uri)
    try:
        s3 = boto3.client("s3")
        resp = s3.get_bucket_location(Bucket=bucket)
        location = resp.get("LocationConstraint") or "us-east-1"
        if location == expected_region:
            return CheckResult("s3_bucket_region", True, f"Bucket '{bucket}' is in {expected_region}")
        return CheckResult(
            "s3_bucket_region", False,
            f"Bucket '{bucket}' is in {location}, expected {expected_region}",
        )
    except ClientError as e:
        return CheckResult("s3_bucket_region", False, str(e))


def check_bucket_encryption(s3_uri: str, region: str) -> CheckResult:
    """Check that the bucket has KMS encryption configured."""
    bucket = _parse_bucket(s3_uri)
    try:
        s3 = boto3.client("s3", region_name=region)
        resp = s3.get_bucket_encryption(Bucket=bucket)
        rules = resp.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
        for rule in rules:
            sse = rule.get("ApplyServerSideEncryptionByDefault", {})
            if sse.get("SSEAlgorithm") == "aws:kms":
                key = sse.get("KMSMasterKeyID", "AWS managed key")
                return CheckResult("s3_bucket_encryption", True, f"KMS encryption: {key}")
        return CheckResult(
            "s3_bucket_encryption", False,
            f"Bucket '{bucket}' does not use KMS encryption. "
            f"Fix: aws s3api put-bucket-encryption --bucket {bucket} "
            "--server-side-encryption-configuration "
            "'{\"Rules\":[{\"ApplyServerSideEncryptionByDefault\":{\"SSEAlgorithm\":\"aws:kms\"}}]}'",
        )
    except ClientError as e:
        if "ServerSideEncryptionConfigurationNotFoundError" in str(e):
            return CheckResult(
                "s3_bucket_encryption", False,
                f"No encryption configured on bucket '{bucket}'",
            )
        return CheckResult("s3_bucket_encryption", False, str(e))


def check_bucket_versioning(s3_uri: str, region: str) -> CheckResult:
    """Check that the bucket has versioning enabled."""
    bucket = _parse_bucket(s3_uri)
    try:
        s3 = boto3.client("s3", region_name=region)
        resp = s3.get_bucket_versioning(Bucket=bucket)
        status = resp.get("Status", "Disabled")
        if status == "Enabled":
            return CheckResult("s3_bucket_versioning", True, f"Versioning enabled on '{bucket}'")
        return CheckResult(
            "s3_bucket_versioning", False,
            f"Versioning not enabled on '{bucket}'. "
            f"Fix: aws s3api put-bucket-versioning --bucket {bucket} "
            "--versioning-configuration Status=Enabled",
        )
    except ClientError as e:
        return CheckResult("s3_bucket_versioning", False, str(e))


def check_path_empty(s3_uri: str, region: str) -> CheckResult:
    """Check that the S3 path has no existing objects."""
    bucket = _parse_bucket(s3_uri)
    prefix = s3_uri.replace("s3://", "").split("/", 1)[1] if "/" in s3_uri.replace("s3://", "") else ""
    try:
        s3 = boto3.client("s3", region_name=region)
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        count = resp.get("KeyCount", 0)
        if count == 0:
            return CheckResult("s3_path_empty", True, f"Path '{s3_uri}' is empty")
        return CheckResult(
            "s3_path_empty", False,
            f"Path '{s3_uri}' is not empty — leftover files may cause issues",
        )
    except ClientError as e:
        return CheckResult("s3_path_empty", False, str(e))


# --- Athena checks ---


def check_athena_database(catalog: str, database: str, region: str) -> CheckResult:
    """Check that the Athena database exists."""
    try:
        athena = boto3.client("athena", region_name=region)
        athena.get_database(CatalogName=catalog, DatabaseName=database)
        return CheckResult("athena_database", True, f"Database '{database}' found in catalog '{catalog}'")
    except ClientError as e:
        if "EntityNotFoundException" in str(e) or "MetadataException" in str(e):
            return CheckResult("athena_database", False, f"Database '{database}' not found in catalog '{catalog}'")
        return CheckResult("athena_database", False, str(e))


def check_athena_table(catalog: str, database: str, table: str, region: str) -> CheckResult:
    """Check that the Athena table exists and return its columns."""
    try:
        athena = boto3.client("athena", region_name=region)
        resp = athena.get_table_metadata(
            CatalogName=catalog, DatabaseName=database, TableName=table
        )
        columns = resp["TableMetadata"].get("Columns", [])
        col_names = [c["Name"] for c in columns]
        return CheckResult(
            "athena_table", True,
            f"Table '{table}' found with {len(columns)} columns: {', '.join(col_names)}",
        )
    except ClientError as e:
        if "EntityNotFoundException" in str(e) or "MetadataException" in str(e):
            return CheckResult("athena_table", False, f"Table '{table}' not found in '{database}'")
        return CheckResult("athena_table", False, str(e))


def check_athena_query(
    sql_query: str, catalog: str, database: str, output_location: str, region: str
) -> CheckResult:
    """Validate SQL query by running with LIMIT 0 and checking required columns."""
    import time

    try:
        import re

        athena = boto3.client("athena", region_name=region)
        # Append LIMIT 0 to validate without scanning data
        stripped = re.sub(r'\s+LIMIT\s+\d+\s*$', '', sql_query.rstrip().rstrip(';'), flags=re.IGNORECASE)
        wrapped = f"{stripped} LIMIT 0"

        exec_id = athena.start_query_execution(
            QueryString=wrapped,
            QueryExecutionContext={"Catalog": catalog, "Database": database},
            ResultConfiguration={"OutputLocation": output_location},
        )["QueryExecutionId"]

        while True:
            resp = athena.get_query_execution(QueryExecutionId=exec_id)
            state = resp["QueryExecution"]["Status"]["State"]
            if state == "SUCCEEDED":
                results = athena.get_query_results(QueryExecutionId=exec_id, MaxResults=1)
                columns = [c["Name"] for c in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
                if "~id" not in columns:
                    return CheckResult(
                        "athena_query", False,
                        f"Query missing required '~id' column. Got: {', '.join(columns)}",
                    )
                return CheckResult("athena_query", True, f"Query valid. Columns: {', '.join(columns)}")
            if state in ("FAILED", "CANCELLED"):
                reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                return CheckResult("athena_query", False, f"Query failed: {reason}")
            time.sleep(1)
    except ClientError as e:
        return CheckResult("athena_query", False, str(e))


# --- Neptune Analytics checks ---


def check_graph_name_available(graph_name: str, region: str) -> CheckResult:
    """Check that no existing graph uses this name prefix."""
    try:
        client = boto3.client("neptune-graph", region_name=region)
        resp = client.list_graphs()
        for g in resp.get("graphs", []):
            if g["name"].startswith(graph_name):
                return CheckResult(
                    "graph_name_available", False,
                    f"Graph '{g['name']}' ({g['id']}) already exists with this prefix",
                )
        return CheckResult("graph_name_available", True, f"No existing graph named '{graph_name}'")
    except ClientError as e:
        return CheckResult("graph_name_available", False, str(e))


# --- Credentials check ---


def check_credentials(region: str) -> CheckResult:
    """Check that AWS credentials are valid."""
    try:
        sts = boto3.client("sts", region_name=region)
        identity = sts.get_caller_identity()
        arn = identity["Arn"]
        return CheckResult("credentials", True, f"Resolved as {arn}")
    except Exception as e:
        return CheckResult("credentials", False, f"Cannot resolve credentials: {e}")


# --- Orchestrator ---


def validate_resources(
    region: str,
    s3_staging_bucket: Optional[str] = None,
    athena_output_bucket: Optional[str] = None,
    athena_catalog: str = "AwsDataCatalog",
    athena_database: Optional[str] = None,
    athena_table: Optional[str] = None,
    sql_query: Optional[str] = None,
    graph_name: Optional[str] = None,
) -> list[dict]:
    """Run all applicable validation checks and return results.

    Only validates resources that are provided — no false errors for unused features.
    """
    results = []

    # Always check credentials first
    cred_check = check_credentials(region)
    results.append(cred_check)
    if not cred_check.passed:
        return [r.to_dict() for r in results]

    # S3 staging bucket
    if s3_staging_bucket:
        results.append(check_bucket_exists(s3_staging_bucket, region))
        results.append(check_bucket_region(s3_staging_bucket, region))
        results.append(check_bucket_versioning(s3_staging_bucket, region))

    # Athena output bucket
    if athena_output_bucket:
        results.append(check_bucket_exists(athena_output_bucket, region))
        results.append(check_bucket_region(athena_output_bucket, region))
        results.append(check_path_empty(athena_output_bucket, region))

    # Athena database/table
    if athena_database:
        results.append(check_athena_database(athena_catalog, athena_database, region))
        if athena_table:
            results.append(check_athena_table(athena_catalog, athena_database, athena_table, region))

    # Athena query validation
    if sql_query and athena_database and s3_staging_bucket:
        results.append(check_athena_query(sql_query, athena_catalog, athena_database, s3_staging_bucket, region))

    return [r.to_dict() for r in results]
