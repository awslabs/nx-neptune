# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Response interpreters for boto3 API responses.

Pure functions that translate raw boto3 response dicts into typed answers.
No API calls — just dict parsing with purpose.

Naming convention: {product}_{verb}_{noun}
  - s3_*      — S3 responses
  - athena_*  — Athena responses
  - nx_*      — Neptune Analytics responses
  - sts_*     — STS responses
  - boto_*    — Generic boto3 responses / error classifiers
"""

from typing import Optional

from botocore.exceptions import ClientError

# --- Error classifiers ---


def boto_is_not_found(e: ClientError) -> bool:
    """Is this a 404 / not-found error?"""
    return e.response["Error"]["Code"] == "404"


def boto_is_access_denied(e: ClientError) -> bool:
    """Is this a 403 / access-denied error?"""
    return e.response["Error"]["Code"] == "403"


def athena_is_entity_not_found(e: ClientError) -> bool:
    """Is this an Athena entity-not-found error?"""
    msg = str(e)
    return "EntityNotFoundException" in msg or "MetadataException" in msg


# --- S3 ---


def s3_get_bucket_region(resp: dict) -> str:
    """Get bucket region from get_bucket_location response."""
    # S3 API returns None for us-east-1
    return resp.get("LocationConstraint") or "us-east-1"


def s3_is_kms_encrypted(resp: dict) -> bool:
    """Check if get_bucket_encryption response uses KMS."""
    for rule in resp.get("ServerSideEncryptionConfiguration", {}).get("Rules", []):
        sse = rule.get("ApplyServerSideEncryptionByDefault", {})
        if sse.get("SSEAlgorithm") == "aws:kms":
            return True
    return False


def s3_get_kms_key_id(resp: dict) -> Optional[str]:
    """Extract KMS key ID from get_bucket_encryption response."""
    for rule in resp.get("ServerSideEncryptionConfiguration", {}).get("Rules", []):
        sse = rule.get("ApplyServerSideEncryptionByDefault", {})
        if sse.get("SSEAlgorithm") == "aws:kms":
            return sse.get("KMSMasterKeyID")
    return None


def s3_is_versioning_enabled(resp: dict) -> bool:
    """Check if get_bucket_versioning response shows enabled."""
    return resp.get("Status") == "Enabled"


def s3_get_object_count(resp: dict) -> int:
    """Get object count from list_objects_v2 response."""
    return resp.get("KeyCount", 0)


def s3_get_contents(resp: dict) -> list[dict]:
    """Get Contents list from list_objects_v2 response."""
    return resp.get("Contents", [])


# --- Athena ---


def athena_get_table_columns(resp: dict) -> list[str]:
    """Extract column names from get_table_metadata response."""
    return [c["Name"] for c in resp.get("TableMetadata", {}).get("Columns", [])]


def athena_get_query_state(resp: dict) -> str:
    """Get execution state from get_query_execution response."""
    return resp["QueryExecution"]["Status"]["State"]


def athena_get_query_execution_id(resp: dict) -> str:
    """Get QueryExecutionId from start_query_execution response."""
    return resp["QueryExecutionId"]


def athena_get_query_failure_reason(resp: dict) -> str:
    """Get failure reason from get_query_execution response."""
    return resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")


def athena_get_query_result_columns(resp: dict) -> list[str]:
    """Extract column names from get_query_results response."""
    return [c["Name"] for c in resp["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]


# --- Neptune ---


def na_get_graph_names(resp: dict) -> list[dict]:
    """Get graph name/id pairs from list_graphs response."""
    return [{"name": g["name"], "id": g["id"]} for g in resp.get("graphs", [])]


def na_get_graph_id(resp: dict) -> str:
    """Get graph ID from create/get graph response."""
    return resp.get("graphId") or resp.get("id")  # type: ignore[return-value]


def na_get_task_id(resp: dict) -> str:
    """Get task ID from import/export task response."""
    return resp.get("taskId")  # type: ignore[return-value]


def na_get_snapshot_id(resp: dict) -> str:
    """Get snapshot ID from create_graph_snapshot response."""
    return resp["id"]


# --- Common ---


def boto_get_status_code(resp: dict) -> Optional[int]:
    """Get HTTP status code from any boto3 response."""
    return (resp.get("ResponseMetadata") or {}).get("HTTPStatusCode")


# --- STS ---


def sts_get_caller_arn(resp: dict) -> str:
    """Get caller ARN from get_caller_identity response."""
    return resp["Arn"]
