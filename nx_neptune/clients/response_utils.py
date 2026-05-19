# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Response interpreters for boto3 API responses.

Pure functions that translate raw boto3 response dicts into typed answers.
No API calls — just dict parsing with purpose.
"""

from typing import Optional

from botocore.exceptions import ClientError

# --- Error classifiers ---


def get_error_code(e: ClientError) -> str:
    """Get HTTP error code from a ClientError."""
    return e.response["Error"]["Code"]


def is_not_found(e: ClientError) -> bool:
    """Is this a 404 / not-found error?"""
    return get_error_code(e) == "404"


def is_access_denied(e: ClientError) -> bool:
    """Is this a 403 / access-denied error?"""
    return get_error_code(e) == "403"


def is_entity_not_found(e: ClientError) -> bool:
    """Is this an Athena entity-not-found error?"""
    msg = str(e)
    return "EntityNotFoundException" in msg or "MetadataException" in msg


# --- S3 ---


def get_bucket_region(resp: dict) -> str:
    """Get bucket region from get_bucket_location response."""
    # S3 API returns None for us-east-1
    return resp.get("LocationConstraint") or "us-east-1"


def is_kms_encrypted(resp: dict) -> bool:
    """Check if get_bucket_encryption response uses KMS."""
    for rule in resp.get("ServerSideEncryptionConfiguration", {}).get("Rules", []):
        sse = rule.get("ApplyServerSideEncryptionByDefault", {})
        if sse.get("SSEAlgorithm") == "aws:kms":
            return True
    return False


def get_kms_key_id(resp: dict) -> Optional[str]:
    """Extract KMS key ID from get_bucket_encryption response."""
    for rule in resp.get("ServerSideEncryptionConfiguration", {}).get("Rules", []):
        sse = rule.get("ApplyServerSideEncryptionByDefault", {})
        if sse.get("SSEAlgorithm") == "aws:kms":
            return sse.get("KMSMasterKeyID")
    return None


def is_versioning_enabled(resp: dict) -> bool:
    """Check if get_bucket_versioning response shows enabled."""
    return resp.get("Status") == "Enabled"


def get_object_count(resp: dict) -> int:
    """Get object count from list_objects_v2 response."""
    return resp.get("KeyCount", 0)


# --- Athena ---


def get_table_columns(resp: dict) -> list[str]:
    """Extract column names from get_table_metadata response."""
    return [c["Name"] for c in resp.get("TableMetadata", {}).get("Columns", [])]


def get_query_state(resp: dict) -> str:
    """Get execution state from get_query_execution response."""
    return resp["QueryExecution"]["Status"]["State"]


def get_query_failure_reason(resp: dict) -> str:
    """Get failure reason from get_query_execution response."""
    return resp["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")


def get_query_result_columns(resp: dict) -> list[str]:
    """Extract column names from get_query_results response."""
    return [c["Name"] for c in resp["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]


# --- Neptune ---


def get_graph_names(resp: dict) -> list[dict]:
    """Get graph name/id pairs from list_graphs response."""
    return [{"name": g["name"], "id": g["id"]} for g in resp.get("graphs", [])]


# --- STS ---


def get_caller_arn(resp: dict) -> str:
    """Get caller ARN from get_caller_identity response."""
    return resp["Arn"]
