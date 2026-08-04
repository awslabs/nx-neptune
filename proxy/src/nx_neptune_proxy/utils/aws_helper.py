# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Callable
from fastapi import HTTPException

def paginate_aws(method: Callable, result_key: str, **kwargs: Any) -> list:
    """Paginate an AWS API call that uses NextToken.

    Args:
        method: The boto3 client method to call (e.g., client.list_databases).
        result_key: The key in the response containing the list items.
        **kwargs: Arguments passed to the API call.

    Returns:
        Collected list of all items across all pages.
    """
    items = []
    while True:
        resp = method(**kwargs)
        items.extend(resp.get(result_key, []))
        if "NextToken" not in resp and "nextToken" not in resp:
            break
        kwargs["NextToken"] = resp.get("NextToken") or resp.get("nextToken")
    return items


def unpack_query_results(rows: list) -> dict:
    """Convert raw query rows (header + data) into {columns, rows} dict."""
    columns = rows[0] if rows else []
    data_rows = [[cell if cell is not None else "n/a" for cell in row] for row in rows[1:]] if len(rows) > 1 else []
    return {"columns": columns, "rows": data_rows}


def friendly_s3_error(e) -> str:
    """Extract a user-friendly message from a boto3 ClientError."""
    code = e.response["Error"].get("Code", "")
    message = e.response["Error"].get("Message", "")
    if code in ("AccessDenied", "403"):
        return "Permission denied — check IAM role has required S3 permissions"
    if code == "NoSuchBucket":
        return f"Bucket not found — {message}"
    if code == "NoSuchKey":
        return "File not found in S3"
    # If the message seems user-readable (short, no stack trace), use it
    if message and len(message) < 200:
        return message
    return f"S3 error: {code}"


def check_content_length(request, max_size: int) -> None:
    """Reject early if Content-Length header exceeds max_size."""
    content_length = request.headers.get("content-length")
    if content_length and int(content_length) > max_size:
        raise HTTPException(status_code=413, detail=f"Payload too large (max {max_size // (1024 * 1024)} MB)")


def check_body_size(contents: bytes, max_size: int) -> None:
    """Reject if body bytes exceed max_size."""
    if len(contents) > max_size:
        raise HTTPException(status_code=413, detail=f"Payload too large (max {max_size // (1024 * 1024)} MB)")
