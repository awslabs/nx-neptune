# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Callable

from botocore.exceptions import ClientError
from fastapi import HTTPException, Request

from nx_neptune_proxy.config import get_settings


def get_graph_or_exception(client, graph_id: str) -> dict:
    """Fetch a graph from Neptune Analytics, raising HTTP exceptions on failure.

    Returns the graph response dict on success.
    Raises HTTP 404 if the graph does not exist.
    """
    try:
        return client.get_graph(graphIdentifier=graph_id)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise HTTPException(status_code=404, detail="Graph not found")
        raise


def assert_managed_graph(graph_name: str | None) -> None:
    """Raise HTTP 403 if graph_name doesn't start with the configured prefix.

    Args:
        graph_name: The full (prefixed) graph name as it exists in Neptune.
                    If None or empty, the guard rejects the request.
    """
    prefix = get_settings().graph_prefix
    if not graph_name or not graph_name.startswith(prefix):
        raise HTTPException(
            status_code=403,
            detail="Refusing to operate on a graph not managed by this tool",
        )


def check_content_length(request: Request, max_size: int) -> None:
    """Reject early if Content-Length header exceeds max_size."""
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > max_size:
                raise HTTPException(
                    status_code=413,
                    detail=f"Payload too large (max {max_size // (1024 * 1024)} MB)",
                )
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid Content-Length header")


def require_name(value: str | None, max_length: int = 100) -> str:
    """Validate a required name. Returns stripped value or raises HTTP 400."""
    stripped = (value or "").strip()
    if not stripped:
        raise HTTPException(status_code=400, detail="Name is required")
    if len(stripped) > max_length:
        raise HTTPException(
            status_code=400, detail=f"Name too long (max {max_length} characters)"
        )
    return stripped


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
    data_rows = (
        [[cell if cell is not None else "n/a" for cell in row] for row in rows[1:]]
        if len(rows) > 1
        else []
    )
    return {"columns": columns, "rows": data_rows}


def check_body_size(contents: bytes, max_size: int) -> None:
    """Reject if body bytes exceed max_size."""
    if len(contents) > max_size:
        raise HTTPException(
            status_code=413,
            detail=f"Payload too large (max {max_size // (1024 * 1024)} MB)",
        )
