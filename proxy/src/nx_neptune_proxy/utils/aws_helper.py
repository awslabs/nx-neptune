# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Callable


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
    data_rows = rows[1:] if len(rows) > 1 else []
    return {"columns": columns, "rows": data_rows}
