# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Schema preview: discover node/edge labels via Athena SELECT DISTINCT."""

from __future__ import annotations

from nx_neptune.clients.client_factory import ClientFactory
from nx_neptune.clients.response_utils import get_query_failure_reason, get_query_state
from nx_neptune.instance_management import _execute_athena_query, get_athena_query_results
from nx_neptune.utils.task_future import TaskType, wait_until_all_complete


async def discover_labels(
    sql: str,
    catalog: str,
    database: str,
    s3_staging_bucket: str,
    limit: int = 50,
) -> list[str]:
    """Run SELECT DISTINCT ~label via Athena to discover label values.

    Args:
        sql: The user's node or edge query (must produce a ~label column).
        catalog: Athena catalog name.
        database: Athena database name.
        s3_staging_bucket: S3 output location for Athena results.
        limit: Max distinct labels to return (safety cap).

    Returns:
        List of distinct label strings.

    Raises:
        RuntimeError: If the Athena query fails.
    """
    client = ClientFactory().athena()
    wrapped = f'SELECT DISTINCT "~label" FROM ({sql}) AS _sub LIMIT {limit}'

    exec_id = _execute_athena_query(
        client, wrapped, s3_staging_bucket, catalog=catalog, database=database
    )
    await wait_until_all_complete([exec_id], TaskType.EXPORT_ATHENA_TABLE, client, polling_interval=3)

    resp = client.get_query_execution(QueryExecutionId=exec_id)
    state = get_query_state(resp)
    if state != "SUCCEEDED":
        reason = get_query_failure_reason(resp)
        raise RuntimeError(f"Athena query failed: {reason}")

    rows = get_athena_query_results(query_execution_id=exec_id, client=client)
    # First row is header, rest are data
    if len(rows) <= 1:
        return []
    return [row[0] for row in rows[1:] if row and row[0]]
