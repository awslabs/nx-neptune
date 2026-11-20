# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared Athena query-execution helper.

Extracted from ``routers/projection.py::preview_projection`` so the projection
preview endpoint and the assistant's ``sample_table`` tool (spec §9, Group C)
share one execution path: LIMIT-wrap → start query → wait → check state →
fetch + unpack rows.
"""

from nx_neptune.clients.response_utils import get_query_failure_reason, get_query_state
from nx_neptune.instance_management import (
    _execute_athena_query,
    get_athena_query_results,
)
from nx_neptune.utils.task_future import TaskType, wait_until_all_complete
from nx_neptune.validators import wrap_with_limit

from nx_neptune_proxy.utils import unpack_query_results


class AthenaQueryError(Exception):
    """An Athena query finished in a non-SUCCEEDED state.

    Carries the service-reported failure ``reason`` so callers can surface it.
    """

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


async def execute_query_rows(
    client,
    sql: str,
    output_location: str,
    *,
    catalog: str,
    database: str,
    limit: int = None,
) -> dict:
    """Run a single Athena query and return ``{columns, rows}``.

    Args:
        client: an Athena client (from ``ClientFactory().athena()``).
        sql: the query to run.
        output_location: S3 URI for Athena results (e.g. ``s3://bucket/prefix``).
        catalog: Athena data catalog name.
        database: database name the query runs against.
        limit: if given, replaces/appends a ``LIMIT`` clause via
            :func:`wrap_with_limit`; if ``None``, the query runs unmodified.

    Raises:
        AthenaQueryError: if the query does not reach ``SUCCEEDED``.
    """
    statement = wrap_with_limit(sql, limit) if limit is not None else sql

    exec_id = _execute_athena_query(
        client, statement, output_location, catalog=catalog, database=database
    )

    await wait_until_all_complete(
        [exec_id], TaskType.EXPORT_ATHENA_TABLE, client, polling_interval=5
    )

    resp = client.get_query_execution(QueryExecutionId=exec_id)
    state = get_query_state(resp)
    if state != "SUCCEEDED":
        raise AthenaQueryError(get_query_failure_reason(resp))

    rows = get_athena_query_results(query_execution_id=exec_id, client=client)
    return unpack_query_results(rows)
