# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Athena tool layer for the assistant agents (spec §9, Group C).

Plain functions the Schema Discovery agent calls to inspect a selected
database. ``list_tables`` / ``get_columns`` reuse Athena's metadata API (the
same calls behind ``/metadata/athena/{tables,columns}``) — no query, no scan
cost. ``sample_table`` runs a guarded ``SELECT ... LIMIT`` through the shared
Athena execution helper.

Per spec §9.11, these take **only data args** and resolve their Athena client
internally via ``agent_athena_client()`` — no credential parameters, so there is
no path to inject or leak credentials through the agent layer. That client is
scoped to the assumed read-only role when ``BEDROCK_AGENT_ROLE_ARN`` is set, and
falls back to the proxy's process role otherwise. They are kept free of any
Strands import so the package stays importable without ``strands-agents``;
Group D wraps them as ``@tool`` when building the agent.
"""

import asyncio
import re

from nx_neptune_proxy.assistant.agent_aws import agent_athena_client, agent_s3_client
from nx_neptune_proxy.config import get_settings
from nx_neptune_proxy.services.athena_query import execute_query_rows
from nx_neptune_proxy.utils import paginate_aws

# Server-side hard cap on sampled rows: a model-supplied ``limit`` is clamped to
# this regardless of what the model asks for (spec §9.10 guardrail).
MAX_SAMPLE_ROWS = 100
DEFAULT_SAMPLE_ROWS = 10


class AthenaToolError(Exception):
    """A precondition for an assistant Athena tool was not met."""


def list_buckets() -> list[str]:
    """Return the S3 bucket names in the configured region.

    Mirrors the import page's ``GET /metadata/s3/buckets`` endpoint: filters to
    ``settings.region`` and returns an empty list when no region is configured.
    Lets the assistant propose a real export/staging bucket for an import (the
    ``bucket`` field of a proposal) instead of asking the user to type one.
    Read-only, under the scoped agent role.
    """
    region = get_settings().region
    if not region:
        return []
    client = agent_s3_client()
    resp = client.list_buckets(BucketRegion=region)
    return [b["Name"] for b in resp.get("Buckets", [])]


def list_catalogs() -> list[dict]:
    """Return the Athena data catalogs as ``[{"name", "type"}]``.

    Uses the ``ListDataCatalogs`` metadata API — no query, no scan cost. The
    ``type`` (e.g. ``GLUE``, ``FEDERATED``, ``LAMBDA``) helps the discovery
    agent pick the right catalog before it enumerates databases within one.
    """
    client = agent_athena_client()
    items = paginate_aws(
        client.list_data_catalogs,
        "DataCatalogsSummary",
    )
    return [{"name": c["CatalogName"], "type": c.get("Type")} for c in items]


def list_databases(catalog: str) -> list[str]:
    """Return the database names in ``catalog`` (metadata API, no query cost)."""
    client = agent_athena_client()
    items = paginate_aws(
        client.list_databases,
        "DatabaseList",
        CatalogName=catalog,
    )
    return [d["Name"] for d in items]


def list_tables(catalog: str, database: str) -> list[str]:
    """Return the table names in ``database`` (metadata API, no query cost)."""
    client = agent_athena_client()
    items = paginate_aws(
        client.list_table_metadata,
        "TableMetadataList",
        CatalogName=catalog,
        DatabaseName=database,
    )
    return [t["Name"] for t in items]


def get_columns(catalog: str, database: str, table: str) -> list[dict]:
    """Return ``[{"name", "type"}]`` for ``table`` (metadata API, no query cost)."""
    client = agent_athena_client()
    resp = client.get_table_metadata(
        CatalogName=catalog, DatabaseName=database, TableName=table
    )
    columns = resp["TableMetadata"].get("Columns", [])
    return [{"name": c["Name"], "type": c["Type"]} for c in columns]


def list_tables_with_columns(
    catalog: str, database: str, tables: list[str]
) -> list[dict]:
    """Return name + columns for each requested table in one call.

    Given a set of tables the agent has already judged relevant, fetch their
    column definitions together — ``[{"name", "columns": [{"name", "type"}]}]``
    — instead of a separate ``get_columns`` round-trip per table. Metadata API
    only, no query/scan cost.

    Use this after narrowing to the relevant tables (via ``list_tables``); do
    NOT call it for every table in a database. A table name not present in the
    database is skipped rather than raising, so one bad guess does not fail the
    whole batch.
    """
    if not tables:
        return []
    client = agent_athena_client()
    known = set(list_tables(catalog, database))
    result: list[dict] = []
    for table in tables:
        if table not in known:
            continue
        resp = client.get_table_metadata(
            CatalogName=catalog, DatabaseName=database, TableName=table
        )
        columns = resp["TableMetadata"].get("Columns", [])
        result.append(
            {
                "name": table,
                "columns": [{"name": c["Name"], "type": c["Type"]} for c in columns],
            }
        )
    return result


def sample_table(
    catalog: str,
    database: str,
    table: str,
    limit: int = DEFAULT_SAMPLE_ROWS,
) -> dict:
    """Return up to ``limit`` sample rows from ``table`` as ``{columns, rows}``.

    Guarded (spec §9.10): ``limit`` is clamped to ``[1, MAX_SAMPLE_ROWS]``, and
    ``table`` is validated against the database's actual table list before it is
    interpolated into SQL — so a model cannot smuggle arbitrary SQL through the
    table name. The query runs against a server-resolved staging location.
    """
    limit = max(1, min(int(limit), MAX_SAMPLE_ROWS))

    known = list_tables(catalog, database)
    if table not in known:
        raise AthenaToolError(
            f"Table {table!r} not found in {database!r}; cannot sample."
        )

    output_location = _staging_location()
    quoted = _quote_ident(table)
    sql = f'SELECT * FROM {quoted}'

    return asyncio.run(
        execute_query_rows(
            agent_athena_client(),
            sql,
            output_location,
            catalog=catalog,
            database=database,
            limit=limit,
        )
    )


def _quote_ident(identifier: str) -> str:
    """Quote a validated Athena identifier, escaping embedded double quotes."""
    return '"' + identifier.replace('"', '""') + '"'


def _staging_location() -> str:
    """Resolve the S3 staging URI for sample queries from settings."""
    raw = get_settings().config_bucket
    if not raw:
        raise AthenaToolError(
            "No Athena staging location configured (set NX_NEPTUNE_CONFIG_BUCKET)."
        )
    base = raw if raw.startswith("s3://") else f"s3://{raw}"
    return re.sub(r"/+$", "", base) + "/assistant-samples"
