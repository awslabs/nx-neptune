# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Agent tools for projection drafting.

These are the *only* actions the agent can take. They are deliberately narrow:

  * ``get_schema`` — read-only Athena/Glue metadata (table + column names/types).
    Never returns row data, so no customer content enters the model context.
  * (write tool ``create_projection_draft`` lives here too — added in a later
    step — and only ever creates a ``draft`` projection; it does not execute the
    import pipeline or manage graph lifecycle.)
"""

from __future__ import annotations

from typing import Any

from nx_neptune.clients.client_factory import ClientFactory
from strands import tool

from nx_neptune_proxy.utils import paginate_aws
from nx_neptune_proxy.services.projection_service import projection_service
from nx_neptune_proxy.services.project_store import store as project_store

# Cap how much schema we pull into the model context. Metadata only (names +
# types), but still bounded for token budget and latency.
MAX_TABLES = 50
MAX_COLUMNS_PER_TABLE = 200
MAX_DATABASES = 200


@tool
def list_databases(catalog: str = "AwsDataCatalog") -> dict[str, Any]:
    """List the databases available in an Athena catalog.

    Use this first when the user has not told you which database to use, so you
    can show them the options and let them pick. Returns names only (metadata) —
    no tables or rows. Drill into a chosen database with get_schema afterward.

    Args:
        catalog: Athena data catalog name (defaults to the AWS Glue catalog).

    Returns:
        ``{"catalog": "...", "truncated": bool, "databases": ["db1", ...]}``
    """
    client = ClientFactory().athena()
    names = paginate_aws(
        client.list_databases, "DatabaseList", CatalogName=catalog
    )
    truncated = len(names) > MAX_DATABASES
    return {
        "catalog": catalog,
        "truncated": truncated,
        "databases": [db["Name"] for db in names[:MAX_DATABASES]],
    }


@tool
def get_schema(database: str, catalog: str = "AwsDataCatalog") -> dict[str, Any]:
    """Return the schema (tables and their columns) for an Athena database.

    Use this before proposing any SQL so the queries reference real tables and
    columns. Returns metadata only — table names, column names, and column
    types — never row data.

    Args:
        database: Athena database name to inspect.
        catalog: Athena data catalog name (defaults to the AWS Glue catalog).

    Returns:
        A dict of the form::

            {
              "catalog": "...",
              "database": "...",
              "truncated": bool,          # True if more tables exist than shown
              "tables": [
                {"name": "t1", "columns": [{"name": "c", "type": "string"}, ...]},
                ...
              ]
            }
    """
    client = ClientFactory().athena()

    table_meta = paginate_aws(
        client.list_table_metadata,
        "TableMetadataList",
        CatalogName=catalog,
        DatabaseName=database,
    )

    truncated = len(table_meta) > MAX_TABLES
    tables: list[dict[str, Any]] = []
    for t in table_meta[:MAX_TABLES]:
        # list_table_metadata already includes Columns, so we avoid a
        # per-table get_table_metadata round-trip.
        cols = (t.get("Columns") or [])[:MAX_COLUMNS_PER_TABLE]
        tables.append(
            {
                "name": t["Name"],
                "columns": [
                    {"name": c["Name"], "type": c.get("Type", "")} for c in cols
                ],
            }
        )

    return {
        "catalog": catalog,
        "database": database,
        "truncated": truncated,
        "tables": tables,
    }


@tool
def create_project(name: str) -> dict[str, Any]:
    """Create a project to hold projections, returning its real id.

    A projection must belong to a project. If the user has not given you an
    existing project id, call this to create one, then use the returned id when
    calling create_projection_draft. NEVER invent or guess a project_id — it
    must come from create_project or from the user.

    Args:
        name: A human-readable project name.

    Returns:
        ``{"id": "<uuid>", "name": "..."}``
    """
    p = project_store.create(name=name)
    return {"id": p.id, "name": p.name}


@tool
def create_projection_draft(
    database: str,
    node_query: str,
    edge_query: str,
    project_id: str | None = None,
    project_name: str | None = None,
    graph_name: str | None = None,
    catalog: str = "AwsDataCatalog",
    s3_staging_bucket: str | None = None,
    graph_memory_gb: int = 16,
) -> dict[str, Any]:
    """Create a DRAFT projection from proposed node/edge SQL.

    Use this only after the user has reviewed and approved the queries. The
    projection is created in ``draft`` status: this does NOT run the import
    pipeline, create a Neptune graph, or spend money. The user runs the import
    later from the UI.

    A projection must belong to a project. You do NOT need to create one first:
    if ``project_id`` is omitted (or refers to a project that no longer exists),
    this tool creates a project automatically and attaches the draft to it. Pass
    ``project_id`` only when the user gave you a real existing one; never invent
    a project_id.

    Query contract (the SQL must follow this so Neptune can build the graph):
      * node_query  -> must select ``~id`` and ``~label`` columns.
      * edge_query  -> must select ``~from``, ``~to``, and ``~label`` columns.

    Args:
        database: Athena database the queries run against.
        node_query: SQL producing node rows (``~id``, ``~label``).
        edge_query: SQL producing edge rows (``~from``, ``~to``, ``~label``).
        project_id: Existing project id, if the user supplied one. Otherwise
            leave unset and a project is created automatically.
        project_name: Name to use if a project is auto-created (defaults to a
            name derived from graph_name/database).
        graph_name: Optional Neptune graph name suffix (prefix added by the
            server). Must be 3-63 chars, starting alphanumeric.
        catalog: Athena data catalog (defaults to the AWS Glue catalog).
        s3_staging_bucket: Optional S3 path for staging Athena results.
        graph_memory_gb: Graph memory allocation in GB (default 16).

    Returns:
        A dict describing the created draft::

            {"id": "...", "project_id": "...", "status": "draft",
             "graph_name": "...", "database": "...", "catalog": "..."}
    """
    # Resolve the project: reuse a valid existing id, otherwise auto-create one.
    resolved_project_id = project_id
    if not resolved_project_id or project_store.get(resolved_project_id) is None:
        name = project_name or graph_name or f"{database}-projection"
        resolved_project_id = project_store.create(name=name).id

    projection_data: dict[str, Any] = {
        "catalog": catalog,
        "database": database,
        "graph_name": graph_name,
        "graph_memory_gb": graph_memory_gb,
        "s3_staging_bucket": s3_staging_bucket,
        "project_id": resolved_project_id,
    }

    projection = projection_service.create_with_queries(
        projection_data,
        node_sql=[node_query] if node_query else [],
        edge_sql=[edge_query] if edge_query else [],
    )

    return {
        "id": projection.id,
        "project_id": resolved_project_id,
        "status": projection.status,  # stays "draft" — creation never executes
        "graph_name": projection.graph_name,
        "database": projection.database,
        "catalog": projection.catalog,
    }
