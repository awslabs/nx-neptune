# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the create_projection_draft write tool.

The tool must create a projection in ``draft`` status and MUST NOT execute the
import pipeline or create a graph.
"""

from nx_neptune_proxy.agent.tools import create_projection_draft
from nx_neptune_proxy.services.projection_service import projection_service


def _call(**kwargs):
    fn = getattr(create_projection_draft, "__wrapped__", create_projection_draft)
    return fn(**kwargs)


def _make_project() -> str:
    from nx_neptune_proxy.services.project_store import store as project_store

    return project_store.create(name="agent-test").id


def test_creates_draft_projection_with_queries():
    project_id = _make_project()
    result = _call(
        project_id=project_id,
        database="db1",
        node_query='SELECT id AS "~id", \'Account\' AS "~label" FROM accounts',
        edge_query='SELECT src AS "~from", dst AS "~to", \'PAYS\' AS "~label" FROM tx',
        graph_name="nxp-demo",
        catalog="AwsDataCatalog",
    )

    # Status is draft — creation never executes the pipeline.
    assert result["status"] == "draft"
    assert result["database"] == "db1"

    # The projection and its queries were persisted.
    stored = projection_service.get_or_raise(result["id"])
    assert stored.status == "draft"
    assert stored.database == "db1"
    node_sql, edge_sql = projection_service.get_query_sql_lists(result["id"])
    assert any("~id" in q for q in node_sql)
    assert any("~from" in q for q in edge_sql)


def test_draft_has_no_graph_and_no_progress():
    project_id = _make_project()
    result = _call(
        project_id=project_id,
        database="db2",
        node_query='SELECT a AS "~id", \'N\' AS "~label" FROM t',
        edge_query='SELECT a AS "~from", b AS "~to", \'E\' AS "~label" FROM e',
    )
    stored = projection_service.get_or_raise(result["id"])
    # A draft has not been executed: no graph, no progress.
    assert stored.graph_id is None
    assert stored.graph_endpoint is None
    assert stored.progress == 0


def test_auto_creates_project_when_id_omitted():
    """No project_id -> the tool creates a project and attaches the draft."""
    from nx_neptune_proxy.services.project_store import store as project_store

    result = _call(
        database="db3",
        node_query='SELECT a AS "~id", \'N\' AS "~label" FROM t',
        edge_query='SELECT a AS "~from", b AS "~to", \'E\' AS "~label" FROM e',
        project_name="auto-made",
    )
    assert result["status"] == "draft"
    # A real project was created and returned.
    pid = result["project_id"]
    assert project_store.get(pid) is not None
    assert project_store.get(pid).name == "auto-made"


def test_invalid_project_id_is_replaced_not_fk_error():
    """A bogus project_id (e.g. an LLM-fabricated slug) must NOT cause a foreign
    key error — the tool creates a real project instead."""
    from nx_neptune_proxy.services.project_store import store as project_store

    result = _call(
        project_id="mitre-attack-fraud-malware",  # not a real project row
        database="db4",
        node_query='SELECT a AS "~id", \'N\' AS "~label" FROM t',
        edge_query='SELECT a AS "~from", b AS "~to", \'E\' AS "~label" FROM e',
    )
    assert result["status"] == "draft"
    # The bogus id was discarded; a real, existing project backs the draft.
    assert result["project_id"] != "mitre-attack-fraud-malware"
    assert project_store.get(result["project_id"]) is not None
