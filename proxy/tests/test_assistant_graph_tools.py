# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Live graph-schema reader (spec §9): parses get_graph_summary(DETAILED) into a
GraphSchema, defensively (missing keys → empty lists)."""

from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from nx_neptune_proxy.assistant import graph_tools


def _with_client(resp):
    client = MagicMock()
    client.get_graph_summary.return_value = resp
    return patch.object(graph_tools, "agent_neptune_client", return_value=client), client


def test_fetch_graph_schema_parses_labels_and_property_union():
    resp = {
        "graphSummary": {
            "nodeLabels": ["Person", "Company"],
            "edgeLabels": ["WORKS_AT"],
            "nodeStructures": [
                {"nodeProperties": ["name", "age"]},
                {"nodeProperties": ["age", "revenue"]},  # dup 'age' collapses
            ],
            "edgeStructures": [{"edgeProperties": ["since"]}],
        }
    }
    ctx, client = _with_client(resp)
    with ctx:
        schema = graph_tools.fetch_graph_schema("g-1")

    client.get_graph_summary.assert_called_once_with(
        graphIdentifier="g-1", mode="DETAILED"
    )
    assert schema.node_labels == ["Person", "Company"]
    assert schema.edge_labels == ["WORKS_AT"]
    assert schema.node_properties == ["name", "age", "revenue"]
    assert schema.edge_properties == ["since"]
    assert not schema.is_empty


def test_fetch_graph_schema_defensive_on_sparse_summary():
    ctx, _ = _with_client({"graphSummary": {}})
    with ctx:
        schema = graph_tools.fetch_graph_schema("g-2")
    assert schema.node_labels == [] and schema.node_properties == []
    assert schema.is_empty


def test_fetch_graph_schema_handles_dict_property_entries():
    # Some summary shapes return properties as {"name": ...} dicts.
    resp = {
        "graphSummary": {
            "nodeLabels": ["Person"],
            "nodeStructures": [{"nodeProperties": [{"name": "name"}, {"name": "age"}]}],
        }
    }
    ctx, _ = _with_client(resp)
    with ctx:
        schema = graph_tools.fetch_graph_schema("g-3")
    assert schema.node_properties == ["name", "age"]


# --- validate_opencypher (EXPLAIN syntax check) ---


def test_validate_opencypher_prefixes_explain_and_reports_valid():
    client = MagicMock()
    client.execute_query.return_value = {"payload": None}
    with patch.object(graph_tools, "agent_neptune_client", return_value=client):
        valid, err = graph_tools.validate_opencypher("g-1", "MATCH (n) RETURN n")

    assert valid is True and err is None
    kwargs = client.execute_query.call_args.kwargs
    assert kwargs["graphIdentifier"] == "g-1"
    assert kwargs["queryString"] == "EXPLAIN MATCH (n) RETURN n"
    assert kwargs["language"] == "OPEN_CYPHER"


def test_validate_opencypher_does_not_double_prefix():
    client = MagicMock()
    with patch.object(graph_tools, "agent_neptune_client", return_value=client):
        graph_tools.validate_opencypher("g-1", "  explain MATCH (n) RETURN n  ")
    assert client.execute_query.call_args.kwargs["queryString"] == "explain MATCH (n) RETURN n"


def test_validate_opencypher_reports_engine_error_as_invalid():
    client = MagicMock()
    client.execute_query.side_effect = ClientError(
        {"Error": {"Code": "MalformedQueryException", "Message": "bad cypher"}},
        "ExecuteQuery",
    )
    with patch.object(graph_tools, "agent_neptune_client", return_value=client):
        valid, err = graph_tools.validate_opencypher("g-1", "BROKEN")

    assert valid is False
    assert err
