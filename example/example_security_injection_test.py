# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""
Security test demonstrating that ParameterMapBuilder and parameterized queries
in NeptuneAnalyticsClient mitigate OpenCypher injection attacks.

User-supplied values are never interpolated into query strings — they are passed
separately via the parameter_map, ensuring malicious input is treated as data.
"""
import json
from unittest.mock import MagicMock

from nx_neptune.clients.opencypher_builder import (
    ParameterMapBuilder,
    insert_node,
    insert_edge,
    delete_node,
    update_node,
    bfs_query,
)
from nx_neptune.clients.na_models import Node, Edge
from nx_neptune.clients.na_client import NeptuneAnalyticsClient


# --- Injection payloads ---
INJECTION_PAYLOADS = [
    "' OR 1=1 --",
    "Alice' DETACH DELETE n //",
    "}) MATCH (n) DETACH DELETE n //",
    "Alice' RETURN n UNION MATCH (m) DETACH DELETE m //",
    "$0}) MATCH (x) SET x.hacked=true //",
]


def test_parameter_map_builder_isolates_values():
    """Verify ParameterMapBuilder never embeds raw values into placeholders."""
    print("=== Test: ParameterMapBuilder isolates values ===")

    for payload in INJECTION_PAYLOADS:
        builder = ParameterMapBuilder()
        masked = builder.read_map({"name": payload})
        params = builder.get_param_values()

        assert masked["name"] == "$0", f"Expected $0, got {masked['name']}"
        assert params["0"] == payload, "Payload must be stored in param values"
        assert payload not in str(masked), "Payload must not appear in masked map"
        print(f"  PASS: payload '{payload[:40]}...' safely parameterized")

    print()


def test_insert_node_injection():
    """Verify insert_node keeps injection payloads out of the query string."""
    print("=== Test: insert_node resists injection ===")

    for payload in INJECTION_PAYLOADS:
        node = Node(id=payload, labels=["Person"], properties={"name": payload})
        query, params = insert_node(node)

        assert payload not in query, f"Injection payload found in query: {query}"
        assert payload in params.values(), "Payload must be in parameter map"
        print(f"  PASS: node id='{payload[:40]}...' not in query string")

    print()


def test_insert_edge_injection():
    """Verify insert_edge keeps injection payloads out of the query string."""
    print("=== Test: insert_edge resists injection ===")

    for payload in INJECTION_PAYLOADS:
        src = Node(id=payload, labels=["Person"], properties={})
        dest = Node(id="Bob", labels=["Person"], properties={})
        edge = Edge(
            label="KNOWS",
            properties={"since": payload},
            node_src=src,
            node_dest=dest,
        )
        query, params = insert_edge(edge)

        assert payload not in query, f"Injection payload found in query: {query}"
        assert payload in params.values()
        print(f"  PASS: edge with payload '{payload[:40]}...' safely parameterized")

    print()


def test_delete_node_injection():
    """Verify delete_node keeps injection payloads out of the query string."""
    print("=== Test: delete_node resists injection ===")

    for payload in INJECTION_PAYLOADS:
        node = Node(id=payload, labels=["Person"], properties={})
        query, params = delete_node(node)

        assert payload not in query, f"Injection payload found in query: {query}"
        assert payload in params.values()
        print(f"  PASS: delete with payload '{payload[:40]}...' safely parameterized")

    print()


def test_update_node_injection():
    """Verify update_node keeps injection payloads out of the query string."""
    print("=== Test: update_node resists injection ===")

    for payload in INJECTION_PAYLOADS:
        query, params = update_node("Person", "a", [payload], {"a.name": payload})

        assert payload not in query, f"Injection payload found in query: {query}"
        assert payload in params.values()
        print(f"  PASS: update with payload '{payload[:40]}...' safely parameterized")

    print()


def test_bfs_query_injection():
    """Verify bfs_query keeps injection payloads out of the query string."""
    print("=== Test: bfs_query resists injection ===")

    for payload in INJECTION_PAYLOADS:
        query, params = bfs_query("n", {"id(n)": payload})

        assert payload not in query, f"Injection payload found in query: {query}"
        assert payload in params.values()
        print(f"  PASS: bfs with payload '{payload[:40]}...' safely parameterized")

    print()


def test_execute_generic_query_passes_params_separately():
    """Verify NeptuneAnalyticsClient sends parameters separately from the query string."""
    print("=== Test: execute_generic_query passes params via parameter_map ===")

    mock_client = MagicMock()
    mock_payload = MagicMock()
    mock_payload.read.return_value = json.dumps({"results": []})
    mock_client.execute_query.return_value = {"payload": mock_payload}

    na_client = NeptuneAnalyticsClient(graph_id="g-test123", client=mock_client)

    payload = "' OR 1=1 --"
    query = "MATCH (n) WHERE id(n) = $0 RETURN n"
    param_map = {"0": payload}

    na_client.execute_generic_query(query, parameter_map=param_map)

    call_kwargs = mock_client.execute_query.call_args[1]
    assert call_kwargs["queryString"] == query, "Query string must not contain payload"
    assert call_kwargs["parameters"] == param_map, "Parameters must be passed separately"
    assert payload not in call_kwargs["queryString"], "Payload must not be in query string"
    print(f"  PASS: payload '{payload}' passed via parameters, not in queryString")

    print()


def main():
    test_parameter_map_builder_isolates_values()
    test_insert_node_injection()
    test_insert_edge_injection()
    test_delete_node_injection()
    test_update_node_injection()
    test_bfs_query_injection()
    test_execute_generic_query_passes_params_separately()
    print("=" * 50)
    print("All security tests passed.")


if __name__ == "__main__":
    main()
