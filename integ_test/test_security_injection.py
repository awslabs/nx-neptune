# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
Integration test proving that parameterized queries prevent OpenCypher injection
against a real Neptune Analytics instance.

The test inserts nodes and edges with malicious payloads as IDs and properties,
then verifies:
  1. The injection payloads are stored as literal data, not executed
  2. A canary node survives all injection attempts (graph not destroyed)
  3. The injected data can be queried back exactly as inserted

Usage:
    export NETWORKX_GRAPH_ID=g-your-graph-id
    pytest integ_test/test_security_injection.py -v
"""
import pytest
import networkx as nx

from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID
from nx_neptune.clients import Node, Edge

INJECTION_PAYLOADS = [
    "' OR 1=1 --",
    "Alice' DETACH DELETE n //",
    "}) MATCH (n) DETACH DELETE n //",
    "Alice' RETURN n UNION MATCH (m) DETACH DELETE m //",
    "$0}) MATCH (x) SET x.hacked=true //",
]

CANARY_NODE_ID = "canary_security_test"


@pytest.fixture(scope="module")
def na_graph():
    if not NETWORKX_GRAPH_ID:
        pytest.skip('Environment Variable "NETWORKX_GRAPH_ID" is not defined')

    g = nx.Graph()
    graph = NeptuneGraph.from_config(graph=g)
    graph.clear_graph()

    # Insert a canary node that must survive all injection attempts
    canary = Node(id=CANARY_NODE_ID, labels=["Canary"], properties={"purpose": "injection_test"})
    graph.add_node(canary)

    yield graph

    # Cleanup
    graph.clear_graph()


class TestInjectionInsertNode:
    """Prove injection payloads in node IDs and properties are stored as literal data."""

    def test_insert_nodes_with_injection_ids(self, na_graph):
        for payload in INJECTION_PAYLOADS:
            node = Node(id=payload, labels=["InjectionTest"], properties={"name": payload})
            na_graph.add_node(node)

        # Verify canary survived
        all_nodes = na_graph.get_all_nodes()
        node_ids = {n["~id"] for n in all_nodes}
        assert CANARY_NODE_ID in node_ids, "Canary node was destroyed by injection"

        # Verify all payloads stored literally
        for payload in INJECTION_PAYLOADS:
            assert payload in node_ids, f"Payload '{payload[:30]}...' not found as literal node ID"

    def test_query_back_injection_node(self, na_graph):
        """Query a node whose ID is an injection payload and verify it returns as data."""
        payload = INJECTION_PAYLOADS[0]
        results = na_graph.na_client.execute_generic_query(
            "MATCH (n) WHERE id(n) = $id RETURN n",
            parameter_map={"id": payload},
        )
        assert len(results) == 1
        assert results[0]["n"]["~id"] == payload


class TestInjectionInsertEdge:
    """Prove injection payloads in edge properties are stored as literal data."""

    def test_insert_edges_with_injection_properties(self, na_graph):
        for i, payload in enumerate(INJECTION_PAYLOADS):
            src = Node(id=f"edge_src_{i}", labels=["EdgeTest"], properties={})
            dest = Node(id=f"edge_dst_{i}", labels=["EdgeTest"], properties={})
            na_graph.add_node(src)
            na_graph.add_node(dest)

            edge = Edge(
                label="INJECTION_EDGE",
                properties={"malicious": payload},
                node_src=src,
                node_dest=dest,
            )
            na_graph.add_edge(edge)

        # Verify canary survived
        all_nodes = na_graph.get_all_nodes()
        node_ids = {n["~id"] for n in all_nodes}
        assert CANARY_NODE_ID in node_ids, "Canary node was destroyed by edge injection"

        # Verify edges exist with literal payload in properties
        all_edges = na_graph.get_all_edges()
        malicious_values = set()
        for e in all_edges:
            props = e.get("~properties", {})
            if "malicious" in props:
                malicious_values.add(props["malicious"])
        for payload in INJECTION_PAYLOADS:
            assert payload in malicious_values, f"Edge payload '{payload[:30]}...' not stored literally"


class TestInjectionUpdateNode:
    """Prove injection payloads in update properties are stored as literal data."""

    def test_update_node_with_injection_property(self, na_graph):
        payload = INJECTION_PAYLOADS[0]
        node = Node(id=CANARY_NODE_ID, labels=["Canary"], properties={})
        na_graph.update_node("Canary", "a", node, {"a.injected": payload})

        # Verify canary still exists and has the literal payload as a property
        results = na_graph.na_client.execute_generic_query(
            "MATCH (n) WHERE id(n) = $id RETURN n",
            parameter_map={"id": CANARY_NODE_ID},
        )
        assert len(results) == 1
        props = results[0]["n"].get("~properties", {})
        assert props["injected"] == payload


class TestInjectionCanarySurvival:
    """Final check: canary node must still exist after all injection tests."""

    def test_canary_survives(self, na_graph):
        results = na_graph.na_client.execute_generic_query(
            "MATCH (n:Canary) WHERE id(n) = $id RETURN n",
            parameter_map={"id": CANARY_NODE_ID},
        )
        assert len(results) == 1, "Canary node did not survive injection tests"
        assert results[0]["n"]["~id"] == CANARY_NODE_ID

    def test_graph_not_empty(self, na_graph):
        """Graph must still contain data — injection did not wipe it."""
        all_nodes = na_graph.get_all_nodes()
        assert len(all_nodes) > 1, "Graph appears wiped — injection may have succeeded"
