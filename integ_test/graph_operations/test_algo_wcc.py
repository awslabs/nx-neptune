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

import pytest

import networkx as nx
from nx_neptune import Node
from utils.test_utils import BACKEND, neptune_graph


@pytest.fixture
def graph():
    """Create a new directed graph"""
    return nx.DiGraph()


@pytest.fixture
def test_graph():
    """Create test graph with multiple weakly connected components"""
    g = nx.DiGraph()
    # Component 1: A-B-C cycle
    g.add_edge("A", "B")
    g.add_edge("B", "C")
    g.add_edge("C", "A")
    # Component 2: D-E-F chain
    g.add_edge("D", "E")
    g.add_edge("E", "F")
    # Component 3: isolated pair
    g.add_edge("G", "H")
    return g


class TestWCC:
    def test_wcc_basic(self, test_graph):
        """Test basic weakly connected components"""
        result = nx.weakly_connected_components(test_graph, backend=BACKEND)

        assert isinstance(result, list)
        assert len(result) > 0

        # Verify components are sets
        for component in result:
            assert isinstance(component, set)
            assert len(component) > 0

    def test_wcc_component_count(self, test_graph):
        """Test that WCC finds the correct number of components"""
        result = nx.weakly_connected_components(test_graph, backend=BACKEND)

        # Our test graph has 3 components
        assert len(result) == 3

    def test_wcc_component_membership(self, test_graph):
        """Test that nodes are in the correct components"""
        result = nx.weakly_connected_components(test_graph, backend=BACKEND)

        # All nodes should appear exactly once across all components
        all_nodes = set()
        for component in result:
            all_nodes.update(component)

        expected_nodes = {"A", "B", "C", "D", "E", "F", "G", "H"}
        assert all_nodes == expected_nodes

    def test_wcc_with_concurrency(self, test_graph):
        """Test WCC with concurrency parameter"""
        result = nx.weakly_connected_components(
            test_graph, backend=BACKEND, concurrency=1
        )

        assert isinstance(result, list)
        assert len(result) == 3

    def test_wcc_with_edge_labels(self, test_graph):
        """Test WCC with edge_labels parameter"""
        result = nx.weakly_connected_components(
            test_graph, backend=BACKEND, edge_labels=["RELATES_TO"]
        )

        assert isinstance(result, list)
        assert len(result) > 0

    def test_wcc_mutation(self, test_graph, neptune_graph):
        """Test WCC with write_property (mutation)"""
        result = nx.weakly_connected_components(
            test_graph, backend=BACKEND, write_property="wccid"
        )

        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0

        # Verify nodes exist after mutation
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None
            assert "wccid" in node.properties

    def test_wcc_empty_graph(self, graph):
        """Test WCC on empty graph"""
        result = nx.weakly_connected_components(graph, backend=BACKEND)

        assert isinstance(result, list)
        assert len(result) == 0

    def test_wcc_single_node(self, graph):
        """Test WCC on single node graph"""
        graph.add_node("A")
        result = nx.weakly_connected_components(graph, backend=BACKEND)

        assert isinstance(result, list)
        assert len(result) == 1
        assert {"A"} in result

    def test_wcc_fully_connected(self):
        """Test WCC on a fully connected graph (single component)"""
        g = nx.DiGraph()
        g.add_edges_from([("A", "B"), ("B", "C"), ("C", "D"), ("D", "A")])
        result = nx.weakly_connected_components(g, backend=BACKEND)

        assert len(result) == 1
        assert {"A", "B", "C", "D"} in result

    def test_wcc_matches_networkx(self, test_graph):
        """Test that Neptune Analytics WCC matches NetworkX local result"""
        # Run with Neptune Analytics
        na_result = nx.weakly_connected_components(test_graph, backend=BACKEND)
        na_sets = {frozenset(c) for c in na_result}

        # Run with NetworkX locally
        nx_result = list(nx.weakly_connected_components(test_graph))
        nx_sets = {frozenset(c) for c in nx_result}

        assert na_sets == nx_sets
