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

import pytest

import networkx as nx
from utils.test_utils import BACKEND, neptune_graph


@pytest.fixture
def weighted_graph():
    """Create a weighted directed graph for SSSP testing."""
    g = nx.DiGraph()
    # Simple graph:
    # A --4--> B --3--> D --1--> E
    # A --2--> C --1--> B
    # C --5--> D
    # Shortest path A->D: A->C->B->D (cost 6)
    # Shortest path A->E: A->C->B->D->E (cost 7)
    g.add_edge("A", "B", weight=4)
    g.add_edge("A", "C", weight=2)
    g.add_edge("B", "D", weight=3)
    g.add_edge("C", "B", weight=1)
    g.add_edge("C", "D", weight=5)
    g.add_edge("D", "E", weight=1)
    return g


class TestBellmanFordPath:

    def test_bellman_ford_path_basic(self, weighted_graph):
        """Test basic shortest path finding on weighted graph."""
        result = nx.bellman_ford_path(
            weighted_graph,
            source="A",
            target="E",
            weight="weight",
            backend=BACKEND,
        )

        assert isinstance(result, list)
        assert len(result) >= 2
        assert result[0] == "A"
        assert result[-1] == "E"

    def test_bellman_ford_path_adjacent(self, weighted_graph):
        """Test path between adjacent nodes."""
        result = nx.bellman_ford_path(
            weighted_graph,
            source="A",
            target="B",
            weight="weight",
            backend=BACKEND,
        )

        assert isinstance(result, list)
        assert result[0] == "A"
        assert result[-1] == "B"

    def test_bellman_ford_path_with_edge_weight_type(self, weighted_graph):
        """Test with explicit edge_weight_type parameter."""
        result = nx.bellman_ford_path(
            weighted_graph,
            source="A",
            target="D",
            weight="weight",
            edge_weight_type="int",
            backend=BACKEND,
        )

        assert isinstance(result, list)
        assert result[0] == "A"
        assert result[-1] == "D"


class TestSingleSourceBellmanFordPathLength:

    def test_path_length_basic(self, weighted_graph):
        """Test basic SSSP distances from source."""
        result = nx.single_source_bellman_ford_path_length(
            weighted_graph,
            source="A",
            weight="weight",
            backend=BACKEND,
        )

        assert isinstance(result, dict)
        assert len(result) > 0
        # All distances should be non-negative
        for node, dist in result.items():
            assert dist >= 0

    def test_path_length_values(self, weighted_graph):
        """Test that distances are reasonable for our known graph."""
        result = nx.single_source_bellman_ford_path_length(
            weighted_graph,
            source="A",
            weight="weight",
            edge_weight_type="int",
            backend=BACKEND,
        )

        assert isinstance(result, dict)
        # Should have distances to reachable nodes (B, C, D, E)
        assert len(result) >= 4


class TestBellmanFordPredecessorAndDistance:

    def test_predecessor_and_distance_basic(self, weighted_graph):
        """Test basic predecessor and distance computation."""
        pred, dist = nx.bellman_ford_predecessor_and_distance(
            weighted_graph,
            source="A",
            weight="weight",
            backend=BACKEND,
        )

        assert isinstance(pred, dict)
        assert isinstance(dist, dict)
        # Source should have empty predecessors
        assert pred["A"] == []
        # Source distance should be 0
        assert dist["A"] == 0
        # Other nodes should have predecessors
        for node in ["B", "C", "D", "E"]:
            if node in pred:
                assert isinstance(pred[node], list)
            if node in dist:
                assert dist[node] >= 0

    def test_predecessor_and_distance_with_params(self, weighted_graph):
        """Test with edge_weight_type parameter."""
        pred, dist = nx.bellman_ford_predecessor_and_distance(
            weighted_graph,
            source="A",
            weight="weight",
            edge_weight_type="int",
            backend=BACKEND,
        )

        assert isinstance(pred, dict)
        assert isinstance(dist, dict)
        assert len(dist) >= 4  # A, B, C, D, E all reachable
