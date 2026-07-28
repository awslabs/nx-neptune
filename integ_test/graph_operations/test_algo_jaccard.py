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
from utils.test_utils import BACKEND, neptune_graph, air_route_graph


@pytest.fixture
def graph():
    """Create a new undirected graph"""
    return nx.Graph()


class TestJaccardCoefficient:

    def test_jaccard_coefficient_basic(self, air_route_graph):
        """Test basic jaccard coefficient on airline routes data"""
        pairs = [("JFK", "LAX"), ("SFO", "ORD")]
        result = nx.jaccard_coefficient(air_route_graph, ebunch=pairs, backend=BACKEND)

        result_list = list(result)
        assert len(result_list) == 2

        for u, v, score in result_list:
            assert isinstance(score, float)
            assert 0 <= score <= 1

    def test_jaccard_coefficient_with_edge_labels(self, air_route_graph):
        """Test jaccard coefficient with edge label filtering"""
        pairs = [("JFK", "LAX")]
        result = nx.jaccard_coefficient(
            air_route_graph,
            ebunch=pairs,
            backend=BACKEND,
            edge_labels=["RELATES_TO"],
        )

        result_list = list(result)
        assert len(result_list) == 1

        u, v, score = result_list[0]
        assert u == "JFK"
        assert v == "LAX"
        assert isinstance(score, float)
        assert 0 <= score <= 1

    def test_jaccard_coefficient_with_vertex_label(self, air_route_graph):
        """Test jaccard coefficient with vertex label filtering"""
        pairs = [("JFK", "LAX")]
        result = nx.jaccard_coefficient(
            air_route_graph,
            ebunch=pairs,
            backend=BACKEND,
            vertex_label="Node",
        )

        result_list = list(result)
        assert len(result_list) == 1
        assert isinstance(result_list[0][2], float)

    def test_jaccard_coefficient_with_traversal_direction(self, air_route_graph):
        """Test jaccard coefficient with traversal direction"""
        pairs = [("JFK", "LAX")]
        result = nx.jaccard_coefficient(
            air_route_graph,
            ebunch=pairs,
            backend=BACKEND,
            traversal_direction="both",
        )

        result_list = list(result)
        assert len(result_list) == 1
        assert isinstance(result_list[0][2], float)
        assert 0 <= result_list[0][2] <= 1

    def test_jaccard_coefficient_empty_graph(self, graph):
        """Test jaccard coefficient on empty graph"""
        result = nx.jaccard_coefficient(graph, ebunch=[], backend=BACKEND)

        result_list = list(result)
        assert len(result_list) == 0

    def test_jaccard_coefficient_single_pair(self, graph):
        """Test jaccard coefficient with a simple graph"""
        # Create a graph with shared neighbors
        graph.add_edges_from([("A", "C"), ("A", "D"), ("B", "C"), ("B", "D"), ("B", "E")])
        result = nx.jaccard_coefficient(graph, ebunch=[("A", "B")], backend=BACKEND)

        result_list = list(result)
        assert len(result_list) == 1
        u, v, score = result_list[0]
        assert u == "A"
        assert v == "B"
        assert isinstance(score, float)
        assert 0 <= score <= 1
