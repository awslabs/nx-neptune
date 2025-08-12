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
import os
from unittest.mock import MagicMock, patch

import pytest
from networkx.classes import Graph

from nx_neptune import degree_centrality, in_degree_centrality, out_degree_centrality
from nx_neptune.clients import pagerank_query, PARAM_TRAVERSAL_DIRECTION
from nx_neptune.clients.neptune_constants import (
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_VERTEX_LABEL,
    PARAM_WRITE_PROPERTY,
)
from nx_neptune.clients.opencypher_builder import (
    degree_centrality_query,
    degree_centrality_mutation_query,
)
from nx_neptune.na_graph import NeptuneGraph


class TestDegreeCentrality:
    """Test suite for all three variants of degree centrality function in nx_neptune."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph_nx.execute_call.return_value = [
            {"n.id": "A", "degree": 1},
            {"n.id": "B", "degree": 2},
            {"n.id": "C", "degree": 3},
            {"n.id": "D", "degree": 2},
            {"n.id": "E", "degree": 2},
        ]

        graph = MagicMock(spec=Graph)
        graph.number_of_nodes.return_value = 3

        graph_nx.graph = graph
        return graph_nx

    def test_degree_centrality_basic(self, mock_graph):
        """Test basic functionality of degree centrality."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = degree_centrality(mock_graph)

            # Verify the correct query was built and executed
            parameters = {}
            (expected_query, param_values) = degree_centrality_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.degree" in expected_query

            # Verify the result contains the expected nodes with their degree values
            assert result == {"A": 0.5, "B": 1.0, "C": 1.5, "D": 1.0, "E": 1.0}

    def test_in_degree_centrality_basic(self, mock_graph):
        """Test basic functionality of In Degree Centrality."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = in_degree_centrality(mock_graph)

            # Verify the correct query was built and executed
            parameters = {PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTION_INBOUND}
            (expected_query, param_values) = degree_centrality_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.degree" in expected_query

            # Verify the result contains the expected nodes with their degree values
            assert result == {"A": 0.5, "B": 1.0, "C": 1.5, "D": 1.0, "E": 1.0}

    def test_out_degree_centrality_basic(self, mock_graph):
        """Test basic functionality of Out Degree Centrality."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = out_degree_centrality(mock_graph)

            # Verify the correct query was built and executed
            parameters = {PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTION_OUTBOUND}
            (expected_query, param_values) = degree_centrality_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.degree" in expected_query

            # Verify the result contains the expected nodes with their degree values
            assert result == {"A": 0.5, "B": 1.0, "C": 1.5, "D": 1.0, "E": 1.0}

    def test_degree_centrality_extra_options(self, mock_graph):
        """Test Degree Centrality with Neptune Specific parameters"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = degree_centrality(
                mock_graph,
                vertex_label="test_vertex_label",
                edge_labels=["test_edge_label"],
                concurrency=0,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_VERTEX_LABEL: "test_vertex_label",
                PARAM_EDGE_LABELS: ["test_edge_label"],
                PARAM_CONCURRENCY: 0,
            }

            (expected_query, param_values) = degree_centrality_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.degree" in expected_query

            # Verify the result contains the expected nodes with their degree values
            assert result == {"A": 0.5, "B": 1.0, "C": 1.5, "D": 1.0, "E": 1.0}

    def test_degree_centrality_mutation(self, mock_graph):
        """Test Degree Centrality Mutation with writeProperty"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = degree_centrality(mock_graph, write_property="degree")

            # Verify the correct query was built and executed
            parameters = {PARAM_WRITE_PROPERTY: "degree"}

            (expected_query, param_values) = degree_centrality_mutation_query(
                parameters
            )

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.degree.mutate" in expected_query

            # Verify the result contains the expected nodes with their degree values
            assert result == {}
