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

from nx_neptune import jaccard_coefficient
from nx_neptune.clients.neptune_constants import (
    PARAM_EDGE_LABELS,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_VERTEX_LABEL,
)
from nx_neptune.clients.opencypher_builder import jaccard_coefficient_query
from nx_neptune.na_graph import NeptuneGraph


class TestJaccardCoefficient:
    """Test suite for jaccard_coefficient function in nx_neptune."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph_nx.execute_call.return_value = [{"score": 0.6}]

        graph = MagicMock(spec=Graph)
        graph.number_of_nodes.return_value = 5
        graph_nx.graph = graph
        return graph_nx

    def test_jaccard_coefficient_basic(self, mock_graph):
        """Test basic functionality of jaccard coefficient with ebunch."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(mock_graph, ebunch=[("A", "B")])

            # Verify the correct query was built and executed
            expected_query, param_values = jaccard_coefficient_query(["A"], ["B"])
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.jaccardSimilarity" in expected_query

            # Verify the result format (iterator of (u, v, p) tuples)
            result_list = list(result)
            assert len(result_list) == 1
            assert result_list[0] == ("A", "B", 0.6)

    def test_jaccard_coefficient_multiple_pairs(self, mock_graph):
        """Test jaccard coefficient with multiple node pairs (batched)."""
        mock_graph.execute_call.return_value = [
            {"score": 0.6},
            {"score": 0.4},
            {"score": 0.8},
        ]

        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(
                mock_graph, ebunch=[("A", "B"), ("C", "D"), ("E", "F")]
            )

            result_list = list(result)
            assert len(result_list) == 3
            # Batched: only one call to Neptune
            mock_graph.execute_call.assert_called_once()

            assert result_list[0] == ("A", "B", 0.6)
            assert result_list[1] == ("C", "D", 0.4)
            assert result_list[2] == ("E", "F", 0.8)

    def test_jaccard_coefficient_with_edge_labels(self, mock_graph):
        """Test jaccard coefficient with edge label filtering."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(
                mock_graph,
                ebunch=[("A", "B")],
                edge_labels=["route", "knows"],
            )

            expected_query, param_values = jaccard_coefficient_query(
                ["A"], ["B"], {PARAM_EDGE_LABELS: ["route", "knows"]}
            )
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            result_list = list(result)
            assert len(result_list) == 1
            assert result_list[0] == ("A", "B", 0.6)

    def test_jaccard_coefficient_with_vertex_label(self, mock_graph):
        """Test jaccard coefficient with vertex label filtering."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(
                mock_graph,
                ebunch=[("A", "B")],
                vertex_label="airport",
            )

            expected_query, param_values = jaccard_coefficient_query(
                ["A"], ["B"], {PARAM_VERTEX_LABEL: "airport"}
            )
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            result_list = list(result)
            assert len(result_list) == 1
            assert result_list[0] == ("A", "B", 0.6)

    def test_jaccard_coefficient_with_traversal_direction(self, mock_graph):
        """Test jaccard coefficient with traversal direction."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(
                mock_graph,
                ebunch=[("A", "B")],
                traversal_direction="both",
            )

            expected_query, param_values = jaccard_coefficient_query(
                ["A"], ["B"], {PARAM_TRAVERSAL_DIRECTION: "both"}
            )
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            result_list = list(result)
            assert len(result_list) == 1
            assert result_list[0] == ("A", "B", 0.6)

    def test_jaccard_coefficient_all_parameters(self, mock_graph):
        """Test jaccard coefficient with all Neptune Analytics parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(
                mock_graph,
                ebunch=[("A", "B")],
                edge_labels=["route"],
                vertex_label="airport",
                traversal_direction="outbound",
            )

            expected_query, param_values = jaccard_coefficient_query(
                ["A"],
                ["B"],
                {
                    PARAM_EDGE_LABELS: ["route"],
                    PARAM_VERTEX_LABEL: "airport",
                    PARAM_TRAVERSAL_DIRECTION: "outbound",
                },
            )
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            result_list = list(result)
            assert len(result_list) == 1
            assert result_list[0] == ("A", "B", 0.6)

    def test_jaccard_coefficient_empty_result(self, mock_graph):
        """Test jaccard coefficient when Neptune returns empty result."""
        mock_graph.execute_call.return_value = []

        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(mock_graph, ebunch=[("A", "B")])

            result_list = list(result)
            assert len(result_list) == 1
            # Should default to 0.0 when no result
            assert result_list[0] == ("A", "B", 0.0)

    def test_jaccard_coefficient_empty_ebunch(self, mock_graph):
        """Test jaccard coefficient with empty ebunch."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = jaccard_coefficient(mock_graph, ebunch=[])

            result_list = list(result)
            assert len(result_list) == 0
            mock_graph.execute_call.assert_not_called()
