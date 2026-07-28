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

from nx_neptune.algorithms.communities.wcc import weakly_connected_components
from nx_neptune.clients.opencypher_builder import (
    wcc_mutation_query,
    wcc_query,
)
from nx_neptune.na_graph import NeptuneGraph


class TestWCC:
    """Test suite for Weakly Connected Components algorithm in nx_neptune."""

    PARSED_RESULT_SET = [
        {"ATL", "JFK", "LAX", "ORD"},
        {"SEA", "SFO", "PDX"},
        {"ANC", "FAI"},
    ]

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph_nx.execute_call.return_value = [
            {
                "component": 1001,
                "members": ["ATL", "JFK", "LAX", "ORD"],
            },
            {
                "component": 1002,
                "members": ["SEA", "SFO", "PDX"],
            },
            {
                "component": 1003,
                "members": ["ANC", "FAI"],
            },
        ]

        graph = MagicMock(spec=Graph)
        graph_nx.graph = graph
        return graph_nx

    def test_wcc_basic(self, mock_graph):
        """Test basic functionality of weakly_connected_components."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(mock_graph)

            # Verify the correct query was built and executed
            expected_query, param_values = wcc_query()

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            assert "neptune.algo.wcc" in expected_query
            assert result == self.PARSED_RESULT_SET

    def test_wcc_with_edge_labels(self, mock_graph):
        """Test WCC with edge label filtering."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(
                mock_graph,
                edge_labels=["route"],
            )

            parameters = {
                "edgeLabels": ["route"],
            }

            expected_query, param_values = wcc_query(parameters)

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.wcc" in expected_query
            assert result == self.PARSED_RESULT_SET

    def test_wcc_with_vertex_label(self, mock_graph):
        """Test WCC with vertex label filtering."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(
                mock_graph,
                vertex_label="airport",
            )

            parameters = {
                "vertexLabel": "airport",
            }

            expected_query, param_values = wcc_query(parameters)

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.wcc" in expected_query
            assert result == self.PARSED_RESULT_SET

    def test_wcc_with_all_options(self, mock_graph):
        """Test WCC with all Neptune Analytics options."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(
                mock_graph,
                edge_labels=["route", "connects"],
                vertex_label="airport",
                concurrency=0,
            )

            parameters = {
                "edgeLabels": ["route", "connects"],
                "vertexLabel": "airport",
                "concurrency": 0,
            }

            expected_query, param_values = wcc_query(parameters)

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.wcc" in expected_query
            assert result == self.PARSED_RESULT_SET

    def test_wcc_mutate(self, mock_graph):
        """Test mutate variant of weakly_connected_components."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(
                mock_graph,
                write_property="wccid",
            )

            parameters = {
                "writeProperty": "wccid",
            }

            expected_query, param_values = wcc_mutation_query(parameters)

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            assert "neptune.algo.wcc.mutate" in expected_query
            assert result == {}

    def test_wcc_mutate_with_options(self, mock_graph):
        """Test mutate variant with additional filtering options."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(
                mock_graph,
                edge_labels=["route"],
                vertex_label="airport",
                concurrency=1,
                write_property="component_id",
            )

            parameters = {
                "edgeLabels": ["route"],
                "vertexLabel": "airport",
                "concurrency": 1,
                "writeProperty": "component_id",
            }

            expected_query, param_values = wcc_mutation_query(parameters)

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            assert "neptune.algo.wcc.mutate" in expected_query
            assert result == {}

    def test_wcc_returns_list_of_sets(self, mock_graph):
        """Test that WCC returns the correct data structure (list of sets)."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(mock_graph)

            assert isinstance(result, list)
            for component in result:
                assert isinstance(component, set)

    def test_wcc_empty_result(self, mock_graph):
        """Test WCC with empty result set."""
        mock_graph.execute_call.return_value = []

        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = weakly_connected_components(mock_graph)

            assert result == []
