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

from nx_neptune import closeness_centrality
from nx_neptune.clients import PARAM_TRAVERSAL_DIRECTION
from nx_neptune.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_VERTEX_LABEL,
    PARAM_NUM_SOURCES,
    PARAM_NORMALIZE,
    MAX_INT, PARAM_WRITE_PROPERTY,
)
from nx_neptune.clients.opencypher_builder import (
    closeness_centrality_query, closeness_centrality_mutation_query,
)
from nx_neptune.na_graph import NeptuneGraph


class TestClosenessCentrality:
    """Test suite for closeness centrality function in nx_neptune."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph_nx.execute_call.return_value = [
            {"nodeId": "YVR", "score": 0.16},
            {"nodeId": "HKG", "score": 0.23},
            {"nodeId": "SYD", "score": 0.11},
            {"nodeId": "AXT", "score": 0.45},
        ]

        graph = MagicMock(spec=Graph)
        graph.number_of_nodes.return_value = 4
        graph_nx.graph = graph
        return graph_nx

    def test_closeness_centrality_basic(self, mock_graph):
        """Test basic functionality of closeness centrality."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = closeness_centrality(mock_graph)

            # Verify the correct query was built and executed
            parameters = {PARAM_NUM_SOURCES: MAX_INT, PARAM_NORMALIZE: True}
            (expected_query, param_values) = closeness_centrality_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.closenessCentrality" in expected_query

            # Verify the result contains the expected nodes with their score values
            assert result == {"YVR": 0.16, "HKG": 0.23, "SYD": 0.11, "AXT": 0.45}

    def test_closeness_centrality_nx_options(self, mock_graph):
        """Test closeness centrality's ability to handle networkX arguments."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = closeness_centrality(
                mock_graph,
                u="YVR",
                distance="distance_property_name",
                wf_improved=False,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_NUM_SOURCES: 9223372036854775807,
                PARAM_NORMALIZE: False,
            }
            (expected_query, param_values) = closeness_centrality_query(
                parameters, ["YVR"]
            )

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.closenessCentrality" in expected_query

            # Verify the result contains the expected nodes with their score values
            assert result == {"YVR": 0.16, "HKG": 0.23, "SYD": 0.11, "AXT": 0.45}

    def test_closeness_centrality_aws_options(self, mock_graph):
        """Test closeness centrality's ability to handle AWS arguments."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = closeness_centrality(
                mock_graph,
                num_sources=100,
                edge_labels=["label_1", "label_2"],
                vertex_label="test_vertex_label",
                traversal_direction="both",
                concurrency=0,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_VERTEX_LABEL: "test_vertex_label",
                PARAM_EDGE_LABELS: ["label_1", "label_2"],
                PARAM_TRAVERSAL_DIRECTION: "both",
                PARAM_CONCURRENCY: 0,
                PARAM_NUM_SOURCES: 100,
                PARAM_NORMALIZE: True,
            }
            (expected_query, param_values) = closeness_centrality_query(parameters)

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.closenessCentrality" in expected_query

            # Verify the result contains the expected nodes with their score values
            assert result == {"YVR": 0.16, "HKG": 0.23, "SYD": 0.11, "AXT": 0.45}

    def test_closeness_centrality_conflict_options(self, mock_graph):
        """Make sure AWS options always take precedence."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = closeness_centrality(
                mock_graph,
                num_sources=100,
                edge_labels=["label_1", "label_2"],
                vertex_label="test_vertex_label",
                traversal_direction="both",
                concurrency=0,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_VERTEX_LABEL: "test_vertex_label",
                PARAM_EDGE_LABELS: ["label_1", "label_2"],
                PARAM_TRAVERSAL_DIRECTION: "both",
                PARAM_CONCURRENCY: 0,
                PARAM_NUM_SOURCES: 100,
                PARAM_NORMALIZE: True,
            }
            (expected_query, param_values) = closeness_centrality_query(parameters)

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.closenessCentrality" in expected_query

            # Verify the result contains the expected nodes with their score values
            assert result == {"YVR": 0.16, "HKG": 0.23, "SYD": 0.11, "AXT": 0.45}


    def test_closeness_centrality_mutation(self, mock_graph):
        """Test functionality of closeness centrality Mutation with writeProperty"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = closeness_centrality(
                mock_graph,
                u="YVR",
                distance="distance_property_name",
                wf_improved=False,
                write_property="score",
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_NUM_SOURCES: 9223372036854775807,
                PARAM_NORMALIZE: False,
                PARAM_WRITE_PROPERTY: "score"
            }
            (expected_query, param_values) = closeness_centrality_query(
                parameters, ["YVR"]
            )
            (expected_query, param_values) = closeness_centrality_mutation_query(
                parameters
            )

            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.closenessCentrality.mutate" in expected_query

            # Verify the result contains the expected nodes with their score values
            assert result == {}
