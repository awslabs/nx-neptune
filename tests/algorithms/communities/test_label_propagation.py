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

from nx_neptune import (
    label_propagation_communities,
    asyn_lpa_communities,
    fast_label_propagation_communities,
)
from resources_management.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_VERTEX_LABEL,
    PARAM_VERTEX_WEIGHT_PROPERTY,
    PARAM_VERTEX_WEIGHT_TYPE,
    PARAM_EDGE_WEIGHT_PROPERTY,
    PARAM_EDGE_WEIGHT_TYPE,
    PARAM_MAX_ITERATIONS,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_WRITE_PROPERTY,
)
from resources_management.clients.opencypher_builder import (
    label_propagation_query,
    label_propagation_mutation_query,
)
from nx_neptune.na_graph import NeptuneGraph


class TestLabelPropagation:
    """Test suite for all three variants of labels propagation algorithms in nx_neptune."""

    PARSED_RESULT_SET = [
        {"SLM", "IAA", "GRV", "ETZ", "MME", "ZAD"},
        {"FAI", "HSL", "HUS", "LMA", "GAL", "KBC"},
        {"FAT", "UII", "SJT", "VSA", "QBC", "LAM"},
    ]

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph_nx.execute_call.return_value = [
            {
                "community": 2357352929952144,
                "members": ["SLM", "IAA", "GRV", "ETZ", "MME", "ZAD"],
            },
            {
                "community": 2357352929952663,
                "members": ["FAI", "HSL", "HUS", "LMA", "GAL", "KBC"],
            },
            {
                "community": 2357352929952157,
                "members": ["FAT", "UII", "SJT", "VSA", "QBC", "LAM"],
            },
        ]

        graph = MagicMock(spec=Graph)
        graph_nx.graph = graph
        return graph_nx

    def test_label_propagation_communities_basic(self, mock_graph):
        """Test basic functionality of label_propagation_communities."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = label_propagation_communities(mock_graph)

            # Verify the correct query was built and executed
            parameters = {}

            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    def test_label_propagation_communities_extra_options(self, mock_graph):
        """Test functionality of label_propagation_communities with Neptune Specific parameters"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = label_propagation_communities(
                mock_graph,
                vertex_label="test_vertex_label",
                edge_labels=["test_edge_label"],
                vertex_weight_property="test_weight_property",
                vertex_weight_type="int",
                edge_weight_property="test_weight_property",
                edge_weight_type="int",
                max_iterations=100,
                traversal_direction="both",
                concurrency=0,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_EDGE_LABELS: ["test_edge_label"],
                PARAM_VERTEX_LABEL: "test_vertex_label",
                PARAM_VERTEX_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_VERTEX_WEIGHT_TYPE: "int",
                PARAM_EDGE_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_EDGE_WEIGHT_TYPE: "int",
                PARAM_MAX_ITERATIONS: 100,
                PARAM_TRAVERSAL_DIRECTION: "both",
                PARAM_CONCURRENCY: 0,
            }
            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    def test_asyn_lpa_communities_basic(self, mock_graph):
        """Test basic functionality of asyn_lpa_communities."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = asyn_lpa_communities(mock_graph)

            # Verify the correct query was built and executed
            parameters = {}

            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    def test_asyn_lpa_communities_nx_options(self, mock_graph):
        """Test basic functionality of asyn_lpa_communities."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = asyn_lpa_communities(mock_graph, weight="test_weight_property")

            # Verify the correct query was built and executed
            parameters = {
                PARAM_EDGE_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_EDGE_WEIGHT_TYPE: "float",
            }

            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    def test_asyn_lpa_communities_extra_options(self, mock_graph):
        """Test functionality of asyn_lpa_communities with Neptune Specific parameters"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = asyn_lpa_communities(
                mock_graph,
                vertex_label="test_vertex_label",
                edge_labels=["test_edge_label"],
                vertex_weight_property="test_weight_property",
                vertex_weight_type="int",
                edge_weight_property="test_weight_property",
                edge_weight_type="int",
                max_iterations=100,
                traversal_direction="both",
                concurrency=0,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_EDGE_LABELS: ["test_edge_label"],
                PARAM_VERTEX_LABEL: "test_vertex_label",
                PARAM_VERTEX_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_VERTEX_WEIGHT_TYPE: "int",
                PARAM_EDGE_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_EDGE_WEIGHT_TYPE: "int",
                PARAM_MAX_ITERATIONS: 100,
                PARAM_TRAVERSAL_DIRECTION: "both",
                PARAM_CONCURRENCY: 0,
            }

            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    @patch("nx_neptune.algorithms.util.algorithm_utils.logger")
    def test_asyn_lpa_communities_parameters_warning(self, mock_logger, mock_graph):
        """Test execution of asyn_lpa_communities with unsupported parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):

            # Execute
            result = asyn_lpa_communities(mock_graph, weight="A", seed=12)

            # Verify warnings were logged for each unsupported parameter
            assert mock_logger.warning.call_count == 1

            # Common warning message suffix
            warning_suffix = (
                " parameter is not supported in Neptune Analytics implementation. "
                "This argument will be ignored and execution will proceed without it."
            )

            # Check specific warning messages
            mock_logger.warning.assert_any_call(f"'seed'{warning_suffix}")

            assert list(result) == self.PARSED_RESULT_SET

    def test_fast_label_propagation_communities_basic(self, mock_graph):
        """Test basic functionality of fast_label_propagation_communities."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = fast_label_propagation_communities(mock_graph)

            # Verify the correct query was built and executed
            parameters = {}

            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    def test_fast_label_propagation_communities_nx_options(self, mock_graph):
        """Test basic functionality of fast_label_propagation_communities."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = fast_label_propagation_communities(
                mock_graph, weight="test_weight_property"
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_EDGE_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_EDGE_WEIGHT_TYPE: "float",
            }

            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    def test_fast_label_propagation_communities_extra_options(self, mock_graph):
        """Test functionality of fast_label_propagation_communities with Neptune Specific parameters"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = fast_label_propagation_communities(
                mock_graph,
                vertex_label="test_vertex_label",
                edge_labels=["test_edge_label"],
                vertex_weight_property="test_weight_property",
                vertex_weight_type="int",
                edge_weight_property="test_weight_property",
                edge_weight_type="int",
                max_iterations=100,
                traversal_direction="both",
                concurrency=0,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_EDGE_LABELS: ["test_edge_label"],
                PARAM_VERTEX_LABEL: "test_vertex_label",
                PARAM_VERTEX_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_VERTEX_WEIGHT_TYPE: "int",
                PARAM_EDGE_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_EDGE_WEIGHT_TYPE: "int",
                PARAM_MAX_ITERATIONS: 100,
                PARAM_TRAVERSAL_DIRECTION: "both",
                PARAM_CONCURRENCY: 0,
            }

            (expected_query, param_values) = label_propagation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation" in expected_query

            assert list(result) == self.PARSED_RESULT_SET

    @patch("nx_neptune.algorithms.util.algorithm_utils.logger")
    def test_fast_label_propagation_communities_parameters_warning(
        self, mock_logger, mock_graph
    ):
        """Test execution of fast_label_propagation_communities with unsupported parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):

            # Execute
            result = fast_label_propagation_communities(mock_graph, weight="A", seed=12)

            # Verify warnings were logged for each unsupported parameter
            assert mock_logger.warning.call_count == 1

            # Common warning message suffix
            warning_suffix = (
                " parameter is not supported in Neptune Analytics implementation. "
                "This argument will be ignored and execution will proceed without it."
            )

            # Check specific warning messages
            mock_logger.warning.assert_any_call(f"'seed'{warning_suffix}")

            assert list(result) == self.PARSED_RESULT_SET

    def test_label_propagation_communities_mutation(self, mock_graph):
        """Test functionality of label_propagation_communities Mutation with writeProperty"""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = asyn_lpa_communities(
                mock_graph,
                vertex_label="test_vertex_label",
                edge_labels=["test_edge_label"],
                vertex_weight_property="test_weight_property",
                vertex_weight_type="int",
                edge_weight_property="test_weight_property",
                edge_weight_type="int",
                max_iterations=100,
                traversal_direction="both",
                concurrency=0,
                write_property="communities",
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_EDGE_LABELS: ["test_edge_label"],
                PARAM_VERTEX_LABEL: "test_vertex_label",
                PARAM_VERTEX_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_VERTEX_WEIGHT_TYPE: "int",
                PARAM_EDGE_WEIGHT_PROPERTY: "test_weight_property",
                PARAM_EDGE_WEIGHT_TYPE: "int",
                PARAM_MAX_ITERATIONS: 100,
                PARAM_TRAVERSAL_DIRECTION: "both",
                PARAM_CONCURRENCY: 0,
                PARAM_WRITE_PROPERTY: "communities",
            }

            (expected_query, param_values) = label_propagation_mutation_query(
                parameters
            )

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.labelPropagation.mutate" in expected_query
            assert result == {}
