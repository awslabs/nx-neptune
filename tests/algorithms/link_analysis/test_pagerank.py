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

import pytest
from unittest.mock import MagicMock, patch

from nx_neptune.clients import pagerank_query
from nx_neptune.clients.neptune_constants import (
    PARAM_DAMPING_FACTOR,
    PARAM_NUM_OF_ITERATIONS,
    PARAM_TOLERANCE,
    PARAM_VERTEX_LABEL,
    PARAM_EDGE_LABELS,
    PARAM_CONCURRENCY,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_EDGE_WEIGHT_PROPERTY,
    PARAM_EDGE_WEIGHT_TYPE,
    PARAM_SOURCE_NODES,
    PARAM_SOURCE_WEIGHTS,
    PARAM_WRITE_PROPERTY,
)
from nx_neptune.clients.opencypher_builder import pagerank_mutation_query
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.algorithms.link_analysis.pagerank import pagerank


class TestPageRank:
    """Test suite for the pagerank function in nx_neptune."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph.execute_call.return_value = [
            {
                "n": {"~id": "1", "~labels": ["Person"], "~properties": {"name": "A"}},
                "rank": 0.3,
            },
            {
                "n": {"~id": "2", "~labels": ["Person"], "~properties": {"name": "B"}},
                "rank": 0.2,
            },
            {
                "n": {"~id": "3", "~labels": ["Person"], "~properties": {"name": "C"}},
                "rank": 0.5,
            },
        ]
        return graph

    def test_pagerank_basic(self, mock_graph):
        """Test basic functionality of pagerank."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization=None,
                max_iter=100,
                tol=1e-06,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the correct query was built and executed
            parameters = {}
            (expected_query, param_values) = pagerank_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.pageRank" in expected_query

            # Verify the result contains the expected nodes with their PageRank values
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    def test_pagerank_with_alpha(self, mock_graph):
        """Test pagerank with custom alpha parameter (0.75)."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            damping_factor = 0.75
            result = pagerank(
                mock_graph,
                alpha=damping_factor,
                personalization=None,
                max_iter=100,
                tol=1e-06,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the correct query was built and executed
            parameters = {PARAM_DAMPING_FACTOR: damping_factor}
            (expected_query, param_values) = pagerank_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.pageRank" in expected_query
            assert f"{PARAM_DAMPING_FACTOR}:{damping_factor}" in expected_query

            # Verify the result
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    def test_pagerank_with_max_iter(self, mock_graph):
        """Test pagerank with custom max_iter parameter (50)."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            num_of_iterations = 50
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization=None,
                max_iter=num_of_iterations,
                tol=1e-06,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the correct query was built and executed
            parameters = {PARAM_NUM_OF_ITERATIONS: num_of_iterations}
            (expected_query, param_values) = pagerank_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.pageRank" in expected_query
            assert f"{PARAM_NUM_OF_ITERATIONS}:{num_of_iterations}" in expected_query

            # Verify the result
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    def test_pagerank_with_tolerance(self, mock_graph):
        """Test pagerank with custom tolerance parameter (1e-04)."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            tolerance = 1e-04
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization=None,
                max_iter=100,
                tol=tolerance,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the correct query was built and executed
            parameters = {PARAM_TOLERANCE: tolerance}
            (expected_query, param_values) = pagerank_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify the result
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    def test_pagerank_with_personalisation(self, mock_graph):
        """Test pagerank with personalization parameter."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            tolerance = 1e-04
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization={"A": 1, "B": 2.4},
                max_iter=100,
                tol=tolerance,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_TOLERANCE: tolerance,
                PARAM_SOURCE_NODES: ["A", "B"],
                PARAM_SOURCE_WEIGHTS: [1, 2.4],
            }
            (expected_query, param_values) = pagerank_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify the result
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    def test_pagerank_with_na_parameters(self, mock_graph, traversalDirection=None):
        """Test pagerank with custom Neptune Analytics parameters"""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            tolerance = 1e-04
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization=None,
                max_iter=100,
                tol=tolerance,
                nstart=None,
                weight=None,
                dangling=None,
                vertex_label="A",
                edge_labels=["RELATES_TO"],
                concurrency=0,
                traversal_direction="inbound",
                edge_weight_property="weight",
                edge_weight_type="int",
                source_nodes=["A", "B"],
                source_weights=[1, 1.5],
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_TOLERANCE: tolerance,
                PARAM_VERTEX_LABEL: "A",
                PARAM_EDGE_LABELS: ["RELATES_TO"],
                PARAM_CONCURRENCY: 0,
                PARAM_TRAVERSAL_DIRECTION: "inbound",
                PARAM_EDGE_WEIGHT_PROPERTY: "weight",
                PARAM_EDGE_WEIGHT_TYPE: "int",
                PARAM_SOURCE_NODES: ["A", "B"],
                PARAM_SOURCE_WEIGHTS: [1, 1.5],
            }
            (expected_query, param_values) = pagerank_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify the result
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    def test_pagerank_empty_result(self, mock_graph):
        """
        Test pagerank when no results are returned,
        in the of method being called with networkX default value,
        no additional option should be passed as part of the openCypher call.
        """
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            mock_graph.execute_call.return_value = []

            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization=None,
                max_iter=100,
                tol=1e-06,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the result is an empty dictionary
            assert result == {}

    @patch("nx_neptune.algorithms.util.algorithm_utils.logger")
    def test_pagerank_unsupported_parameters_warning(self, mock_logger, mock_graph):
        """Test that warnings are logged for unsupported parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            # Call pagerank with unsupported parameters
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization={"A": 1.0},  # Unsupported
                max_iter=100,
                tol=1e-06,
                nstart={"B": 0.5},  # Unsupported
                weight="weight",  # Unsupported
                dangling={"C": 0.3},  # Unsupported
            )

            # Verify warnings were logged for each unsupported parameter
            assert mock_logger.warning.call_count == 2

            # Common warning message suffix
            warning_suffix = (
                " parameter is not supported in Neptune Analytics implementation. "
                "This argument will be ignored and execution will proceed without it."
            )

            # Check specific warning messages
            mock_logger.warning.assert_any_call(f"'nstart'{warning_suffix}")
            mock_logger.warning.assert_any_call(f"'dangling'{warning_suffix}")

            # Verify the result is still correct
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    @patch("nx_neptune.algorithms.link_analysis.pagerank.logger")
    def test_pagerank_with_personalisation_option_conflict(
        self, mock_logger, mock_graph
    ):
        """Test pagerank when personalization and [source_nodes,source_weights] present."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            tolerance = 1e-04
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization={"A": 1, "B": 2.4},
                source_nodes=["C", "D"],
                source_weights=[3, 4],
                max_iter=100,
                tol=tolerance,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_TOLERANCE: tolerance,
                PARAM_SOURCE_NODES: ["C", "D"],
                PARAM_SOURCE_WEIGHTS: [3, 4],
            }
            (expected_query, param_values) = pagerank_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify warnings were logged for each unsupported parameter
            assert mock_logger.warning.call_count == 1
            # Make sure user receive warning about it.
            mock_logger.warning.assert_any_call(
                "Since personalization and both source_nodes and source_weights are provided, "
                "Neptune Analytics options will take precedence."
            )

            # Verify the result
            assert result == {"1": 0.3, "2": 0.2, "3": 0.5}

    @patch("nx_neptune.algorithms.link_analysis.pagerank.logger")
    def test_pagerank_with_incomplete_aws_personalisation_option(
        self, mock_logger, mock_graph
    ):
        """Test pagerank either source_nodes or source_weights present but not both."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            tolerance = 1e-04
            pagerank(
                mock_graph,
                alpha=0.85,
                personalization=None,
                source_nodes=["C", "D"],
                max_iter=100,
                tol=tolerance,
                nstart=None,
                weight=None,
                dangling=None,
            )

            # Verify the correct query was built and executed
            parameters = {
                PARAM_TOLERANCE: tolerance,
            }
            (expected_query, param_values) = pagerank_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify warnings were logged for each unsupported parameter
            assert mock_logger.warning.call_count == 1
            # Make sure user receive warning about it.
            mock_logger.warning.assert_any_call(
                "source_nodes and source_weights must be provided together. "
                "If only one is specified, both parameters will be ignored"
            )

    def test_pagerank_mutation(self, mock_graph):
        """Test pagerank with custom Neptune Analytics parameters"""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = pagerank(
                mock_graph,
                alpha=0.85,
                personalization=None,
                max_iter=100,
                tol=1e-06,
                nstart=None,
                weight=None,
                dangling=None,
                write_property="pageRank",
            )

            # Verify the correct query was built and executed
            parameters = {PARAM_WRITE_PROPERTY: "pageRank"}
            (expected_query, param_values) = pagerank_mutation_query(parameters)

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            assert "neptune.algo.pageRank.mutate" in expected_query

            # Verify the result contains the expected nodes with their degree values
            assert result == {}
