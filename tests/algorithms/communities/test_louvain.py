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

from nx_neptune.algorithms import louvain_communities
from nx_neptune.clients.opencypher_builder import (
    louvain_query,
    louvain_mutation_query,
)
from nx_neptune.na_graph import NeptuneGraph


class TestLouvain:
    """Test suite for Louvain algorithms in nx_neptune."""

    PARSED_RESULT_SET = [
        {"TRW", "INU", "MAJ"},
        {"NDU", "ERS", "OND", "MPA"},
        {"TLJ", "TCT", "MCG", "NIB"},
        {"BLD", "GCW"},
    ]

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph_nx.execute_call.return_value = [
            {
                "community": 137,
                "members": ["TRW", "INU", "MAJ"],
            },
            {
                "community": 138,
                "members": ["NDU", "ERS", "OND", "MPA"],
            },
            {
                "community": 140,
                "members": ["TLJ", "TCT", "MCG", "NIB"],
            },
            {
                "community": 143,
                "members": ["BLD", "GCW"],
            },
        ]

        graph = MagicMock(spec=Graph)
        graph_nx.graph = graph
        return graph_nx

    @patch("nx_neptune.algorithms.util.algorithm_utils.logger")
    def test_louvain_communities_basic(self, mock_logger, mock_graph):
        """Test basic functionality of louvain_communities."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = louvain_communities(
                mock_graph,
                # Default para from NX
                weight="weight",
                resolution=1,
                threshold=0.0000001,
                max_level=None,
                seed=None,
            )

            # Verify the correct query was built and executed
            parameters = {"iterationTolerance": 0.0000001}

            (expected_query, param_values) = louvain_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify warnings were logged for each unsupported parameter
            assert mock_logger.warning.call_count == 1

            # Common warning message suffix
            warning_suffix = (
                " parameter is not supported in Neptune Analytics implementation. "
                "This argument will be ignored and execution will proceed without it."
            )

            # Check specific warning messages
            mock_logger.warning.assert_any_call(f"'resolution'{warning_suffix}")

            assert "neptune.algo.louvain" in expected_query
            assert result == self.PARSED_RESULT_SET

    def test_louvain_communities_mappable_options(self, mock_graph):
        """Test the common options between NetworkX and Neptune Analytics."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = louvain_communities(
                mock_graph,
                resolution=1,
                seed=None,
                # NX mappable parameters
                weight="customer_weight",
                max_level=100,
                threshold=0.5,
            )

            # Verify the correct query was built and executed
            parameters = {
                "maxLevels": 100,
                "iterationTolerance": 0.5,
                "edgeWeightProperty": "customer_weight",
                "edgeWeightType": "float",
            }

            (expected_query, param_values) = louvain_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.louvain" in expected_query

            assert result == self.PARSED_RESULT_SET

    def test_louvain_communities_nx_options(self, mock_graph):
        """Test the Neptune Analytics specific options."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = louvain_communities(
                mock_graph,
                # Default para from NX
                weight="weight",
                resolution=1,
                threshold=0.0000001,
                max_level=None,
                seed=None,
                # AWS options
                edge_weight_property="customer_weight",
                edge_weight_type="int",
                concurrency=0,
                max_iterations=600,
                edge_labels=["test_labels"],
                level_tolerance=90,
            )

            # Verify the correct query was built and executed
            parameters = {
                "iterationTolerance": 0.0000001,
                "edgeWeightProperty": "customer_weight",
                "edgeWeightType": "int",
                "concurrency": 0,
                "maxIterations": 600,
                "edgeLabels": ["test_labels"],
                "levelTolerance": 90,
            }

            (expected_query, param_values) = louvain_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.louvain" in expected_query

            assert result == self.PARSED_RESULT_SET

    def test_louvain_communities_mutate(self, mock_graph):
        """Test mutate variant of louvain_communities."""
        # Set up the environment
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = louvain_communities(
                mock_graph,
                # Default para from NX
                weight="weight",
                resolution=1,
                threshold=0.0000001,
                max_level=None,
                seed=None,
                write_property="communities",
            )

            # Verify the correct query was built and executed
            parameters = {
                "iterationTolerance": 0.0000001,
                "writeProperty": "communities",
            }

            (expected_query, param_values) = louvain_mutation_query(parameters)

            # No conversion should happen if method receiving networkX default.
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            assert "neptune.algo.louvain.mutate" in expected_query
            assert result == {}
