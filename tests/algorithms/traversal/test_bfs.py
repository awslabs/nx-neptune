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

import networkx
import pytest
from unittest.mock import MagicMock, patch

from resources_management.clients import (
    bfs_query,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_TRAVERSAL_DIRECTION_BOTH,
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    PARAM_MAX_DEPTH,
)
from nx_neptune import NeptuneGraph
from nx_neptune.algorithms.traversal.bfs import (
    bfs_edges,
    descendants_at_distance,
    bfs_layers,
)
from resources_management.clients.neptune_constants import (
    PARAM_VERTEX_LABEL,
    PARAM_EDGE_LABELS,
    PARAM_CONCURRENCY,
)
from resources_management.clients.opencypher_builder import (
    descendants_at_distance_query,
    bfs_layers_query,
)


class TestBfsEdges:
    """Test suite for the bfs_edges function in nx_neptune."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph.execute_call.return_value = [
            {
                "node": {"~id": "A", "~properties": {"name": "A-name"}},
                "parent": {"~id": "A", "~properties": {"name": "A-name"}},
            },
            {
                "node": {"~id": "B", "~properties": {"name": "B-name"}},
                "parent": {"~id": "A", "~properties": {"name": "A-name"}},
            },
            {
                "node": {"~id": "C", "~properties": {"name": "C-name"}},
                "parent": {"~id": "A", "~properties": {"name": "A-name"}},
            },
        ]
        graph.traversal_direction.return_value = "both"
        return graph

    @pytest.fixture
    def mock_digraph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph.execute_call.return_value = [
            {
                "node": {
                    "~id": "A",
                    "~labels": ["Node"],
                    "~properties": {"name": "A-name"},
                },
                "parent": {
                    "~id": "A",
                    "~labels": ["Node"],
                    "~properties": {"name": "A-name"},
                },
            },
            {
                "node": {
                    "~id": "B",
                    "~labels": ["Node"],
                    "~properties": {"name": "B-name"},
                },
                "parent": {
                    "~id": "A",
                    "~labels": ["Node"],
                    "~properties": {"name": "A-name"},
                },
            },
            {
                "node": {
                    "~id": "C",
                    "~labels": ["Node"],
                    "~properties": {"name": "C-name"},
                },
                "parent": {
                    "~id": "A",
                    "~labels": ["Node"],
                    "~properties": {"name": "A-name"},
                },
            },
        ]
        # TODO fix
        graph.traversal_direction.return_value = '"both"'
        graph.traversal_direction.side_effect = lambda r: (
            "inbound" if r else "outbound"
        )
        return graph

    @pytest.fixture
    def mock_distance_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph.execute_call.return_value = [
            {"id(node)": "Alice"},
            {"id(node)": "Bob"},
        ]
        return graph

    @pytest.fixture
    def mock_bfs_layers_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph.execute_call.return_value = [
            {"id": ["4", "1"], "level": 0},
            {"id": ["0", "3", "2"], "level": 1},
        ]
        return graph

    def test_bfs_edges_basic(self, mock_graph):
        """Test basic functionality of bfs_edges."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"

            # Execute
            result = list(bfs_edges(mock_graph, source))

            # Verify the correct query was built and executed
            source_node = "n"
            where_filters = {"id(n)": source}
            parameters = {PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTION_BOTH}
            (expected_query, param_values) = bfs_query(
                source_node, where_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.bfs.parents" in expected_query
            assert (
                f'{PARAM_TRAVERSAL_DIRECTION}:"{PARAM_TRAVERSAL_DIRECTION_BOTH}"'
                in expected_query
            )

            # Verify the result contains the expected nodes
            assert result == [["A", "B"], ["A", "C"]]

    def test_bfs_edges_with_reverse(self, mock_digraph):
        """Test bfs_edges with reverse parameter."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"

            # Execute
            result = list(bfs_edges(mock_digraph, source, reverse=True))

            # Verify the correct query was built and executed
            source_node = "n"
            where_filters = {"id(n)": source}
            parameters = {PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTION_INBOUND}
            (expected_query, param_values) = bfs_query(
                source_node, where_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            assert "neptune.algo.bfs.parents" in expected_query
            assert (
                f'{PARAM_TRAVERSAL_DIRECTION}:"{PARAM_TRAVERSAL_DIRECTION_INBOUND}"'
                in expected_query
            )
            mock_digraph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify the result contains the expected nodes
            assert result == [["A", "B"], ["A", "C"]]

    def test_bfs_edges_with_depth_limit(self, mock_digraph):
        """Test bfs_edges with depth_limit parameter."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"
            depth_limit = 2

            # Execute
            result = list(bfs_edges(mock_digraph, source, depth_limit=depth_limit))

            # Verify the correct query was built and executed
            source_node = "n"
            where_filters = {"id(n)": source}
            parameters = {
                PARAM_MAX_DEPTH: depth_limit,
                PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
            }
            (expected_query, param_values) = bfs_query(
                source_node, where_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            assert "neptune.algo.bfs.parents" in expected_query
            assert (
                f'{PARAM_TRAVERSAL_DIRECTION}:"{PARAM_TRAVERSAL_DIRECTION_OUTBOUND}"'
                in expected_query
            )
            assert f"{PARAM_MAX_DEPTH}:{depth_limit}" in expected_query
            mock_digraph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify the result contains the expected nodes
            assert result == [["A", "B"], ["A", "C"]]

    def test_bfs_edges_with_sort_neighbors(self, mock_graph):
        """Test bfs_edges with sort_neighbors parameter."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"
            sort_neighbors = True

            # Execute
            result = list(bfs_edges(mock_graph, source, sort_neighbors=sort_neighbors))

            # Verify the correct query was built and executed
            source_node = "n"
            where_filters = {"id(n)": source}
            parameters = {
                # Note: sort_neighbours is not used in the query
                PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTION_BOTH,
            }
            (expected_query, param_values) = bfs_query(
                source_node, where_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            assert "neptune.algo.bfs.parents" in expected_query
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            assert result == [["A", "B"], ["A", "C"]]

    def test_bfs_edges_empty_result(self, mock_graph):
        """Test bfs_edges when no results are returned."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            mock_graph.execute_call.return_value = []
            source = "A"

            # Execute
            result = list(bfs_edges(mock_graph, source))

            # Verify the result is an empty list
            assert result == []

    @patch("nx_neptune.algorithms.util.algorithm_utils.logger")
    def test_bfs_edges_unsupported_parameters_warning(self, mock_logger, mock_graph):
        """Test basic functionality of bfs_edges."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"

            # Execute
            result = list(bfs_edges(mock_graph, source=source, sort_neighbors="test"))

            # Verify warnings were logged for each unsupported parameter
            assert mock_logger.warning.call_count == 1

            # Common warning message suffix
            warning_suffix = (
                " parameter is not supported in Neptune Analytics implementation. "
                "This argument will be ignored and execution will proceed without it."
            )

            # Check specific warning messages
            mock_logger.warning.assert_any_call(f"'sort_neighbors'{warning_suffix}")

            # Verify the result contains the expected nodes
            assert result == [["A", "B"], ["A", "C"]]

    def test_bfs_edges_with_na_parameters(self, mock_graph):
        """Test basic functionality of bfs_edges."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"

            # Execute
            result = list(
                bfs_edges(
                    mock_graph,
                    source=source,
                    vertex_label="A",
                    edge_labels=["RELATES_TO"],
                    concurrency=0,
                )
            )

            # Verify the correct query was built and executed
            source_node = "n"
            where_filters = {"id(n)": source}
            parameters = {
                # Note: sort_neighbours is not used in the query
                PARAM_TRAVERSAL_DIRECTION: PARAM_TRAVERSAL_DIRECTION_BOTH,
                PARAM_VERTEX_LABEL: "A",
                PARAM_EDGE_LABELS: ["RELATES_TO"],
                PARAM_CONCURRENCY: 0,
            }
            (expected_query, param_values) = bfs_query(
                source_node, where_filters, parameters
            )

            # Verify the function called execute_call with correct parameters
            mock_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )

            # Verify the result contains the expected nodes
            assert result == [["A", "B"], ["A", "C"]]

    def test_descendants_at_distance_base(self, mock_distance_graph):
        """Test basic functionality of descendants_at_distance."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"
            distance = 1

            # Execute
            result = descendants_at_distance(mock_distance_graph, source, distance)

            # Verify the correct query was built and executed
            source_node = "n"
            where_filters = {f"id({source_node})": source}
            parameters = {"maxDepth": distance}
            (expected_query, param_values) = descendants_at_distance_query(
                source_node, where_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            mock_distance_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.bfs.levels" in expected_query

            # Verify the result contains the expected nodes
            assert result == {"Alice", "Bob"}

    def test_descendants_at_distance_na_parameters(self, mock_distance_graph):
        """Test descendants_at_distance with Neptune Analytics parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = "A"
            distance = 1

            # Execute
            result = descendants_at_distance(
                mock_distance_graph,
                source,
                distance,
                vertex_label="A",
                edge_labels=["RELATES_TO"],
                concurrency=0,
            )

            # Verify the correct query was built and executed
            source_node = "n"
            where_filters = {f"id({source_node})": source}
            parameters = {
                "maxDepth": distance,
                PARAM_VERTEX_LABEL: "A",
                PARAM_EDGE_LABELS: ["RELATES_TO"],
                PARAM_CONCURRENCY: 0,
            }
            (expected_query, param_values) = descendants_at_distance_query(
                source_node, where_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            mock_distance_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.bfs.levels" in expected_query

            # Verify the result contains the expected nodes
            assert result == {"Alice", "Bob"}

    def test_bfs_layers_single_source(self, mock_bfs_layers_graph):
        """Test basic functionality of descendants_at_distance."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = ["A"]

            # Execute
            result = list(bfs_layers(mock_bfs_layers_graph, source))

            # Verify the correct query was built and executed
            source_node = "n"
            where_in_filters = {f"id({source_node})": source}
            parameters = {}
            (expected_query, param_values) = bfs_layers_query(
                source_node, where_in_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            mock_bfs_layers_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.bfs.levels" in expected_query

            # Verify the result contains the expected nodes
            assert result == [["4", "1"], ["0", "3", "2"]]

    def test_bfs_layers_multiple_sources(self, mock_bfs_layers_graph):
        """Test basic functionality of descendants_at_distance."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            source = ["A", "B"]

            # Execute
            result = list(bfs_layers(mock_bfs_layers_graph, source))

            # Verify the correct query was built and executed
            source_node = "n"
            where_in_filters = {f"id({source_node})": source}
            parameters = {}
            (expected_query, param_values) = bfs_layers_query(
                source_node, where_in_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            mock_bfs_layers_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.bfs.levels" in expected_query

            # Verify the result contains the expected nodes
            assert result == [["4", "1"], ["0", "3", "2"]]

    def test_bfs_layers_na_parameters(self, mock_bfs_layers_graph):
        """Test basic functionality of descendants_at_distance."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            sources = ["A", "B"]

            # Execute
            result = list(
                bfs_layers(
                    mock_bfs_layers_graph,
                    sources,
                    vertex_label="A",
                    edge_labels=["RELATES_TO"],
                    concurrency=0,
                )
            )

            # Verify the correct query was built and executed
            source_node = "n"
            where_in_filters = {f"id({source_node})": sources}
            parameters = {
                PARAM_VERTEX_LABEL: "A",
                PARAM_EDGE_LABELS: ["RELATES_TO"],
                PARAM_CONCURRENCY: 0,
            }
            (expected_query, param_values) = bfs_layers_query(
                source_node, where_in_filters, parameters
            )

            # Verify the function called execute_algo_bfs with correct parameters
            mock_bfs_layers_graph.execute_call.assert_called_once_with(
                expected_query, param_values
            )
            assert "neptune.algo.bfs.levels" in expected_query

            # Verify the result contains the expected nodes
            assert result == [["4", "1"], ["0", "3", "2"]]
