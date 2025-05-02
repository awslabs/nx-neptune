import networkx
import pytest
from unittest.mock import MagicMock, patch

from nx_neptune.clients import (
    bfs_query,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_TRAVERSAL_DIRECTION_BOTH,
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    PARAM_MAX_DEPTH,
)
from nx_neptune import NeptuneGraph
from nx_neptune.algorithms.traversal.bfs import bfs_edges


class TestBfsEdges:
    """Test suite for the bfs_edges function in nx_neptune."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph.execute_call.return_value = [
            {
                "node": {"~id": "id_A", "~properties": {"name": "A"}},
                "parent": {"~id": "id_A", "~properties": {"name": "A"}},
            },
            {
                "node": {"~id": "id_B", "~properties": {"name": "B"}},
                "parent": {"~id": "id_A", "~properties": {"name": "A"}},
            },
            {
                "node": {"~id": "id_C", "~properties": {"name": "C"}},
                "parent": {"~id": "id_A", "~properties": {"name": "A"}},
            },
        ]
        graph.traversal_direction.return_value = '"both"'
        return graph

    @pytest.fixture
    def mock_digraph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_call method to return a predefined result
        graph.execute_call.return_value = [
            {
                "node": {"~labels": ["Node"], "~properties": {"name": "A"}},
                "parent": {"~labels": ["Node"], "~properties": {"name": "A"}},
            },
            {
                "node": {"~labels": ["Node"], "~properties": {"name": "B"}},
                "parent": {"~labels": ["Node"], "~properties": {"name": "A"}},
            },
            {
                "node": {"~labels": ["Node"], "~properties": {"name": "C"}},
                "parent": {"~labels": ["Node"], "~properties": {"name": "A"}},
            },
        ]
        # TODO fix
        graph.traversal_direction.return_value = '"both"'
        graph.traversal_direction.side_effect = lambda r: (
            '"inbound"' if r else '"outbound"'
        )
        return graph

    def test_bfs_edges_basic(self, mock_graph):
        """Test basic functionality of bfs_edges."""
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
        mock_graph.execute_call.assert_called_once_with(expected_query, param_values)
        assert "neptune.algo.bfs.parents" in expected_query
        assert (
            f"{PARAM_TRAVERSAL_DIRECTION}:{PARAM_TRAVERSAL_DIRECTION_BOTH}"
            in expected_query
        )

        # Verify the result contains the expected nodes
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_with_reverse(self, mock_digraph):
        """Test bfs_edges with reverse parameter."""
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
            f"{PARAM_TRAVERSAL_DIRECTION}:{PARAM_TRAVERSAL_DIRECTION_INBOUND}"
            in expected_query
        )
        mock_digraph.execute_call.assert_called_once_with(expected_query, param_values)

        # Verify the result contains the expected nodes
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_with_depth_limit(self, mock_digraph):
        """Test bfs_edges with depth_limit parameter."""
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
            f"{PARAM_TRAVERSAL_DIRECTION}:{PARAM_TRAVERSAL_DIRECTION_OUTBOUND}"
            in expected_query
        )
        assert f"{PARAM_MAX_DEPTH}:{depth_limit}" in expected_query
        mock_digraph.execute_call.assert_called_once_with(expected_query, param_values)

        # Verify the result contains the expected nodes
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_with_sort_neighbors(self, mock_graph):
        """Test bfs_edges with sort_neighbors parameter."""
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
        mock_graph.execute_call.assert_called_once_with(expected_query, param_values)

        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_empty_result(self, mock_graph):
        """Test bfs_edges when no results are returned."""
        mock_graph.execute_call.return_value = []
        source = "A"

        # Execute
        result = list(bfs_edges(mock_graph, source))

        # Verify the result is an empty list
        assert result == []
