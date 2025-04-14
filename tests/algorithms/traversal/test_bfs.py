import networkx
import pytest
from unittest.mock import MagicMock, patch

from nx_neptune import NeptuneGraph, configure_if_nx_active
from nx_neptune.algorithms.traversal.bfs import bfs_edges


class TestBfsEdges:
    """Test suite for the bfs_edges function in nx_neptune."""

    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph = MagicMock(spec=NeptuneGraph)
        # Mock the execute_algo_bfs method to return a predefined result
        graph.execute_algo_bfs.return_value = [
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
        # Mock the execute_algo_bfs method to return a predefined result
        graph.execute_algo_bfs.return_value = [
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
        graph.traversal_direction.return_value = '"both"'
        graph.traversal_direction.side_effect = lambda r: (
            '"inbound"' if r else '"outbound"'
        )
        return graph

    def test_bfs_edges_basic(self, mock_graph):
        """Test basic functionality of bfs_edges."""
        source = "A"
        result = list(bfs_edges(mock_graph, source))

        # Verify the function called execute_algo_bfs with correct parameters
        mock_graph.execute_algo_bfs.assert_called_once_with(
            "n", {"n.name": "A"}, {"traversalDirection": '"both"'}
        )

        # Verify the result contains the expected nodes
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_with_reverse(self, mock_digraph):
        """Test bfs_edges with reverse parameter."""
        source = "A"
        result = list(bfs_edges(mock_digraph, source, reverse=True))

        # TODO: the reverse parameter is not implemented in the function
        mock_digraph.execute_algo_bfs.assert_called_once_with(
            "n", {"n.name": "A"}, {"traversalDirection": '"inbound"'}
        )
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_with_depth_limit(self, mock_digraph):
        """Test bfs_edges with depth_limit parameter."""
        source = "A"
        result = list(bfs_edges(mock_digraph, source, depth_limit=2))

        # TODO: the depth_limit parameter is not implemented in the function
        mock_digraph.execute_algo_bfs.assert_called_once_with(
            "n", {"n.name": "A"}, {"traversalDirection": '"outbound"', "maxDepth": 2}
        )
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_with_sort_neighbors(self, mock_graph):
        """Test bfs_edges with sort_neighbors parameter."""
        source = "A"
        result = list(bfs_edges(mock_graph, source, sort_neighbors=True))

        # TODO: the sort_neighbors parameter is not implemented in the function
        mock_graph.execute_algo_bfs.assert_called_once_with(
            "n", {"n.name": "A"}, {"traversalDirection": '"both"'}
        )
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_with_all_parameters(self, mock_graph):
        """Test bfs_edges with all parameters specified."""
        source = "A"
        result = list(
            bfs_edges(
                mock_graph, source, reverse=True, depth_limit=3, sort_neighbors=True
            )
        )

        # Verify the function called execute_algo_bfs with correct parameters
        mock_graph.execute_algo_bfs.assert_called_once_with(
            "n", {"n.name": "A"}, {"traversalDirection": '"both"', "maxDepth": 3}
        )
        assert result == [("A", "B"), ("A", "C")]

    def test_bfs_edges_empty_result(self, mock_graph):
        """Test bfs_edges when no results are returned."""
        mock_graph.execute_algo_bfs.return_value = []
        source = "A"
        result = list(bfs_edges(mock_graph, source))

        # Verify the result is an empty list
        assert result == []
