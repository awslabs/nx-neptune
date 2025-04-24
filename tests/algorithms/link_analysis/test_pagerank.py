import pytest
from unittest.mock import MagicMock, patch

from nx_neptune.clients import pagerank_query
from nx_neptune.clients.neptune_constants import (
    PARAM_DAMPING_FACTOR,
    PARAM_NUM_OF_ITERATIONS,
    PARAM_TOLERANCE,
)
from nx_neptune import NeptuneGraph
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
        mock_graph.execute_call.assert_called_once_with(expected_query, param_values)
        assert "neptune.algo.pageRank" in expected_query

        # Verify the result contains the expected nodes with their PageRank values
        assert result == {"A": 0.3, "B": 0.2, "C": 0.5}

    def test_pagerank_with_alpha(self, mock_graph):
        """Test pagerank with custom alpha parameter (0.75)."""
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
        mock_graph.execute_call.assert_called_once_with(expected_query, param_values)
        assert "neptune.algo.pageRank" in expected_query
        assert f"{PARAM_DAMPING_FACTOR}:{damping_factor}" in expected_query

        # Verify the result
        assert result == {"A": 0.3, "B": 0.2, "C": 0.5}

    def test_pagerank_with_max_iter(self, mock_graph):
        """Test pagerank with custom max_iter parameter (50)."""
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
        mock_graph.execute_call.assert_called_once_with(expected_query, param_values)
        assert "neptune.algo.pageRank" in expected_query
        assert f"{PARAM_NUM_OF_ITERATIONS}:{num_of_iterations}" in expected_query

        # Verify the result
        assert result == {"A": 0.3, "B": 0.2, "C": 0.5}

    def test_pagerank_with_tolerance(self, mock_graph):
        """Test pagerank with custom tolerance parameter (1e-04)."""
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
        mock_graph.execute_call.assert_called_once_with(expected_query, param_values)

        # Verify the result
        assert result == {"A": 0.3, "B": 0.2, "C": 0.5}

    def test_pagerank_empty_result(self, mock_graph):
        """
        Test pagerank when no results are returned,
        in the of method being called with networkX default value,
        no additional option should be passed as part of the openCypher call.
        """
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
        assert mock_logger.warning.call_count == 4

        # Common warning message suffix
        warning_suffix = (
            " parameter is not supported in Neptune Analytics implementation. "
            "This argument will be ignored and execution will proceed without it."
        )

        # Check specific warning messages
        mock_logger.warning.assert_any_call(f"'personalization'{warning_suffix}")
        mock_logger.warning.assert_any_call(f"'nstart'{warning_suffix}")
        mock_logger.warning.assert_any_call(f"'weight'{warning_suffix}")
        mock_logger.warning.assert_any_call(f"'dangling'{warning_suffix}")

        # Verify the result is still correct
        assert result == {"A": 0.3, "B": 0.2, "C": 0.5}
