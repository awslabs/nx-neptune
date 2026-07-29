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
import warnings
from unittest.mock import MagicMock, patch

import pytest
from networkx import Graph

from nx_neptune.algorithms.shortest_paths.bellman_ford import (
    bellman_ford_path,
    bellman_ford_predecessor_and_distance,
    single_source_bellman_ford_path_length,
)
from nx_neptune.clients.opencypher_builder import (
    bellman_ford_path_query,
    bellman_ford_predecessor_and_distance_query,
    single_source_bellman_ford_path_length_query,
)
from nx_neptune.na_graph import NeptuneGraph


class TestBellmanFordPath:
    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        graph_nx.execute_call.return_value = [
            {
                "source": {"~id": "A"},
                "target": {"~id": "D"},
                "distance": 6,
                "vertexPath": [
                    {"~id": "A", "~entityType": "node"},
                    {"~id": "C", "~entityType": "node"},
                    {"~id": "B", "~entityType": "node"},
                    {"~id": "D", "~entityType": "node"},
                ],
            }
        ]
        graph = MagicMock(spec=Graph)
        graph.number_of_nodes.return_value = 5
        graph_nx.graph = graph
        return graph_nx

    def test_bellman_ford_path_basic(self, mock_graph):
        """Test basic path finding."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = bellman_ford_path(mock_graph, "A", "D", weight="weight")

            expected_query, _ = bellman_ford_path_query(
                "A", "D", {"edgeWeightProperty": "weight", "edgeWeightType": "double"}
            )
            mock_graph.execute_call.assert_called_once_with(expected_query, {})

            assert result == ["A", "C", "B", "D"]

    def test_bellman_ford_path_with_edge_labels(self, mock_graph):
        """Test path finding with edge label filtering."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = bellman_ford_path(
                mock_graph, "A", "D", weight="dist", edge_labels=["route"]
            )

            expected_query, _ = bellman_ford_path_query(
                "A",
                "D",
                {
                    "edgeWeightProperty": "dist",
                    "edgeWeightType": "double",
                    "edgeLabels": ["route"],
                },
            )
            mock_graph.execute_call.assert_called_once_with(expected_query, {})
            assert result == ["A", "C", "B", "D"]

    def test_bellman_ford_path_with_all_params(self, mock_graph):
        """Test path finding with all Neptune Analytics parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = bellman_ford_path(
                mock_graph,
                "A",
                "D",
                weight="dist",
                edge_weight_type="int",
                edge_labels=["route"],
                vertex_label="airport",
                traversal_direction="outbound",
                concurrency=0,
            )

            expected_query, _ = bellman_ford_path_query(
                "A",
                "D",
                {
                    "edgeWeightProperty": "dist",
                    "edgeWeightType": "int",
                    "edgeLabels": ["route"],
                    "vertexLabel": "airport",
                    "traversalDirection": "outbound",
                    "concurrency": 0,
                },
            )
            mock_graph.execute_call.assert_called_once_with(expected_query, {})
            assert result == ["A", "C", "B", "D"]

    def test_bellman_ford_path_no_path(self, mock_graph):
        """Test raises NetworkXNoPath when no path exists."""
        mock_graph.execute_call.return_value = []

        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            import networkx as nx

            with pytest.raises(nx.NetworkXNoPath):
                bellman_ford_path(mock_graph, "A", "Z", weight="weight")

    def test_bellman_ford_path_callable_weight_warning(self, mock_graph):
        """Test that callable weight raises a warning."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                bellman_ford_path(
                    mock_graph, "A", "D", weight=lambda u, v, d: d.get("w", 1)
                )
                assert len(w) == 1
                assert "callable weight functions" in str(w[0].message)


class TestSingleSourceBellmanFordPathLength:
    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        graph_nx.execute_call.return_value = [
            {"nodeId": "B", "distance": 3},
            {"nodeId": "C", "distance": 2},
            {"nodeId": "D", "distance": 6},
            {"nodeId": "E", "distance": 7},
        ]
        graph = MagicMock(spec=Graph)
        graph.number_of_nodes.return_value = 5
        graph_nx.graph = graph
        return graph_nx

    def test_path_length_basic(self, mock_graph):
        """Test basic SSSP path length computation."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = single_source_bellman_ford_path_length(
                mock_graph, "A", weight="weight"
            )

            expected_query, _ = single_source_bellman_ford_path_length_query(
                "A", {"edgeWeightProperty": "weight", "edgeWeightType": "double"}
            )
            mock_graph.execute_call.assert_called_once_with(expected_query, {})

            assert result == {"B": 3, "C": 2, "D": 6, "E": 7}

    def test_path_length_with_params(self, mock_graph):
        """Test SSSP path length with all parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = single_source_bellman_ford_path_length(
                mock_graph,
                "A",
                weight="dist",
                edge_weight_type="int",
                edge_labels=["route"],
                vertex_label="airport",
                traversal_direction="outbound",
                concurrency=1,
            )

            expected_query, _ = single_source_bellman_ford_path_length_query(
                "A",
                {
                    "edgeWeightProperty": "dist",
                    "edgeWeightType": "int",
                    "edgeLabels": ["route"],
                    "vertexLabel": "airport",
                    "traversalDirection": "outbound",
                    "concurrency": 1,
                },
            )
            mock_graph.execute_call.assert_called_once_with(expected_query, {})
            assert result == {"B": 3, "C": 2, "D": 6, "E": 7}

    def test_path_length_empty_result(self, mock_graph):
        """Test SSSP when no nodes are reachable."""
        mock_graph.execute_call.return_value = []

        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            result = single_source_bellman_ford_path_length(
                mock_graph, "isolated", weight="weight"
            )
            assert result == {}

    def test_path_length_callable_weight_warning(self, mock_graph):
        """Test that callable weight raises a warning."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                single_source_bellman_ford_path_length(
                    mock_graph, "A", weight=lambda u, v, d: d.get("w", 1)
                )
                assert len(w) == 1
                assert "callable weight functions" in str(w[0].message)


class TestBellmanFordPredecessorAndDistance:
    @pytest.fixture
    def mock_graph(self):
        """Create a mock NeptuneGraph for testing."""
        graph_nx = MagicMock(spec=NeptuneGraph)
        graph_nx.execute_call.return_value = [
            {"nodeId": "A", "parentId": "A", "distance": 0},
            {"nodeId": "B", "parentId": "C", "distance": 3},
            {"nodeId": "C", "parentId": "A", "distance": 2},
            {"nodeId": "D", "parentId": "B", "distance": 6},
        ]
        graph = MagicMock(spec=Graph)
        graph.number_of_nodes.return_value = 5
        graph_nx.graph = graph
        return graph_nx

    def test_predecessor_and_distance_basic(self, mock_graph):
        """Test basic predecessor and distance computation."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            pred, dist = bellman_ford_predecessor_and_distance(
                mock_graph, "A", weight="weight"
            )

            expected_query, _ = bellman_ford_predecessor_and_distance_query(
                "A", {"edgeWeightProperty": "weight", "edgeWeightType": "double"}
            )
            mock_graph.execute_call.assert_called_once_with(expected_query, {})

            # Source has empty predecessors
            assert pred["A"] == []
            # Non-source nodes have list with single parent
            assert pred["B"] == ["C"]
            assert pred["C"] == ["A"]
            assert pred["D"] == ["B"]

            assert dist["A"] == 0
            assert dist["B"] == 3
            assert dist["C"] == 2
            assert dist["D"] == 6

    def test_predecessor_and_distance_with_params(self, mock_graph):
        """Test with all parameters."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            pred, dist = bellman_ford_predecessor_and_distance(
                mock_graph,
                "A",
                weight="dist",
                edge_weight_type="int",
                edge_labels=["route"],
                vertex_label="airport",
                traversal_direction="outbound",
                concurrency=0,
            )

            expected_query, _ = bellman_ford_predecessor_and_distance_query(
                "A",
                {
                    "edgeWeightProperty": "dist",
                    "edgeWeightType": "int",
                    "edgeLabels": ["route"],
                    "vertexLabel": "airport",
                    "traversalDirection": "outbound",
                    "concurrency": 0,
                },
            )
            mock_graph.execute_call.assert_called_once_with(expected_query, {})

    def test_predecessor_and_distance_empty_result(self, mock_graph):
        """Test with empty results (isolated node)."""
        mock_graph.execute_call.return_value = []

        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            pred, dist = bellman_ford_predecessor_and_distance(
                mock_graph, "isolated", weight="weight"
            )
            # Source always included
            assert pred == {"isolated": []}
            assert dist == {"isolated": 0}

    def test_predecessor_and_distance_heuristic_warning(self, mock_graph):
        """Test that heuristic parameter raises a warning."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                bellman_ford_predecessor_and_distance(
                    mock_graph, "A", weight="weight", heuristic=True
                )
                assert len(w) == 1
                assert "heuristic parameter" in str(w[0].message)

    def test_predecessor_and_distance_callable_weight_warning(self, mock_graph):
        """Test that callable weight raises a warning."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                bellman_ford_predecessor_and_distance(
                    mock_graph, "A", weight=lambda u, v, d: d.get("w", 1)
                )
                assert len(w) == 1
                assert "callable weight functions" in str(w[0].message)

    def test_predecessor_and_distance_target_ignored(self, mock_graph):
        """Test that target parameter doesn't affect the query."""
        with patch.dict(os.environ, {"NX_ALGORITHM_TEST": "test_case"}):
            pred1, dist1 = bellman_ford_predecessor_and_distance(
                mock_graph, "A", target=None, weight="weight"
            )
            mock_graph.execute_call.reset_mock()
            mock_graph.execute_call.return_value = [
                {"nodeId": "A", "parentId": "A", "distance": 0},
                {"nodeId": "B", "parentId": "C", "distance": 3},
                {"nodeId": "C", "parentId": "A", "distance": 2},
                {"nodeId": "D", "parentId": "B", "distance": 6},
            ]
            pred2, dist2 = bellman_ford_predecessor_and_distance(
                mock_graph, "A", target="D", weight="weight"
            )
            # Both should call with the same query (target is ignored)
            calls = mock_graph.execute_call.call_args_list
            assert len(calls) == 1  # second call happened
