import os
from unittest.mock import MagicMock, patch

import networkx as nx

from nx_neptune.interface import ALGORITHMS, BackendInterface, assign_algorithms
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.clients import Edge, Node


class TestBackendInterface:
    def test_convert_from_nx_undirected(self):
        """Test converting from NetworkX graph to NeptuneGraph"""
        # Create a simple NetworkX graph
        G = nx.Graph()
        G.add_node("Alice")
        G.add_node("Bob")
        G.add_edge("Alice", "Bob")

        node_alice = Node(labels=["Node"], properties={"name": "Alice"})
        node_bob = Node(labels=["Node"], properties={"name": "Bob"})

        # Mock the NeptuneGraph to avoid actual AWS calls
        with patch("nx_neptune.interface.NeptuneGraph") as mock_neptune_graph:
            # Setup
            mock_instance = MagicMock()
            mock_neptune_graph.return_value = mock_instance

            # Call convert_from_nx
            result = BackendInterface.convert_from_nx(G)

            # Verify NeptuneGraph was created with the correct graph
            mock_neptune_graph.assert_called_once_with(graph=G)

            # Verify add_node was called for each node
            assert mock_instance.add_node.call_count == 2

            mock_instance.add_node.assert_any_call(node_alice)
            mock_instance.add_node.assert_any_call(node_bob)
            mock_instance.add_edge.assert_any_call(
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=node_alice,
                    node_dest=node_bob,
                )
            )
            mock_instance.add_edge.assert_any_call(
                Edge(
                    label="FRIEND_WITH",
                    properties={},
                    node_src=node_bob,
                    node_dest=node_alice,
                )
            )

            # Verify the result is the mock instance
            assert result == mock_instance

    def test_convert_from_nx_undirected(self):
        """Test converting from NetworkX graph to NeptuneGraph"""
        # Create a simple NetworkX graph
        G = nx.DiGraph()
        G.add_node("Alice")
        G.add_node("Bob")
        G.add_edge("Alice", "Bob")

        node_alice = Node(id="Alice", labels=["Node"], properties={})
        node_bob = Node(id="Bob", labels=["Node"], properties={})

        # Mock the NeptuneGraph to avoid actual AWS calls
        with patch("nx_neptune.interface.NeptuneGraph") as mock_neptune_graph:
            # Setup
            mock_instance = MagicMock()
            mock_neptune_graph.return_value = mock_instance

            # Call convert_from_nx
            result = BackendInterface.convert_from_nx(G)

            # Verify NeptuneGraph was created with the correct graph
            mock_neptune_graph.assert_called_once_with(graph=G)

            # Verify add_node was called for each node
            assert mock_instance.add_node.call_count == 2

            mock_instance.add_node.assert_any_call(node_alice)
            mock_instance.add_node.assert_any_call(node_bob)
            mock_instance.add_edge.assert_called_once_with(
                Edge(
                    label="RELATES_TO",
                    properties={},
                    node_src=node_alice,
                    node_dest=node_bob,
                )
            )

            # Verify the result is the mock instance
            assert result == mock_instance

    def test_convert_to_nx_with_neptune_graph(self):
        """Test converting from NeptuneGraph to NetworkX graph"""
        # Create a mock NeptuneGraph
        mock_neptune_graph = MagicMock(spec=NeptuneGraph)
        mock_graph_object = nx.Graph()
        mock_neptune_graph.graph_object.return_value = mock_graph_object

        # Call the method
        result = BackendInterface.convert_to_nx(mock_neptune_graph)

        # Verify the result is the graph_object from the NeptuneGraph
        assert result == mock_graph_object

    def test_convert_to_nx_with_nx_graph(self):
        """Test converting when input is already a NetworkX graph"""
        # Create a NetworkX graph
        G = nx.Graph()

        # Call the method
        result = BackendInterface.convert_to_nx(G)

        # Verify the result is the same graph
        assert result == G

    def test_assign_algorithms(self):
        """Test the assign_algorithms decorator"""

        # Create a test class
        class TestClass:
            pass

        # Apply the decorator
        decorated_class = assign_algorithms(TestClass)

        # Verify all algorithms from ALGORITHMS are assigned to the class
        for algo in ALGORITHMS:
            func_name = algo.rsplit(".", 1)[-1]
            assert hasattr(decorated_class, func_name)

            # Verify the attribute is the correct function from the algorithms module
            from nx_neptune import algorithms

            expected_func = getattr(algorithms, func_name)
            actual_func = getattr(decorated_class, func_name)
            assert actual_func == expected_func
