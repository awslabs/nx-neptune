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
"""
Tests for the na_models.py module.
"""

import pytest

from nx_neptune.clients.na_models import (
    Node,
    Edge,
    DEFAULT_NODE_LABEL_TYPE,
    DEFAULT_EDGE_RELATIONSHIP,
)


class TestNode:
    def test_node_init(self):
        """Test Node initialization with different parameters."""
        # Test with all parameters
        node = Node(
            id="123", labels=["Person"], properties={"name": "Alice", "age": 30}
        )
        assert node.id == "123"
        assert node.labels == ["Person"]
        assert node.properties == {"name": "Alice", "age": 30}

        # Test with default parameters
        node = Node(id="empty")
        assert node.id == "empty"
        assert node.labels == []
        assert node.properties == {}

    def test_convert_from_nx_with_tuple(self):
        """Test converting from NetworkX node representation (tuple format)."""
        nx_node = ("Alice", {"age": 30, "city": "New York"})
        node = Node.convert_from_nx(nx_node)

        assert node.id == "Alice"
        assert node.labels == [DEFAULT_NODE_LABEL_TYPE]
        assert node.properties == {
            "age": 30,
            "city": "New York",
        }

        # Test with custom labels
        custom_labels = ["Person", "Employee"]
        node = Node.convert_from_nx(nx_node, labels=custom_labels)
        assert node.labels == custom_labels

    def test_convert_from_nx_with_scalar(self):
        """Test converting from NetworkX node representation (scalar format)."""
        nx_node = "Alice"
        node = Node.convert_from_nx(nx_node)

        assert node.id == "Alice"
        assert node.labels == [DEFAULT_NODE_LABEL_TYPE]
        assert node.properties == {}

    def test_from_neptune_response(self):
        """Test creating a Node from Neptune response JSON."""
        json_response = {
            "~id": "node-123",
            "~labels": ["Person", "Employee"],
            "~properties": {"name": "Alice", "age": 30},
        }

        node = Node.from_neptune_response(json_response)

        assert node.id == "node-123"
        assert node.labels == ["Person", "Employee"]
        assert node.properties == {"name": "Alice", "age": 30}

    def test_eq(self):
        """Test the equality operator."""
        node1 = Node(id="123", labels=["Person"], properties={"name": "Alice"})
        node2 = Node(id="123", labels=["Employee"], properties={"name": "Bob"})
        node3 = Node(id="456", labels=["Person"], properties={"name": "Alice"})

        # Same ID should be equal
        assert node1 == node2

        # Different ID and different properties should not be equal
        assert node1 != node3

        # Comparison with non-Node object
        assert node1 != "not a node"

    def test_repr(self):
        """Test the string representation."""
        node = Node(id="A", labels=["Person"], properties={"name": "Alice"})
        expected = "Node(id=A, labels=['Person'], properties={'name': 'Alice'})"
        assert repr(node) == expected


class TestEdge:
    def test_edge_init(self):
        """Test Edge initialization with different parameters."""
        src_node = Node(id="A", labels=["Person"], properties={"name": "Alice"})
        dest_node = Node(id="ACME", labels=["Company"], properties={"name": "ACME"})

        # Test with all parameters
        edge = Edge(
            node_src=src_node,
            node_dest=dest_node,
            label="WORKS_AT",
            properties={"since": 2020},
            is_directed=True,
        )

        assert edge.node_src == src_node
        assert edge.node_dest == dest_node
        assert edge.label == "WORKS_AT"
        assert edge.properties == {"since": 2020}
        assert edge.is_directed is True

        # Test with default parameters
        edge = Edge(node_src=src_node, node_dest=dest_node)
        assert edge.label == ""
        assert edge.properties == {}
        assert edge.is_directed is True

    def test_edge_init_validation(self):
        """Test Edge initialization validation."""
        src_node = Node(id="A", labels=["Person"], properties={"name": "Alice"})
        dest_node = Node(id="ACME", labels=["Company"], properties={"name": "ACME"})

        # Test missing source node
        with pytest.raises(
            ValueError,
            match="Edge must have both source and destination nodes specified",
        ):
            Edge(node_src=None, node_dest=dest_node)

        # Test missing destination node
        with pytest.raises(
            ValueError,
            match="Edge must have both source and destination nodes specified",
        ):
            Edge(node_src=src_node, node_dest=None)

        # Test invalid source node type
        with pytest.raises(
            TypeError, match="Edge's node_src and node_dest must be Node objects"
        ):
            Edge(node_src="not a node", node_dest=dest_node)

        # Test invalid destination node type
        with pytest.raises(
            TypeError, match="Edge's node_src and node_dest must be Node objects"
        ):
            Edge(node_src=src_node, node_dest="not a node")

    def test_convert_from_nx(self):
        """Test converting from NetworkX edge representation."""
        # NetworkX edge with properties
        nx_edge = ("Alice", "Bob", {"weight": 0.5})

        edge = Edge.convert_from_nx(nx_edge)

        assert edge.node_src.id == "Alice"
        assert edge.node_dest.id == "Bob"
        assert edge.label == DEFAULT_EDGE_RELATIONSHIP
        assert edge.properties == {"weight": 0.5}
        assert edge.is_directed is True

        # Test with custom relationship and directed flag
        edge = Edge.convert_from_nx(nx_edge, relationship="KNOWS", is_directed=False)
        assert edge.label == "KNOWS"
        assert edge.is_directed is False

        # NetworkX edge without properties
        nx_edge = ("Alice", "Bob")
        edge = Edge.convert_from_nx(nx_edge)
        assert edge.properties == {}

    def test_from_neptune_response(self):
        """Test creating an Edge from Neptune response JSON."""
        json_response = {
            "parent": {
                "~id": "node-123",
                "~labels": ["Person"],
                "~properties": {"name": "Alice"},
            },
            "node": {
                "~id": "node-456",
                "~labels": ["Person"],
                "~properties": {"name": "Bob"},
            },
        }

        edge = Edge.from_neptune_response(json_response)

        assert edge.node_src.id == "node-123"
        assert edge.node_src.properties == {"name": "Alice"}
        assert edge.node_dest.id == "node-456"
        assert edge.node_dest.properties == {"name": "Bob"}

        # Test with custom node labels
        edge = Edge.from_neptune_response(
            json_response, src_node_label="parent", dest_node_label="node"
        )
        assert edge.node_src.id == "node-123"
        assert edge.node_dest.id == "node-456"

    def test_from_neptune_response_missing_nodes(self):
        """Test error handling when nodes are missing in Neptune response."""
        # Missing source node
        json_response = {
            "node": {
                "~id": "node-456",
                "~labels": ["Person"],
                "~properties": {"name": "Bob"},
            }
        }

        with pytest.raises(ValueError, match='json response missing "parent" node'):
            Edge.from_neptune_response(json_response)

        # Missing destination node
        json_response = {
            "parent": {
                "~id": "node-123",
                "~labels": ["Person"],
                "~properties": {"name": "Alice"},
            }
        }

        with pytest.raises(ValueError, match='json response missing "node" node'):
            Edge.from_neptune_response(json_response)

    def test_to_reverse_edge(self):
        """Test creating a reversed edge."""
        src_node = Node(id="A", labels=["Person"], properties={"name": "Alice"})
        dest_node = Node(id="B", labels=["Person"], properties={"name": "Bob"})

        edge = Edge(
            node_src=src_node,
            node_dest=dest_node,
            label="KNOWS",
            properties={"since": 2020},
            is_directed=True,
        )

        reversed_edge = edge.to_reverse_edge()

        assert reversed_edge.node_src == dest_node
        assert reversed_edge.node_dest == src_node
        assert reversed_edge.label == "KNOWS"
        assert reversed_edge.properties == {"since": 2020}
        assert reversed_edge.is_directed == edge.is_directed

    def test_to_list(self):
        """Test converting edge to a tuple of node names."""
        src_node = Node(id="123", properties={"name": "Alice"})
        dest_node = Node(id="456", properties={"name": "Bob"})

        edge = Edge(node_src=src_node, node_dest=dest_node)

        assert edge.to_list() == ["123", "456"]

        # Test with nodes without name property
        src_node = Node("123")
        dest_node = Node("456")

        edge = Edge(node_src=src_node, node_dest=dest_node)

        assert edge.to_list() == ["123", "456"]

    def test_eq(self):
        """Test the equality operator."""
        src_node1 = Node("Alice")
        dest_node1 = Node("Bob")

        edge1 = Edge(node_src=src_node1, node_dest=dest_node1, label="KNOWS")
        edge2 = Edge(node_src=src_node1, node_dest=dest_node1, label="KNOWS")
        edge3 = Edge(node_src=src_node1, node_dest=dest_node1, label="FRIENDS")

        # Same nodes and label should be equal
        assert edge1 == edge2

        # Different label should not be equal
        assert edge1 != edge3

        # Comparison with non-Edge object
        assert edge1 != "not an edge"

    def test_repr(self):
        """Test the string representation."""
        src_node = Node("Alice")
        dest_node = Node("Bob")

        edge = Edge(
            node_src=src_node,
            node_dest=dest_node,
            label="KNOWS",
            properties={"since": 2020},
            is_directed=True,
        )

        expected = (
            f"Edge(label=KNOWS, properties={{'since': 2020}}, node_src={src_node}, "
            f"node_dest={dest_node}, is_directed=True)"
        )
        assert repr(edge) == expected
