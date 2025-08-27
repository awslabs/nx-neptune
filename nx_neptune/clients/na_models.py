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
__all__ = [
    "Node",
    "Edge",
]

from typing import Any, Dict, List, Tuple
from dataclasses import dataclass

DEFAULT_NODE_LABEL_TYPE = "Node"


class Node:
    """
    Represents a node in a graph with labels and properties.

    A node can have multiple labels and a dictionary of properties.
    This class is used to create, update, and delete nodes in Neptune Analytics.

    Attributes:
        id (str, optional): a unique identifier for the node
        labels (list): A list of labels for the node
        properties (dict): A dictionary of properties for the node
    """

    def __init__(self, id, labels=None, properties=None):
        self.id = str(id)
        self.labels = labels if labels else []
        self.properties = properties if properties else {}

    @classmethod
    def convert_from_nx(
        cls,
        node: Any | Tuple[Any, Dict[str, Any]],
        labels=None,
    ):
        if labels is None:
            labels = [DEFAULT_NODE_LABEL_TYPE]
        if isinstance(node, tuple):
            properties = node[1]
            return cls(id=node[0], labels=labels, properties=properties)
        else:
            return cls(id=node, labels=labels, properties={})

    @classmethod
    def from_neptune_response(cls, json: Dict):
        return cls(
            id=json.get("~id"),
            labels=json.get("~labels"),
            properties=json.get("~properties"),
        )

    def to_dict(self) -> Dict:
        """
        Convert node to a Dict with in the format that is compatible with UNWIND operation.
        """
        node_in_dict = self.properties.copy()
        node_in_dict["id"] = self.id
        return node_in_dict

    def to_group_by(self) -> tuple:
        """
        Return the group by key for UNWIND operation, in the form of tuple.
        """
        return tuple(self.labels)

    def __eq__(self, other):
        """
        Comparison operator of a Node

        :param other: Node to compare
        :return: (boolean) if Nodes are considered equal
        """
        if not isinstance(other, Node):
            return False
        return self.id == other.id

    def __repr__(self):
        return f"Node(id={self.id}, labels={self.labels}, properties={self.properties})"


DEFAULT_EDGE_RELATIONSHIP = "RELATES_TO"


class Edge:
    """
    Represents an edge (relationship) in a graph with an optional label and properties.

    An edge connects two nodes and can have a single label and a dictionary of properties.
    In OpenCypher, relationships can only have one lavel. This class is used
    to create, update, and delete relationships in Neptune Analytics.

    Attributes:
        node_src (Node): The source node of the edge. Must be a valid Node object.
        node_dest (Node): The destination node of the edge. Must be a valid Node object.
        label (str, optional): The label for the edge. If not provided, defaults to an empty string.
        properties (dict, optional): A dictionary of properties for the edge. Optional key-value pairs
                          that describe attributes of the relationship.
        is_directed (bool, optional): Indicates whether this is a directed edge. If not provided, defaults to True.

    Examples:
        >>> # Create an edge between two nodes with a label
        >>> src_node = Node(labels=['Person'], properties={'name': 'Alice'})
        >>> dest_node = Node(labels=['Company'], properties={'name': 'ACME'})
        >>> edge = Edge(
        ...     node_src=src_node,
        ...     node_dest=dest_node,
        ...     label='WORKS_AT',
        ...     properties={'since': '2020', 'role': 'Engineer'}
        ... )
        >>> # Create an undirected edge
        >>> undirected_edge = Edge(
        ...     node_src=src_node,
        ...     node_dest=dest_node,
        ...     label='CONNECTED_TO',
        ...     is_directed=False
        ... )
    """

    def __init__(
        self, node_src, node_dest, label=None, properties=None, is_directed=True
    ):
        """
        Initialize an Edge object.

        Args:
            node_src (Node): The source node of the edge
            node_dest (Node): The destination node of the edge
            label (str, optional): The label for the edge. If None, defaults to an empty string.
            properties (dict, optional): A dictionary of properties for the edge
            is_directed (bool, optional): Indicates whether this is a directed edge. If not provided, defaults to True.

        Raises:
            ValueError: If edge doesn't have both source and destination nodes
            TypeError: If edge's source and destination nodes are not Node objects
        """
        # Validate source and destination nodes
        if not node_src or not node_dest:
            raise ValueError(
                "Edge must have both source and destination nodes specified"
            )

        if not isinstance(node_src, Node) or not isinstance(node_dest, Node):
            raise TypeError("Edge's node_src and node_dest must be Node objects")

        self.node_src = node_src
        self.node_dest = node_dest
        self.label = label if label is not None else ""
        self.properties = properties if properties else {}
        self.is_directed = is_directed

    @classmethod
    def convert_from_nx(
        cls,
        edge: Tuple[Any, Any] | Tuple[Any, Any, Dict[str, Any]],
        relationship=DEFAULT_EDGE_RELATIONSHIP,
        is_directed=True,
    ):
        node_src = Node.convert_from_nx(edge[0])
        node_dst = Node.convert_from_nx(edge[1])
        properties = None
        if len(edge) > 2:
            properties = edge[2]
        return cls(
            node_src=node_src,
            node_dest=node_dst,
            label=relationship,
            properties=properties,
            is_directed=is_directed,
        )

    @classmethod
    def from_neptune_response(
        cls, json: Dict, src_node_label="parent", dest_node_label="node"
    ):
        """
        Creates an Edge from the JSON response from Neptune containing two nodes from "parent" and "node" (default labels).

        :param json: (Dict) json-encoded string from neptune-graph containing an edge object
        :param src_node_label: (str, Optional) the name of the json-encoded source node
        :param dest_node_label: (str, Optional) the name of the json-encoded source node
        :return: Edge
        """
        try:
            parent_node = json[src_node_label]
        except KeyError as e:
            raise ValueError(f'json response missing "{src_node_label}" node', e)

        try:
            child_node = json[dest_node_label]
        except KeyError as e:
            raise ValueError(f'json response missing "{dest_node_label}" node', e)

        return cls(
            Node.from_neptune_response(parent_node),
            Node.from_neptune_response(child_node),
        )

    def to_reverse_edge(self):
        return Edge(
            self.node_dest,
            self.node_src,
            label=self.label,
            properties=self.properties,
            is_directed=self.is_directed,
        )

    def to_list(self) -> List[str]:
        """
        Converts edge to a List with the src and destination Node ids
        :return: (List): a pair of strings with the id of the Nodes
        """
        return [self.node_src.id, self.node_dest.id]

    def to_dict(self) -> Dict[str, Any]:
        """
        Converts edge to a List with the src and destination Node ids
        :return: (List): a pair of strings with the id of the Nodes
        """
        return {
            "from": self.node_src.id,
            "to": self.node_dest.id,
            "properties": self.properties,
        }

    def to_group_by(self):
        """
        Return a group by key identifier to categorise the node during UNWIND operation.
        """
        return ImmutableEdgeGroupBy(
            label=self.label,
            labels_src_node=tuple(self.node_src.labels),
            labels_dest_node=tuple(self.node_dest.labels),
            directed=self.is_directed,
        )

    def __eq__(self, other):
        """
        Comparison operator of an Edge

        :param other: Edge to compare
        :return: (boolean) if Edges are considered equal
        """
        if not isinstance(other, Edge):
            return False
        if self.node_src != other.node_src or self.node_dest != other.node_dest:
            return False
        if self.label != other.label or self.properties != other.properties:
            return False
        return True

    def __repr__(self):
        return (
            f"Edge(label={self.label}, properties={self.properties}, node_src={self.node_src}, "
            f"node_dest={self.node_dest}, is_directed={self.is_directed})"
        )


@dataclass(frozen=True)
class ImmutableEdgeGroupBy:
    """
    Immutable data class to represent the group by key during the Edge UNWIND process.
    """

    labels_src_node: Tuple[str, ...]
    labels_dest_node: Tuple[str, ...]
    label: str
    directed: bool
