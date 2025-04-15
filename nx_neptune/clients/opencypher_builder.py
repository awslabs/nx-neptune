from typing import Any, Dict, List, Optional, Tuple

from cymple import QueryBuilder

# Internal constants for reference names
_SRC_NODE_REF = "a"
_DEST_NODE_REF = "b"
_RELATION_REF = "r"
_NODE_REF = "n"
_NODE_FULL_FORM_REF = "node"
_PARENT_FULL_FORM_REF = "parent"
_BFS_PARENTS_ALG = "neptune.algo.bfs.parents"
_PAGE_RANK_ALG = "neptune.algo.pageRank"
_RANK_REF = "rank"

__all__ = [
    "match_all_nodes",
    "match_all_edges",
    "insert_node",
    "insert_edge",
    "update_node",
    "update_edge",
    "delete_node",
    "delete_edge",
    "clear_query",
    "bfs_query",
    "pagerank_query",
    "Node",
    "Edge",
]


def _to_parameter_list(parameters: Dict[str, Any]) -> str:
    """
    Convert a dictionary of parameters to a formatted parameter string for OpenCypher queries.

    :param parameters: Dictionary of algorithm parameters
    :return: Formatted parameter string for inclusion in OpenCypher query

    Example:
        >>> _to_parameter_list({'dampingFactor': 0.9, 'maxIterations': 50})
        'dampingFactor:0.9, maxIterations:50'
    """
    if not parameters:
        return ""

    return ", ".join(
        [
            f'{key}:"{value}"' if isinstance(value, str) else f"{key}:{value}"
            for key, value in parameters.items()
        ]
    )


class ParameterMapBuilder:
    """
    A utility class for building parameter maps for OpenCypher queries.

    This class maintains a counter and provides methods to convert a dictionary
    into a masked version where values are replaced with parameter placeholders
    ($0, $1, etc.). Parameter values are stored internally and can be retrieved
    with get_param_values().
    """

    def __init__(self):
        """Initialize the parameter map builder with a counter starting at 0."""
        self._counter = 0
        self._param_values = {}

    def read(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
        """
        Process a dictionary and create a masked version with parameter placeholders.
        If params is None or empty, returns an empty dictionary.

        Args:
            params: A dictionary containing parameter names and values, or None

        Returns:
            A dictionary with the same keys but values replaced with parameter placeholders ($0, $1, etc.)
        """
        if not params:
            return {}

        masked_params = {}

        for key, value in params.items():
            param_name = str(self._counter)
            masked_param_name = f"${param_name}"
            masked_params[key] = masked_param_name
            self._param_values[param_name] = value
            self._counter += 1

        return masked_params

    def get_param_values(self) -> Dict[str, Any]:
        """
        Get all parameter values collected from previous read() calls.

        Returns:
            A dictionary mapping parameter placeholders to their values
        """
        return self._param_values


class Node:
    """
    Represents a node in a graph with labels and properties.

    A node can have multiple labels and a dictionary of properties.
    This class is used to create, update, and delete nodes in Neptune Analytics.
    TODO: Move data model under na_graph

    Attributes:
        id (str, optional): a unique identifier for the node
        labels (list): A list of labels for the node
        properties (dict): A dictionary of properties for the node
    """

    def __init__(self, id=None, labels=None, properties=None):
        self.id = id
        self.labels = labels if labels else []
        self.properties = properties if properties else {}

    @classmethod
    def from_neptune_response(cls, json: Dict):
        return cls(
            id=json.get("~id"),
            labels=json.get("~labels"),
            properties=json.get("~properties"),
        )

    def by_name(self) -> str:
        """
        Gets the Name property of the Node if available
        :return: str
        """
        return self.properties.get("name", "")

    def __eq__(self, other):
        """
        Comparison operator of a Node

        :param other: Node to compare
        :return: (boolean) if Nodes are considered equal
        """
        if not isinstance(other, Node):
            return False
        if self.id and self.id == other.id:
            return True
        if self.labels == other.labels and self.properties == other.properties:
            return True
        return False

    def __repr__(self):
        return f"Node(labels={self.labels}, properties={self.properties})"


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
    """

    def __init__(self, node_src, node_dest, label=None, properties=None):
        """
        Initialize an Edge object.

        Args:
            node_src (Node): The source node of the edge
            node_dest (Node): The destination node of the edge
            label (str, optional): The label for the edge. If None, defaults to an empty string.
            properties (dict, optional): A dictionary of properties for the edge

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

    @classmethod
    def from_neptune_response(cls, json: Dict):
        """
        Creates an Edge from the JSON response from Neptune

        :param json: json-encoded string from neptune-graph containing an edge object
        :return: Edge
        """
        return cls(
            Node.from_neptune_response(json.get("parent", {})),
            Node.from_neptune_response(json.get("node", {})),
        )

    def to_list(self) -> Tuple[str, str]:
        """
        Converts edge to a Tuple with the src and destination Node name
        :return: (Tuple): pair is strings with the name of the Nodes
        """
        return self.node_src.by_name(), self.node_dest.by_name()

    def __repr__(self):
        return f"Edge(label={self.label}, properties={self.properties}, node_src={self.node_src}, node_dest={self.node_dest})"


def match_all_nodes() -> str:
    """
    Create a query to match all nodes in the graph.

    :return: OpenCypher query string for matching all nodes

    Example:
        >>> match_all_nodes()
        'MATCH (n) RETURN n'
    """
    return (
        QueryBuilder().match().node(ref_name=_NODE_REF).return_literal(_NODE_REF).query
    )


def match_all_edges() -> str:
    """
    Create a query to match all edges (relationships) in the graph.

    :return: OpenCypher query string for matching all edges

    Example:
        >>> match_all_edges()
        'MATCH (a)-[r]->(b) RETURN r'
    """
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_SRC_NODE_REF)
        .related_to(ref_name=_RELATION_REF)
        .node(ref_name=_DEST_NODE_REF)
        .return_literal(_RELATION_REF)
        .query
    )


def insert_node(node: Node) -> Tuple[str, Dict[str, Any]]:
    """
    Create a node in the graph.

    :param node: A Node object with labels and properties
    :return: Tuple of (OpenCypher query string, parameter map) for node creation

    Examples:
        >>> node = Node(labels=['Person'], properties={'name': 'Alice'})
        >>> insert_node(node)
        ('CREATE (:Person {name: $0})', {'0': 'Alice'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    # Mask node properties
    masked_properties = param_builder.read(node.properties)

    return (
        QueryBuilder()
        .create()
        .node(labels=node.labels, properties=masked_properties, escape=False)
    ).query, param_builder.get_param_values()


def insert_edge(edge: Edge) -> Tuple[str, Dict[str, Any]]:
    """
    Create an edge (relationship) in the graph.

    :param edge: An Edge object with label, properties, node_src, and node_dest
    :return: Tuple of (OpenCypher query string, parameter map) for edge creation

    Examples:
        >>> src = Node(labels=['Person'], properties={'name': 'Alice'})
        >>> dest = Node(labels=['Person'], properties={'name': 'Bob'})
        >>> edge = Edge(label='FRIEND_WITH', properties={'since': '2020'}, node_src=src, node_dest=dest)
        >>> insert_edge(edge)
        ('MERGE (a:Person {name: $0}) MERGE (b:Person {name: $1})
        MERGE (a)-[r:FRIEND_WITH {since: $2}]->(b)', {'0': 'Alice', '1': 'Bob', '2': '2020'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    qb = QueryBuilder()
    qb = _append_node(qb, param_builder, edge.node_src, _SRC_NODE_REF, True)
    qb = _append_node(qb, param_builder, edge.node_dest, _DEST_NODE_REF, True)
    masked_properties = param_builder.read(edge.properties)
    qb = qb.merge()
    qb = (
        qb.node(ref_name=_SRC_NODE_REF)
        .related_to(
            label=edge.label,
            ref_name=_RELATION_REF,
            properties=masked_properties,
            escape=False,
        )
        .node(ref_name=_DEST_NODE_REF)
    )

    return qb.query, param_builder.get_param_values()


def update_node(
    match_labels: str, ref_name: str, where_filters: dict, properties_set: dict
) -> Tuple[str, Dict[str, Any]]:
    """
    Update a node's properties.

    :param match_labels: Labels to match
    :param ref_name: Reference name for the node
    :param where_filters: Filters to apply in the WHERE clause
    :param properties_set: Properties to set
    :return: Tuple of (OpenCypher query string, parameter map) for node update

    Example:
        >>> update_node('Person', 'a', {'a.name': 'Alice'}, {'a.age': '25'})
        ('MATCH (a:Person) WHERE a.name = $0 SET a.age = $1', {'0': 'Alice', '1': '25'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    masked_where_filters = param_builder.read(where_filters)
    masked_properties_set = param_builder.read(properties_set)

    return (
        QueryBuilder()
        .match()
        .node(labels=match_labels, ref_name=ref_name)
        .where_multiple(masked_where_filters, escape=False)
        .set(masked_properties_set, escape_values=False)
        .query
    ), param_builder.get_param_values()


def update_edge(
    ref_name_src: str,
    ref_name_edge: str,
    edge: Edge,
    ref_name_des: str,
    where_filters: dict,
    properties_set: dict,
) -> Tuple[str, Dict[str, Any]]:
    """
    Update an edge's properties.

    :param ref_name_src: Reference name for the source node
    :param ref_name_edge: Reference name for the edge
    :param edge: Edge object with node_src and node_dest attributes
    :param ref_name_des: Reference name for the destination node
    :param where_filters: Filters to apply in the WHERE clause
    :param properties_set: Properties to set
    :return: Tuple of (OpenCypher query string, parameter map) for edge update

    Example:
        >>> src = Node(labels=['Person'], properties={})
        >>> dest = Node(labels=['Person'], properties={})
        >>> edge = Edge(label='FRIEND_WITH', properties={}, node_src=src, node_dest=dest)
        >>> update_edge('a', 'r', edge, 'b',
        ...                  {"a.name": "Alice", "b.name": "Bob"},
        ...                  {"r.since": "1997"})
        ('MATCH (a:Person)-[r:FRIEND_WITH]->(b:Person) WHERE a.name = $0 AND b.name = $1 SET r.since = $2',
         {'0': 'Alice', '1': 'Bob', '2': '1997'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    qb = QueryBuilder().match()
    qb = _append_node(qb, param_builder, edge.node_src, ref_name_src)
    qb = qb.related_to(label=edge.label, ref_name=ref_name_edge)
    qb = _append_node(qb, param_builder, edge.node_dest, ref_name_des)

    masked_where_filters = param_builder.read(where_filters)
    masked_properties_set = param_builder.read(properties_set)
    qb = qb.where_multiple(masked_where_filters, escape=False).set(
        masked_properties_set, escape_values=False
    )

    return qb.query, param_builder.get_param_values()


def delete_node(node: Node) -> Tuple[str, Dict[str, Any]]:
    """
    Delete a node from the graph.

    :param node: A Node object with labels and properties
    :return: Tuple of (OpenCypher query string, parameter map) for node deletion

    Examples:
        >>> node = Node(labels=['Person'], properties={'name': 'Alice'})
        >>> delete_node(node)
        ('MATCH (n:Person {name: $0}) DELETE n', {'0': 'Alice'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    qb = QueryBuilder().match()
    qb = _append_node(qb, param_builder, node, _NODE_REF)
    qb = qb.delete(ref_name=_NODE_REF)

    return qb.query, param_builder.get_param_values()


def delete_edge(edge: Edge) -> Tuple[str, Dict[str, Any]]:
    """
    Delete an edge (relationship) from the graph.

    :param edge: An Edge object with label, properties, node_src, and node_dest
    :return: Tuple of (OpenCypher query string, parameter map) for edge deletion

    Examples:
        >>> src = Node(labels=['Person'], properties={'name': 'Alice'})
        >>> dest = Node(labels=['Person'], properties={'name': 'Bob'})
        >>> edge = Edge(label='FRIEND_WITH', properties={}, node_src=src, node_dest=dest)
        >>> delete_edge(edge)
        ('MATCH (a:Person {name: $0})-[r:FRIEND_WITH]->(b:Person {name: $1}) DELETE r', {'0': 'Alice', '1': 'Bob'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    qb = QueryBuilder().match()
    qb = _append_node(qb, param_builder, edge.node_src, _SRC_NODE_REF)
    qb = qb.related_to(label=edge.label, ref_name=_RELATION_REF)
    qb = _append_node(qb, param_builder, edge.node_dest, _DEST_NODE_REF)
    qb = qb.delete(ref_name=_RELATION_REF)

    return qb.query, param_builder.get_param_values()


def clear_query() -> str:
    """
    Create a query to clear all nodes and relationships in the graph.

    :return: OpenCypher query string for clearing the graph

    Example:
        >>> clear_query()
        'MATCH (n) DETACH DELETE n'
    """
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_NODE_REF)
        .detach_delete(ref_name=_NODE_REF)
        .query
    )


def bfs_query(
    source_node: str, where_filters: dict, parameters=None
) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute a Breadth-First Search algorithm on Neptune Analytics.
    TODO: Update source_node_list to receive multiple node objects, for BFS calculation.

    :param source_node: The variable name for the source node
    :param where_filters: Dictionary of filters to apply in the WHERE clause
    :param parameters: Optional dictionary of algorithm parameters to pass to BFS
    :return: Tuple of (OpenCypher query string, parameter map) for BFS algorithm execution

    Example:
        >>> bfs_query('n', {'n.name': 'Alice'})
        ('MATCH (n) WHERE n.name = $0 CALL neptune.algo.bfs.parent(n)
        YIELD parent as parent, node as node RETURN parent, node', {'0': 'Alice'})
        >>> bfs_query('n', {'n.name': 'Alice'}, {'maxDepth': 3})
        ('MATCH (n) WHERE n.name = $0 CALL neptune.algo.bfs.parent(n, {maxDepth:3})
        YIELD parent as parent, node as node RETURN parent, node', {'0': 'Alice'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    masked_where_filters = param_builder.read(where_filters)

    bfs_params = f"{source_node}"
    if parameters:
        parameters_list_str = ", ".join(
            ["%s:%s" % (key, value) for (key, value) in parameters.items()]
        )
        bfs_params = f"{bfs_params}, {{{parameters_list_str}}}"

    # for a query that returns the source and node for each traversal
    query_str = (
        QueryBuilder()
        .match()
        .node(ref_name=source_node)
        .where_multiple(masked_where_filters, escape=False)
        .call()
        .procedure(f"{_BFS_PARENTS_ALG}({bfs_params})")
        .yield_(
            [
                (_PARENT_FULL_FORM_REF, _PARENT_FULL_FORM_REF),
                (_NODE_FULL_FORM_REF, _NODE_FULL_FORM_REF),
            ]
        )
        .return_literal(f"{_PARENT_FULL_FORM_REF}, {_NODE_FULL_FORM_REF}")
        .query
    )
    return query_str, param_builder.get_param_values()


def _append_node(
    query_builder,
    param_builder: ParameterMapBuilder,
    node: Node,
    ref_name: str = _NODE_REF,
    incl_merge: bool = False,
) -> QueryBuilder:
    """
    Helper method to append a node to a query builder.

    :param query_builder: The QueryBuilder instance to modify
    :param param_builder: The ParameterMapBuilder to use for masking properties
    :param node: The node to append
    :param ref_name: Reference name for the node (default: _NODE_REF)
    :param incl_merge: If True, adds .merge() to the node creation (default: False)
    :return: The modified QueryBuilder instance
    """
    # Mask node properties
    masked_properties = param_builder.read(node.properties)

    # Add merge if requested
    if incl_merge:
        query_builder = query_builder.merge()

    # Append the node to the query builder
    query_builder = query_builder.node(
        ref_name=ref_name,
        labels=node.labels,
        properties=masked_properties,
        escape=False,
    )

    return query_builder


def pagerank_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the PageRank algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to PageRank
    :return: Tuple of (OpenCypher query string, parameter map) for PageRank algorithm execution

    Example:
        >>> pagerank_query()
        (' MATCH (n) CALL neptune.algo.pageRank(n ) YIELD rank AS rank RETURN n, rank', {})
        >>> pagerank_query({'dampingFactor': 0.9, 'maxIterations': 50})
        (' MATCH (n) CALL neptune.algo.pageRank(n, {dampingFactor:0.9, maxIterations:50 } )
        YIELD rank AS rank RETURN n, rank', {})
    """
    pagerank_params = f"{_NODE_REF}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        pagerank_params = f"{pagerank_params}, {{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_NODE_REF)
        .call()
        .procedure(f"{_PAGE_RANK_ALG}({pagerank_params})")
        .yield_((_RANK_REF, _RANK_REF))
        .return_literal(_NODE_REF + ", " + _RANK_REF)
        .query
    ), {}
