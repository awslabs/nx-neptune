from typing import Any, Dict, List, Optional, Tuple

from cymple import QueryBuilder

from .na_models import Edge, ImmutableEdgeGroupBy, Node

# Internal constants for reference names
_SRC_NODE_REF = "a"
_DEST_NODE_REF = "b"
_RELATION_REF = "r"
_NODE_REF = "n"
_NODE_FULL_FORM_REF = "node"
_PARENT_FULL_FORM_REF = "parent"
_BFS_PARENTS_ALG = "neptune.algo.bfs.parents"
_PAGE_RANK_ALG = "neptune.algo.pageRank"
_DEGREE_ALG = "neptune.algo.degree"
_RANK_REF = "rank"
_DEGREE_REF = "degree"

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

    def read_map(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, str]:
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

        # handle a map of values
        masked_params = {}
        for key, value in params.items():
            param_name = str(self._counter)
            masked_param_name = f"${param_name}"
            masked_params[key] = masked_param_name
            self._param_values[param_name] = value
            self._counter += 1

        return masked_params

    def read_list(self, params: Optional[List[Any]] = None) -> List[str]:
        """
        Process a list of parameters and create a masked version with parameter placeholders.
        If params is None or empty, returns an empty dictionary.

        Args:
            params: A list containing parameter values, or None

        Returns:
            A list replaced with parameter placeholders ($0, $1, etc.)
        """
        if not params:
            return []

        # handle a list of values
        masked_params = []
        for value in params:
            masked_param_name = f"${self._counter}"
            masked_params.append(masked_param_name)
            self._param_values[str(self._counter)] = value
            self._counter += 1

        return masked_params

    def get_param_values(self) -> Dict[str, Any]:
        """
        Get all parameter values collected from previous read() calls.

        Returns:
            A dictionary mapping parameter placeholders to their values
        """
        return self._param_values


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
        >>> node = Node(id='Alice', labels=['Person'], properties={'age': 15})
        >>> insert_node(node)
        ('CREATE (:Person {'~id': $0, age: $1})', {'0': 'Alice', '1': '15'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    updated_parameters = node.properties
    updated_parameters["`~id`"] = str(node.id)

    # Mask node properties
    masked_properties = param_builder.read_map(updated_parameters)

    return (
        QueryBuilder()
        .create()
        .node(
            labels=node.labels,
            properties=masked_properties,
            escape=False,
        )
    ).query, param_builder.get_param_values()


def insert_nodes(nodes: List[Node]) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Create a list of nodes in the graph.

    :param node: A Node object with labels and properties
    :return: Tuple of (OpenCypher query string, parameter map) for node creation

    """

    group_by_buckets: dict[tuple, list] = {}

    for node in nodes:
        group_by_key = node.to_group_by()
        group_by_buckets.setdefault(group_by_key, []).append(node.to_dict())

    query_list = []
    para_list = []

    for key, value in group_by_buckets.items():
        # Convert key to actual query_string
        query_list.append(get_node_batch_query_str(key))
        para_list.append({"nodes": value})

    return query_list, para_list


def insert_edge(edge: Edge) -> Tuple[str, Dict[str, Any]]:
    """
    Create an edge (relationship) in the graph.

    :param edge: An Edge object with label, properties, node_src, node_dest, and is_directed flag
    :return: Tuple of (OpenCypher query string, parameter map) for edge creation

    Examples:
        >>> src = Node(id='Alice', labels=['Person'], properties={})
        >>> dest = Node(id='Bob', labels=['Person'], properties={})
        >>> edge = Edge(label='FRIEND_WITH', properties={'since': '2020'}, node_src=src, node_dest=dest)
        >>> insert_edge(edge)
        ('MERGE (a:Person {`~id`: $0}) MERGE (b:Person {`~id`: $1})
        MERGE (a)-[r:FRIEND_WITH {since: $2}]->(b)', {'0': 'Alice', '1': 'Bob', '2': '2020'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    qb = QueryBuilder()
    qb = _append_node(qb, param_builder, edge.node_src, _SRC_NODE_REF, True)
    qb = _append_node(qb, param_builder, edge.node_dest, _DEST_NODE_REF, True)
    masked_properties = param_builder.read_map(edge.properties)
    qb = qb.merge().node(ref_name=_SRC_NODE_REF)
    if edge.is_directed:
        qb = qb.related_to(
            label=edge.label,
            ref_name=_RELATION_REF,
            properties=masked_properties,
            escape=False,
        ).node(ref_name=_DEST_NODE_REF)
    else:
        qb = qb.related(
            label=edge.label,
            ref_name=_RELATION_REF,
            properties=masked_properties,
            escape=False,
        ).node(ref_name=_DEST_NODE_REF)

    return qb.query, param_builder.get_param_values()


def get_edge_batch_query_str(group_by_key: ImmutableEdgeGroupBy):
    # TODO: Replace with cymple when it provide wider support of UNWIND.
    src_labels = (
        ":" + ":".join(group_by_key.labels_src_node)
        if group_by_key.labels_src_node
        else ""
    )
    dest_labels = (
        ":" + ":".join(group_by_key.labels_dest_node)
        if group_by_key.labels_dest_node
        else ""
    )

    if group_by_key.directed:
        return (
            f"UNWIND $relations AS rel MATCH (a{src_labels} {{`~id`: rel.from}}), (b{dest_labels} {{`~id`: rel.to}}) "
            f"CREATE (a)-[r:{group_by_key.label}]->(b) SET r += rel.properties"
        )
    else:
        return (
            f"UNWIND $relations AS rel MATCH (a{src_labels} {{`~id`: rel.from}}), (b{dest_labels} {{`~id`: rel.to}}) "
            f"CREATE (a)-[r1:{group_by_key.label}]->(b), (b)-[r2:{group_by_key.label}]->(a)"
            f"SET r1 += rel.properties, r2 += rel.properties"
        )


def get_node_batch_query_str(labels_tuple):
    # TODO: Replace with cymple when it provide wider support of UNWIND.
    labels = ":" + ":".join(labels_tuple) if labels_tuple else ""

    return f"UNWIND $nodes as node CREATE (n{labels} {{`~id`: node.id}}) SET n += node"


def insert_edges(edges: List[Edge]) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Insert a list of edges in the graph.

    :param edges: An list of Edge object with label, properties, node_src, node_dest, and is_directed flag
    :return: Tuple of (OpenCypher query string, parameter map) for edge creation

    """
    group_by_buckets: dict[ImmutableEdgeGroupBy, list] = {}

    for edge in edges:
        group_by_key = edge.to_group_by()
        group_by_buckets.setdefault(group_by_key, []).append(edge.to_dict())

    query_list = []
    para_list = []

    for key, value in group_by_buckets.items():
        query_list.append(get_edge_batch_query_str(key))
        para_list.append({"relations": value})

    return query_list, para_list


def update_node(
    match_labels: str, ref_name: str, node_ids: list[str], properties_set: dict
) -> Tuple[str, Dict[str, Any]]:
    """
    Update a node's properties.

    :param match_labels: Labels to match
    :param ref_name: Reference name for the node
    :param node_ids: list of node IDs to match by
    :param properties_set: Properties to set
    :return: Tuple of (OpenCypher query string, parameter map) for node update

    Example:
        >>> update_node('Person', 'a', ['Alice'], {'a.age': '25'})
        ('MATCH (a:Person) WHERE id(a) = $0 SET a.age = $1', {'0': 'Alice', '1': '25'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    masked_node_ids = param_builder.read_list(node_ids)
    literal_where_clause = " OR ".join(
        [f"id({ref_name})={node_id}" for node_id in masked_node_ids]
    )
    masked_properties_set = param_builder.read_map(properties_set)

    return (
        QueryBuilder()
        .match()
        .node(labels=match_labels, ref_name=ref_name)
        .where_literal(literal_where_clause)
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

    :param ref_name_src: Reference name for the source node.
    :param ref_name_edge: Reference name for the edge.
    :param edge: Edge object with node_src and node_dest attributes.
    :param ref_name_des: Reference name for the destination node.
    :param where_filters: Filters to apply in the WHERE clause.
    :param properties_set: Properties to set.
    :return: Tuple of (OpenCypher query string, parameter map) for edge update.

    Example:
        >>> src = Node(labels=['Person'], properties={})
        >>> dest = Node(labels=['Person'], properties={})
        >>> edge = Edge(label='FRIEND_WITH', properties={}, node_src=src, node_dest=dest)
        >>> update_edge('a', 'r', edge, 'b',
        ...                  {"a.name": "Alice", "b.name": "Bob"},
        ...                  {"r.since": "1997"})
        ('MATCH (a:Person)-[r:FRIEND_WITH]->(b:Person) WHERE id(a) = $0 AND id(b) = $1 SET r.since = $2',
         {'0': 'Alice', '1': 'Bob', '2': '1997'})
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    qb = QueryBuilder().match()
    qb = _append_node(qb, param_builder, edge.node_src, ref_name_src)
    if edge.is_directed:
        qb = qb.related_to(label=edge.label, ref_name=ref_name_edge)
    else:
        qb = qb.relates(label=edge.label, ref_name=ref_name_edge)
    qb = _append_node(qb, param_builder, edge.node_dest, ref_name_des)

    masked_where_filters = param_builder.read_map(where_filters)
    masked_properties_set = param_builder.read_map(properties_set)
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
        >>> node = Node(id='Alice', labels=['Person'], properties={})
        >>> delete_node(node)
        ('MATCH (n:Person {`~id`: $0}) DELETE n', {'0': 'Alice'})
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
    if edge.is_directed:
        qb = qb.related_to(label=edge.label, ref_name=_RELATION_REF)
    else:
        qb = qb.relates(label=edge.label, ref_name=_RELATION_REF)
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

    masked_where_filters = param_builder.read_map(where_filters)

    bfs_params = f"{source_node}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
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


def degree_centrality_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the Degree algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Degree Centrality algorithm execution
    :return: Tuple of (OpenCypher query string, parameter map) for Degree Centrality algorithm execution

    Example:
        >>> degree_centrality_query()
        (' MATCH(n) CALL neptune.algo.degree(n) YIELD degree AS degree RETURN n.id , degree', {})
    """
    degree_params = f"{_NODE_REF}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        degree_params = f"{degree_params}, {{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_NODE_REF)
        .call()
        .procedure(f"{_DEGREE_ALG}({degree_params})")
        .yield_((_DEGREE_REF, _DEGREE_REF))
        .return_literal(f"n.id , {_DEGREE_REF}")
        .query
    ), {}


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
    updated_parameters = node.properties
    updated_parameters["`~id`"] = str(node.id)

    # Mask node properties
    masked_properties = param_builder.read_map(updated_parameters)

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
