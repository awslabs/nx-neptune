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
import re
from typing import Any, Dict, List, Optional, Tuple

from cymple import QueryBuilder

from . import PARAM_MAX_DEPTH
from .na_models import Edge, ImmutableEdgeGroupBy, Node
from .neptune_constants import (
    ALGO_PARAM_ENUM_VALUES,
    ALGO_PARAM_IDENTIFIER_KEYS,
    ALGO_PARAM_LIST_KEYS,
    ALLOWED_ALGO_PARAM_KEYS,
    RESPONSE_SUCCESS,
)

# Internal constants for reference names
_SRC_NODE_REF = "a"
_DEST_NODE_REF = "b"
_RELATION_REF = "r"
_NODE_REF = "n"
_LEVEL_REF = "level"
_MIN_LEVEL_REF = "minLevel"
_MEMBERS_REF = "members"
_SUCCESS_REF = "success"
_SCORE_REF = "score"
_NODE_FULL_FORM_REF = "node"
_NODE_FULL_FORM_ID_REF = "nodeId"
_NODE_FULL_FORM_ID_FUNC_REF = f"id({_NODE_FULL_FORM_REF})"
_PARENT_FULL_FORM_REF = "parent"
_BFS_PARENTS_ALG = "neptune.algo.bfs.parents"
_BFS_LEVELS_ALG = "neptune.algo.bfs.levels"
_PAGE_RANK_ALG = "neptune.algo.pageRank"
_PAGERANK_MUTATE_ALG = "neptune.algo.pageRank.mutate"
_DEGREE_ALG = "neptune.algo.degree"
_DEGREE_MUTATE_ALG = "neptune.algo.degree.mutate"
_LABEL_ALG = "neptune.algo.labelPropagation"
_LABEL_MUTATE_ALG = "neptune.algo.labelPropagation.mutate"
_CLOSENESS_ALG = "neptune.algo.closenessCentrality"
_CLOSENESS_MUTATE_ALG = "neptune.algo.closenessCentrality.mutate"
_LOUVAIN_ALG = "neptune.algo.louvain"
_LOUVAIN_MUTATE_ALG = "neptune.algo.louvain.mutate"
_WCC_ALG = "neptune.algo.wcc"
_WCC_MUTATE_ALG = "neptune.algo.wcc.mutate"
_JACCARD_ALG = "neptune.algo.jaccardSimilarity"
_RANK_REF = "rank"
_DEGREE_REF = "degree"
_COMMUNITY_REF = "community"
_COMPONENT_REF = "component"

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


def _truncate_for_error(value: Any, limit: int = 40) -> str:
    """Return a repr of ``value`` safe to embed in an error message.

    Validation errors can carry attacker-supplied input; reflecting the whole
    value verbatim into exceptions/logs is undesirable. Truncate long values so
    the message stays useful for debugging without echoing an unbounded payload.
    """
    text = repr(value)
    if len(text) > limit:
        return text[:limit] + "…"
    return text


def _escape_labels(labels) -> list:
    """Backtick-escape a list of node/edge labels.

    Labels are interpolated into queries with escape=False (or directly into
    f-strings), so each is backtick-quoted here to prevent injection through a
    caller-supplied label. Returns a new list; accepts None.
    """
    if not labels:
        return labels
    if isinstance(labels, str):
        return _escape_identifier(labels)
    return [_escape_identifier(label) for label in labels]


def _escape_identifier(value: str) -> str:
    """Backtick-quote an openCypher identifier (label / property name).

    openCypher permits almost any character in a backtick-quoted identifier, so
    rather than matching against a narrow charset (which would reject legitimate
    Neptune label/property names) we wrap the value in backticks and escape any
    embedded backtick by doubling it — the openCypher-standard escape. This makes
    it impossible to break out of the identifier and inject query syntax.

    Example:
        >>> _escape_identifier("pageRank")
        '`pageRank`'
        >>> _escape_identifier('a`) DETACH DELETE n //')
        '`a``) DETACH DELETE n //`'
    """
    if not isinstance(value, str):
        raise ValueError(f"Expected a string identifier, got {type(value).__name__}")
    return "`" + value.replace("`", "``") + "`"


def _escape_string_literal(value: str) -> str:
    """Encode a string as a double-quoted openCypher string literal.

    Backslashes and double quotes are escaped so the value cannot terminate the
    literal and inject query syntax. Used for list elements (e.g. edge labels).
    """
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _render_list_element(value: Any) -> str:
    """Render one element of a list-valued algorithm parameter safely."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return _escape_string_literal(value)
    raise ValueError(f"Unsupported list element type: {type(value).__name__}")


def _render_parameter_value(key: str, value: Any) -> str:
    """Validate and safely encode a single algorithm parameter value.

    Fail closed (raise ValueError) for closed-domain values (enums, numeric
    types, wrong types); backtick-escape open-domain identifier values; and
    encode list elements individually. This is the choke point that prevents
    openCypher injection through algorithm parameters.
    """
    # Enum-valued string parameters: must be one of the documented values.
    if key in ALGO_PARAM_ENUM_VALUES:
        allowed = ALGO_PARAM_ENUM_VALUES[key]
        if value not in allowed:
            raise ValueError(
                f"Invalid value {_truncate_for_error(value)} for parameter "
                f"{key!r}; expected one of {sorted(allowed)}"
            )
        return _escape_string_literal(value)

    # Identifier-valued string parameters (label / property names). Inside a
    # neptune.algo.* config map these are passed as string VALUES, so they must
    # be double-quoted string literals (backticks would be parsed as an
    # undefined variable reference). Escaping the quote/backslash prevents the
    # value from terminating the literal and injecting query syntax.
    if key in ALGO_PARAM_IDENTIFIER_KEYS:
        if not isinstance(value, str):
            raise ValueError(
                f"Parameter {key!r} must be a string, got {type(value).__name__}"
            )
        return _escape_string_literal(value)

    # List-valued parameters: encode each element.
    if key in ALGO_PARAM_LIST_KEYS:
        if isinstance(value, (str, bytes)) or not isinstance(value, (list, tuple)):
            raise ValueError(
                f"Parameter {key!r} must be a list, got {type(value).__name__}"
            )
        return "[" + ", ".join(_render_list_element(v) for v in value) + "]"

    # Everything else is expected to be numeric or boolean. Reject strings and
    # other types so a value can never carry query syntax.
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)

    raise ValueError(
        f"Invalid value {_truncate_for_error(value)} for parameter "
        f"{key!r}: expected a numeric value"
    )


def _to_parameter_list(parameters: Dict[str, Any]) -> str:
    """
    Convert a dictionary of parameters to a formatted parameter string for OpenCypher queries.

    Each key is validated against the allowlist of known algorithm parameters and
    each value is validated/encoded by type (see _render_parameter_value), so that
    labels, property keys, and parameter values cannot inject openCypher syntax.
    Unknown keys or values that fail validation raise ValueError (fail closed).

    :param parameters: Dictionary of algorithm parameters
    :return: Formatted parameter string for inclusion in OpenCypher query

    Example:
        >>> _to_parameter_list({'dampingFactor': 0.9, 'numOfIterations': 50})
        'dampingFactor:0.9, numOfIterations:50'
    """
    if not parameters:
        return ""

    rendered = []
    for key, value in parameters.items():
        if key not in ALLOWED_ALGO_PARAM_KEYS:
            raise ValueError(f"Unknown algorithm parameter: {key!r}")
        rendered.append(f"{key}:{_render_parameter_value(key, value)}")

    return ", ".join(rendered)


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
            labels=_escape_labels(node.labels),
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
            label=_escape_identifier(edge.label),
            ref_name=_RELATION_REF,
            properties=masked_properties,
            escape=False,
        ).node(ref_name=_DEST_NODE_REF)
    else:
        qb = qb.related(
            label=_escape_identifier(edge.label),
            ref_name=_RELATION_REF,
            properties=masked_properties,
            escape=False,
        ).node(ref_name=_DEST_NODE_REF)

    return qb.query, param_builder.get_param_values()


def get_edge_batch_query_str(group_by_key: ImmutableEdgeGroupBy):
    # TODO: Replace with cymple when it provide wider support of UNWIND.
    src_labels = (
        ":" + ":".join(_escape_labels(group_by_key.labels_src_node))
        if group_by_key.labels_src_node
        else ""
    )
    dest_labels = (
        ":" + ":".join(_escape_labels(group_by_key.labels_dest_node))
        if group_by_key.labels_dest_node
        else ""
    )

    if group_by_key.directed:
        return (
            f"UNWIND $relations AS rel MATCH (a{src_labels} {{`~id`: rel.from}}), (b{dest_labels} {{`~id`: rel.to}}) "
            f"CREATE (a)-[r:{_escape_identifier(group_by_key.label)}]->(b) SET r += rel.properties"
        )
    else:
        return (
            f"UNWIND $relations AS rel MATCH (a{src_labels} {{`~id`: rel.from}}), (b{dest_labels} {{`~id`: rel.to}}) "
            f"CREATE (a)-[r1:{_escape_identifier(group_by_key.label)}]->(b), (b)-[r2:{_escape_identifier(group_by_key.label)}]->(a)"
            f"SET r1 += rel.properties, r2 += rel.properties"
        )


def get_node_batch_query_str(labels_tuple):
    # TODO: Replace with cymple when it provide wider support of UNWIND.
    labels = ":" + ":".join(_escape_labels(labels_tuple)) if labels_tuple else ""

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
        .node(labels=_escape_labels(match_labels), ref_name=ref_name, escape=False)
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
        qb = qb.related_to(label=_escape_identifier(edge.label), ref_name=ref_name_edge)
    else:
        qb = qb.relates(label=_escape_identifier(edge.label), ref_name=ref_name_edge)
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
        qb = qb.related_to(label=_escape_identifier(edge.label), ref_name=_RELATION_REF)
    else:
        qb = qb.relates(label=_escape_identifier(edge.label), ref_name=_RELATION_REF)
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


def descendants_at_distance_query(
    source_node: str, where_filters: dict, parameters=None
) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the BFS-Levels algorithm on Neptune Analytics to compute descendants_at_distance result.

    :param source_node: The variable name for the source node
    :param where_filters: Dictionary of filters to apply in the WHERE clause
    :param parameters: Optional dictionary of algorithm parameters to pass to BFS-Levels
    :return: Tuple of (OpenCypher query string, parameter map) for BFS-Levels algorithm execution

    Example:
        >>> descendants_at_distance_query("Alice", {'id(n)': 'Alice'}, {maxDepth:2})
        MATCH (n)
        WHERE id(n) = 'Alice'
        CALL neptune.algo.bfs.levels(n, {maxDepth:2})
        YIELD node AS node, level AS level WHERE level = 2
        RETURN id(node)
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    masked_where_filters = param_builder.read_map(where_filters)

    distance_params = f"{source_node}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        distance_params = f"{distance_params}, {{{parameters_list_str}}}"

    query_str = (
        QueryBuilder()
        .match()
        .node(ref_name=source_node)
        .where_multiple(masked_where_filters, escape=False)
        .call()
        .procedure(f"{_BFS_LEVELS_ALG}({distance_params})")
        .yield_([(_NODE_FULL_FORM_REF, _NODE_FULL_FORM_REF), (_LEVEL_REF, _LEVEL_REF)])
        .where(_LEVEL_REF, "=", parameters[PARAM_MAX_DEPTH])
        .return_literal(_NODE_FULL_FORM_ID_FUNC_REF)
        .query
    )
    return query_str, param_builder.get_param_values()


def bfs_layers_query(
    source_node: str, where_in_filters: dict, parameters=None
) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the BFS-Levels algorithm on Neptune Analytics to compute bfs_layers result.

    :param source_node: Source node variable
    :param where_filters: Dictionary of filters to apply in the WHERE clauseß
    :param parameters: Optional dictionary of algorithm parameters to pass to BFS-Levels
    :return: Tuple of (OpenCypher query string, parameter map) for BFS-Levels algorithm execution

    Example:
        >>> bfs_layers_query(["Alice"], {})
        MATCH (n)
        WHERE id(n) IN ['Alice']
        CALL neptune.algo.bfs.levels(n)
        YIELD node AS node, level AS level
        WITH id(node) AS nodeId, level
        WITH nodeId, min(level) AS minLevel
        RETURN collect(nodeId) AS id, minLevel AS level
        ORDER BY minLevel ASC
    """
    # Initialize parameter map builder
    param_builder = ParameterMapBuilder()

    masked_where_filters = param_builder.read_map(where_in_filters)

    bfs_params = f"{source_node}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        bfs_params = f"{bfs_params}, {{{parameters_list_str}}}"

    query_str = (
        QueryBuilder()
        .match()
        .node(ref_name=source_node)
        .where_multiple(masked_where_filters, comparison_operator="IN", escape=False)
        .call()
        .procedure(f"{_BFS_LEVELS_ALG}({bfs_params})")
        .yield_([(_NODE_FULL_FORM_REF, _NODE_FULL_FORM_REF), (_LEVEL_REF, _LEVEL_REF)])
        .with_(
            f"{_NODE_FULL_FORM_ID_FUNC_REF} AS {_NODE_FULL_FORM_ID_REF}, {_LEVEL_REF}"
        )
        .with_(f"{_NODE_FULL_FORM_ID_REF}, min({_LEVEL_REF}) AS {_MIN_LEVEL_REF}")
        .return_mapping(
            [(f"collect({_NODE_FULL_FORM_ID_REF})", "id"), (_MIN_LEVEL_REF, _LEVEL_REF)]
        )
        .order_by(_MIN_LEVEL_REF)
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


def pagerank_mutation_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the mutated version of PageRank algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to PageRank
    :return: Tuple of (OpenCypher query string, parameter map) for PageRank algorithm execution

    Example:
        >>> pagerank_mutation_query()
        (' CALL neptune.algo.pageRank.mutate({ write_property:"pageRank"}) YIELD success AS success RETURN success)')
    """
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        pagerank_params = f"{{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_NODE_REF)
        .call()
        .procedure(f"{_PAGERANK_MUTATE_ALG}({pagerank_params})")
        .yield_((_SUCCESS_REF, _SUCCESS_REF))
        .return_literal(RESPONSE_SUCCESS)
        .query
    ), {}


def label_propagation_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the Label Propagation algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Label Propagation
    :return: Tuple of (OpenCypher query string, parameter map) for Label Propagation algorithm execution

    Example:
        >>> label_propagation_query()
        (' MATCH (n) CALL neptune.algo.labelPropagation(n)
        YIELD node AS node, community AS community WITH community, id(node) AS nodeId
        RETURN community AS community, collect(nodeId) AS members', {})
        >>> label_propagation_query({'maxIterations': 50})
        (' MATCH (n) CALL neptune.algo.labelPropagation(n, {maxIterations:50 })
        YIELD node AS node, community AS community WITH community, id(node) AS nodeId
        RETURN community AS community, collect(nodeId) AS members', {})
    """
    params = f"{_NODE_REF}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{params}, {{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_NODE_REF)
        .call()
        .procedure(f"{_LABEL_ALG}({params})")
        .yield_(
            [
                (_NODE_FULL_FORM_REF, _NODE_FULL_FORM_REF),
                (_COMMUNITY_REF, _COMMUNITY_REF),
            ]
        )
        .with_(
            f"{_COMMUNITY_REF}, {_NODE_FULL_FORM_ID_FUNC_REF} AS {_NODE_FULL_FORM_ID_REF}"
        )
        .return_mapping(
            [
                (_COMMUNITY_REF, _COMMUNITY_REF),
                (f"collect({_NODE_FULL_FORM_ID_REF})", _MEMBERS_REF),
            ]
        )
        .query
    ), {}


def louvain_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the Louvain algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Louvain algorithm
    :return: Tuple of (OpenCypher query string, parameter map) for Louvain algorithm execution

    Example:
        >>> louvain_query()
        (' MATCH (n) CALL neptune.algo.louvain(n)
        YIELD node AS node, community AS community WITH community, id(node) AS nodeId
        RETURN community AS community, collect(nodeId) AS members', {})
        >>> louvain_query({'iterationTolerance': 1e-07})
        (' MATCH (n) CALL neptune.algo.louvain(n, {iterationTolerance:1e-07 })
        YIELD node AS node, community AS community WITH community, id(node) AS nodeId
        RETURN community AS community, collect(nodeId) AS members', {})
    """
    params = f"{_NODE_REF}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{params}, {{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_NODE_REF)
        .call()
        .procedure(f"{_LOUVAIN_ALG}({params})")
        .yield_(
            [
                (_NODE_FULL_FORM_REF, _NODE_FULL_FORM_REF),
                (_COMMUNITY_REF, _COMMUNITY_REF),
            ]
        )
        .with_(
            f"{_COMMUNITY_REF}, {_NODE_FULL_FORM_ID_FUNC_REF} AS {_NODE_FULL_FORM_ID_REF}"
        )
        .return_mapping(
            [
                (_COMMUNITY_REF, _COMMUNITY_REF),
                (f"collect({_NODE_FULL_FORM_ID_REF})", _MEMBERS_REF),
            ]
        )
        .query
    ), {}


def louvain_mutation_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the mutated version of Louvain algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Louvain algorithm execution
    :return: Tuple of (OpenCypher query string, parameter map) for Louvain algorithm execution

    Example:
        >>> louvain_mutation_query()
        (' CALL neptune.algo.louvain.mutate()
        YIELD success AS success RETURN success', {})
        >>> louvain_mutation_query({'writeProperty': 'community_id'})
        (' CALL neptune.algo.louvain.mutate({writeProperty:"community_id", iterationTolerance:1e-07 })
        YIELD success AS success RETURN success', {})
    """
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .call()
        .procedure(f"{_LOUVAIN_MUTATE_ALG}({params})")
        .yield_((_SUCCESS_REF, _SUCCESS_REF))
        .return_literal(RESPONSE_SUCCESS)
        .query
    ), {}


def label_propagation_mutation_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the mutated version of Label Propagation algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Degree Centrality algorithm execution
    :return: Tuple of (OpenCypher query string, parameter map) for Degree Centrality algorithm execution

    Example:
        >>> label_propagation_query()
        (' CALL neptune.algo.labelPropagation.mutate({writeProperty:"degree"})
        YIELD success AS success RETURN success', {})
        >>> label_propagation_query({'maxIterations': 50})
        (' CALL neptune.algo.labelPropagation.mutate({writeProperty:"degree", 'maxIterations': 50})
        YIELD success AS success RETURN success', {})
    """
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .call()
        .procedure(f"{_LABEL_MUTATE_ALG}({params})")
        .yield_((_SUCCESS_REF, _SUCCESS_REF))
        .return_literal(RESPONSE_SUCCESS)
        .query
    ), {}


def closeness_centrality_query(
    parameters=None,
    source_nodes: Optional[List] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the Closenss Centrality algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Closeness Centrality algorithm
    :param source_nodes: If a vertexLabel is provided, nodes that do not have the given vertexLabel are ignored.
    :return: Tuple of (OpenCypher query string, parameter map) for Closeness Centrality algorithm execution

    Example:
        >>> closeness_centrality_query()
        (' CALL neptune.algo.closenessCentrality(
        {numSources:9223372036854775807, normalize:True})
        YIELD node AS node, score AS score RETURN score AS score, id(node) AS nodeId, {})
    """

    if source_nodes:
        params = _get_nodes_in_list(source_nodes)
        qb = QueryBuilder()
    else:
        params = f"{_NODE_REF}"
        qb = QueryBuilder().match().node(ref_name=_NODE_REF)

    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{params}, {{{parameters_list_str}}}"

    return (
        qb.call()
        .procedure(f"{_CLOSENESS_ALG}({params})")
        .yield_(
            [
                (_NODE_FULL_FORM_REF, _NODE_FULL_FORM_REF),
                (_SCORE_REF, _SCORE_REF),
            ]
        )
        .return_mapping(
            [
                (_SCORE_REF, _SCORE_REF),
                (_NODE_FULL_FORM_ID_FUNC_REF, _NODE_FULL_FORM_ID_REF),
            ]
        )
        .query
    ), {}


def closeness_centrality_mutation_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the mutated version of Closeness Centrality algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Closeness Centrality algorithm execution
    :return: Tuple of (OpenCypher query string, parameter map) for Closeness Centrality algorithm execution

    Example:
        >>> closeness_centrality_mutation_query()
        (' CALL neptune.algo.closenessCentrality.mutate()
        YIELD success AS success RETURN success', {})
        >>> closeness_centrality_mutation_query({'writeProperty': 'community_id'})
        (' CALL neptune.algo.closenessCentrality.mutate({writeProperty:"community_id" })
        YIELD success AS success RETURN success', {})
    """
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .call()
        .procedure(f"{_CLOSENESS_MUTATE_ALG}({params})")
        .yield_((_SUCCESS_REF, _SUCCESS_REF))
        .return_literal(RESPONSE_SUCCESS)
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


def degree_centrality_mutation_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the mutated version of Degree algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to Degree Centrality algorithm execution
    :return: Tuple of (OpenCypher query string, parameter map) for Degree Centrality algorithm execution

    Example:
        >>> degree_centrality_query()
        (' CALL neptune.algo.degree.mutate({writeProperty:"degree"}) YIELD success AS success RETURN success', {})
    """
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        degree_params = f"{{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .call()
        .procedure(f"{_DEGREE_MUTATE_ALG}({degree_params})")
        .yield_((_SUCCESS_REF, _SUCCESS_REF))
        .return_literal(RESPONSE_SUCCESS)
        .query
    ), {}


def wcc_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the Weakly Connected Components (WCC) algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to WCC algorithm
    :return: Tuple of (OpenCypher query string, parameter map) for WCC algorithm execution

    Example:
        >>> wcc_query()
        (' MATCH (n) CALL neptune.algo.wcc(n)
        YIELD node AS node, component AS component WITH component, id(node) AS nodeId
        RETURN component AS component, collect(nodeId) AS members', {})
        >>> wcc_query({'edgeLabels': ['route']})
        (' MATCH (n) CALL neptune.algo.wcc(n, {edgeLabels:["route"]})
        YIELD node AS node, component AS component WITH component, id(node) AS nodeId
        RETURN component AS component, collect(nodeId) AS members', {})
    """
    params = f"{_NODE_REF}"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{params}, {{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .match()
        .node(ref_name=_NODE_REF)
        .call()
        .procedure(f"{_WCC_ALG}({params})")
        .yield_(
            [
                (_NODE_FULL_FORM_REF, _NODE_FULL_FORM_REF),
                (_COMPONENT_REF, _COMPONENT_REF),
            ]
        )
        .with_(
            f"{_COMPONENT_REF}, {_NODE_FULL_FORM_ID_FUNC_REF} AS {_NODE_FULL_FORM_ID_REF}"
        )
        .return_mapping(
            [
                (_COMPONENT_REF, _COMPONENT_REF),
                (f"collect({_NODE_FULL_FORM_ID_REF})", _MEMBERS_REF),
            ]
        )
        .query
    ), {}


def wcc_mutation_query(parameters=None) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the mutated version of WCC algorithm on Neptune Analytics.

    :param parameters: Optional dictionary of algorithm parameters to pass to WCC algorithm execution
    :return: Tuple of (OpenCypher query string, parameter map) for WCC algorithm execution

    Example:
        >>> wcc_mutation_query({'writeProperty': 'wccid'})
        (' CALL neptune.algo.wcc.mutate({writeProperty:"wccid"})
        YIELD success AS success RETURN success', {})
    """
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        params = f"{{{parameters_list_str}}}"
    return (
        QueryBuilder()
        .call()
        .procedure(f"{_WCC_MUTATE_ALG}({params})")
        .yield_((_SUCCESS_REF, _SUCCESS_REF))
        .return_literal(RESPONSE_SUCCESS)
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
        labels=_escape_labels(node.labels),
        properties=masked_properties,
        escape=False,
    )

    return query_builder


def _get_nodes_in_list(source_nodes: list[str]):
    """
    Converts a list of node IDs into a formatted string representation.

    :param source_nodes: A list of node IDs or a single node ID as a string.
    :return: A string with node IDs enclosed in square brackets and single quotes, e.g., "['A','B','C']".
    :raises ValueError: If any node ID contains unsafe characters.
    """
    _NODE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-:.]+\Z")
    nodes = [source_nodes] if isinstance(source_nodes, str) else source_nodes
    for node_id in nodes:
        if not _NODE_ID_RE.match(str(node_id)):
            raise ValueError(f"Invalid node ID: {_truncate_for_error(node_id)}")
    return "[" + ",".join(f"'{s}'" for s in nodes) + "]"


def jaccard_coefficient_query(
    first_node: str,
    second_node: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> Tuple[str, Dict[str, Any]]:
    """
    Create a query to execute the Jaccard Similarity algorithm on Neptune Analytics
    for a single pair of nodes.

    Neptune requires Node references from MATCH clauses, and using MATCH ... IN
    with multiple nodes produces a cross join. Each pair must be queried separately.

    :param first_node: The ID of the first node
    :param second_node: The ID of the second node
    :param parameters: Optional dictionary of algorithm parameters (edgeLabels, vertexLabel, traversalDirection)
    :return: Tuple of (OpenCypher query string, parameter map) for Jaccard Similarity algorithm execution

    Example:
        >>> jaccard_coefficient_query("Alice", "Bob")
        ('MATCH (n1) WHERE id(n1) = $0 MATCH (n2) WHERE id(n2) = $1
        CALL neptune.algo.jaccardSimilarity(n1, n2) YIELD score RETURN score', {'0': 'Alice', '1': 'Bob'})
    """
    param_builder = ParameterMapBuilder()

    masked_first = param_builder.read_map({"id(n1)": first_node})
    masked_second = param_builder.read_map({"id(n2)": second_node})

    jaccard_params = "n1, n2"
    if parameters:
        parameters_list_str = _to_parameter_list(parameters)
        jaccard_params = f"{jaccard_params}, {{{parameters_list_str}}}"

    query_str = (
        QueryBuilder()
        .match()
        .node(ref_name="n1")
        .where_multiple(masked_first, escape=False)
        .match()
        .node(ref_name="n2")
        .where_multiple(masked_second, escape=False)
        .call()
        .procedure(f"{_JACCARD_ALG}({jaccard_params})")
        .yield_((_SCORE_REF, _SCORE_REF))
        .return_literal(_SCORE_REF)
        .query
    )
    return query_str, param_builder.get_param_values()
