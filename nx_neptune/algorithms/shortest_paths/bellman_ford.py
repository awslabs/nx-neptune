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
import logging
import warnings
from typing import Any, Dict, List, Optional

from nx_neptune.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_EDGE_WEIGHT_PROPERTY,
    PARAM_EDGE_WEIGHT_TYPE,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_VERTEX_LABEL,
)
from nx_neptune.clients.opencypher_builder import (
    bellman_ford_path_query,
    bellman_ford_predecessor_and_distance_query,
    single_source_bellman_ford_path_length_query,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = [
    "bellman_ford_path",
    "single_source_bellman_ford_path_length",
    "bellman_ford_predecessor_and_distance",
]

_DEFAULT_WEIGHT = "weight"
_DEFAULT_WEIGHT_TYPE = "double"


def _build_sssp_parameters(
    weight: str = _DEFAULT_WEIGHT,
    edge_weight_type: str = _DEFAULT_WEIGHT_TYPE,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
) -> Dict[str, Any]:
    """Build the common SSSP parameter dictionary for Neptune Analytics."""
    parameters: Dict[str, Any] = {
        PARAM_EDGE_WEIGHT_PROPERTY: weight,
        PARAM_EDGE_WEIGHT_TYPE: edge_weight_type,
    }

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if traversal_direction:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    return parameters


@configure_if_nx_active()
def bellman_ford_path(
    neptune_graph: NeptuneGraph,
    source,
    target,
    weight="weight",
    edge_weight_type: str = _DEFAULT_WEIGHT_TYPE,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
):
    """
    Returns the shortest path from source to target in a weighted graph.
    Link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/sssp-bellmanFord-path.html

    Uses Neptune Analytics' .sssp.bellmanFord.path algorithm, which finds the
    shortest path from a source node to a target node using the Bellman-Ford algorithm.

    :param neptune_graph: A NeptuneGraph instance
    :param source: Starting node
    :param target: Ending node
    :param weight: Edge attribute name used as weight. Must be a string property name
        on edges in Neptune. Default: "weight".
        Note: callable weight functions are not supported by Neptune Analytics.
    :param edge_weight_type: The numeric type of the edge weight property.
        Must be one of: "int", "long", "float", "double". Default: "double".
    :param edge_labels: To filter on one or more edge labels, provide a list.
        If not provided, all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param traversal_direction: The direction of edge to follow.
        Must be one of: "inbound" or "outbound". Default: "outbound".
        Note: "both" is not supported by Neptune's Bellman-Ford implementation.
    :param concurrency: Controls the number of concurrent threads (0=all, 1=single).
    :return: List of nodes in the shortest path from source to target.
    :raises NetworkXNoPath: If no path exists between source and target.
    """
    if callable(weight):
        warnings.warn(
            "Neptune Analytics does not support callable weight functions. "
            "Please provide a string property name instead.",
            UserWarning,
            stacklevel=2,
        )
        weight = _DEFAULT_WEIGHT

    parameters = _build_sssp_parameters(
        weight=weight,
        edge_weight_type=edge_weight_type,
        edge_labels=edge_labels,
        vertex_label=vertex_label,
        traversal_direction=traversal_direction,
        concurrency=concurrency,
    )

    query_str, para_map = bellman_ford_path_query(
        str(source), str(target), parameters
    )
    json_result = neptune_graph.execute_call(query_str, para_map)

    if not json_result:
        import networkx as nx

        raise nx.NetworkXNoPath(
            f"No path between {source} and {target}."
        )

    # Extract node IDs from vertexPath
    vertex_path = json_result[0].get("vertexPath", [])
    path = [node["~id"] for node in vertex_path]

    return path


@configure_if_nx_active()
def single_source_bellman_ford_path_length(
    neptune_graph: NeptuneGraph,
    source,
    weight="weight",
    edge_weight_type: str = _DEFAULT_WEIGHT_TYPE,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
):
    """
    Compute the shortest path length between source and all other reachable nodes.
    Link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/sssp-bellmanFord.html

    Uses Neptune Analytics' .sssp.bellmanFord algorithm, which computes shortest
    path distances from a single source vertex to all other vertices.

    :param neptune_graph: A NeptuneGraph instance
    :param source: Starting node for path
    :param weight: Edge attribute name used as weight. Must be a string property name
        on edges in Neptune. Default: "weight".
        Note: callable weight functions are not supported by Neptune Analytics.
    :param edge_weight_type: The numeric type of the edge weight property.
        Must be one of: "int", "long", "float", "double". Default: "double".
    :param edge_labels: To filter on one or more edge labels, provide a list.
        If not provided, all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param traversal_direction: The direction of edge to follow.
        Must be one of: "inbound" or "outbound". Default: "outbound".
        Note: "both" is not supported by Neptune's Bellman-Ford implementation.
    :param concurrency: Controls the number of concurrent threads (0=all, 1=single).
    :return: Dictionary keyed by target node with shortest path length as value.
    """
    if callable(weight):
        warnings.warn(
            "Neptune Analytics does not support callable weight functions. "
            "Please provide a string property name instead.",
            UserWarning,
            stacklevel=2,
        )
        weight = _DEFAULT_WEIGHT

    parameters = _build_sssp_parameters(
        weight=weight,
        edge_weight_type=edge_weight_type,
        edge_labels=edge_labels,
        vertex_label=vertex_label,
        traversal_direction=traversal_direction,
        concurrency=concurrency,
    )

    query_str, para_map = single_source_bellman_ford_path_length_query(
        str(source), parameters
    )
    json_result = neptune_graph.execute_call(query_str, para_map)

    result = {}
    for item in json_result:
        result[item["nodeId"]] = item["distance"]

    return result


@configure_if_nx_active()
def bellman_ford_predecessor_and_distance(
    neptune_graph: NeptuneGraph,
    source,
    target=None,
    weight="weight",
    heuristic=False,
    edge_weight_type: str = _DEFAULT_WEIGHT_TYPE,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
):
    """
    Compute shortest path lengths and predecessors on shortest paths.
    Link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/sssp-bellmanFord-parents.html

    Uses Neptune Analytics' .sssp.bellmanFord.parents algorithm, which finds parent
    nodes along with shortest path distances from the source to all other nodes.

    :param neptune_graph: A NeptuneGraph instance
    :param source: Starting node for path
    :param target: (Unused) Ending node for path. Neptune computes all reachable nodes
        regardless. Included for NetworkX API compatibility.
    :param weight: Edge attribute name used as weight. Must be a string property name
        on edges in Neptune. Default: "weight".
        Note: callable weight functions are not supported by Neptune Analytics.
    :param heuristic: (Unsupported) Neptune Analytics does not support the heuristic
        parameter. Included for NetworkX API compatibility.
    :param edge_weight_type: The numeric type of the edge weight property.
        Must be one of: "int", "long", "float", "double". Default: "double".
    :param edge_labels: To filter on one or more edge labels, provide a list.
        If not provided, all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param traversal_direction: The direction of edge to follow.
        Must be one of: "inbound" or "outbound". Default: "outbound".
        Note: "both" is not supported by Neptune's Bellman-Ford implementation.
    :param concurrency: Controls the number of concurrent threads (0=all, 1=single).
    :return: Tuple of (pred, dist) dictionaries. pred maps each node to a list of
        predecessors [parent_id]. dist maps each node to its distance from source.
        Note: Neptune returns a single parent per node. The predecessor list will
        contain at most one element per node.
    """
    if callable(weight):
        warnings.warn(
            "Neptune Analytics does not support callable weight functions. "
            "Please provide a string property name instead.",
            UserWarning,
            stacklevel=2,
        )
        weight = _DEFAULT_WEIGHT

    if heuristic:
        warnings.warn(
            "Neptune Analytics does not support the heuristic parameter. "
            "It will be ignored.",
            UserWarning,
            stacklevel=2,
        )

    parameters = _build_sssp_parameters(
        weight=weight,
        edge_weight_type=edge_weight_type,
        edge_labels=edge_labels,
        vertex_label=vertex_label,
        traversal_direction=traversal_direction,
        concurrency=concurrency,
    )

    query_str, para_map = bellman_ford_predecessor_and_distance_query(
        str(source), parameters
    )
    json_result = neptune_graph.execute_call(query_str, para_map)

    pred = {str(source): []}
    dist = {str(source): 0}

    for item in json_result:
        node_id = item["nodeId"]
        parent_id = item["parentId"]
        distance = item["distance"]

        dist[node_id] = distance
        # Neptune returns singular parent; wrap in list for NX compatibility
        if parent_id == node_id:
            # Source node has itself as parent in Neptune
            pred[node_id] = []
        else:
            pred[node_id] = [parent_id]

    return pred, dist
