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
from typing import Any, List, Optional

from nx_neptune.algorithms.util import process_unsupported_param
from nx_neptune.clients import Edge
from nx_neptune.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_MAX_DEPTH,
    PARAM_SORT_NEIGHBORS,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_VERTEX_LABEL,
)
from nx_neptune.clients.opencypher_builder import (
    _NODE_FULL_FORM_ID_FUNC_REF,
    bfs_layers_query,
    bfs_query,
    descendants_at_distance_query,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["bfs_edges", "descendants_at_distance", "bfs_layers"]


@configure_if_nx_active()
def bfs_edges(
    neptune_graph: NeptuneGraph,
    source,
    reverse=False,
    depth_limit=None,
    sort_neighbors=None,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
):
    """
    Iterate over edges in a breadth-first-search starting at source. Runs the
    breadth-first search (BFS) algorithm for finding nodes.

    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/bfs-standard.html

    Parameters
    ----------
    neptune_graph : NeptuneGraph
    source : node
       Specify starting node for breadth-first search; this function
       iterates over only those edges in the component reachable from
       this node.
    reverse : bool, optional
       If True traverse a directed graph in the reverse direction
    depth_limit : int, optional(default=len(G))
        Specify the maximum search depth
    sort_neighbors : function (default=None)
        A function that takes an iterator over nodes as the input, and
        returns an iterable of the same nodes with a custom ordering.
        For example, `sorted` will sort the nodes in increasing order.
        (not supported in Neptune Analytics)
    vertex_label : str, optional
        A vertex label for vertex filtering.
    edge_labels : list, optional
        To filter on one more edge labels, provide a list of the ones to filter on.
        If no edgeLabels field is provided then all edge labels are processed during traversal.
    concurrency : int, optional
        Controls the number of concurrent threads used to run the algorithm.
        If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
        If set to 1, uses a single thread.

    Yields
    ------
    edge
        Edges in the breadth-first search starting from `source`.
    """
    logger.debug(
        f"nx_neptune.bfs_edges() with: \nneptune_graph={neptune_graph}\nsource={source}\n"
        f"reverse={reverse}\n"
        f"depth_limit={depth_limit}\n"
        f"sort_neighbors={sort_neighbors}\n"
        f"vertex_label={vertex_label}\n"
        f"edge_labels={edge_labels}\n"
        f"concurrency={concurrency}"
    )

    source_node = "n"

    parameters = {}
    # map parameters:
    if depth_limit:
        parameters[PARAM_MAX_DEPTH] = depth_limit
    parameters[PARAM_TRAVERSAL_DIRECTION] = neptune_graph.traversal_direction(reverse)

    # Process NA specific parameters
    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    # Process unsupported parameters (for warnings only)
    process_unsupported_param(
        {
            PARAM_SORT_NEIGHBORS: sort_neighbors,
        }
    )

    query_str, para_map = bfs_query(
        source_node, {f"id({source_node})": source}, parameters
    )
    json_result = neptune_graph.execute_call(query_str, para_map)

    for json_edge in json_result:
        edge = Edge.from_neptune_response(json=json_edge)
        # Neptune returns a result with source node -> source node - skip it
        if edge.node_src == edge.node_dest:
            continue
        yield edge.to_list()


@configure_if_nx_active()
def descendants_at_distance(
    neptune_graph: NeptuneGraph,
    source,
    distance: int,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
):
    """
    The BFS-Levels algorithm is executed on the Neptune Analytics graph,
    to achieve feature parity with the results generated by NetworkX.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/bfs-levels.html

    :param neptune_graph: A NeptuneGraph instance
    :param source: Specify starting node for breadth-first levels search
    :param distance: The distance of the wanted nodes from source
    :param neptune_graph: A NeptuneGraph instance
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.

    :return: set(): The descendants of `source` in `G` at the given `distance` from `source`

    """

    # Process all parameters
    parameters: dict[str, Any] = {PARAM_MAX_DEPTH: distance}

    # Process NA specific parameters
    if traversal_direction is not None:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    query_str, para_map = descendants_at_distance_query(source, parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)

    # Transform response from list of dict to set.
    # Ex: [{'id(node)': 'Bob'}, {'id(node)': 'Carl'}] -> {'Bob', 'Carl'}
    return {item[_NODE_FULL_FORM_ID_FUNC_REF] for item in json_result}


@configure_if_nx_active()
def bfs_layers(
    neptune_graph: NeptuneGraph,
    sources,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
):
    """
    The BFS-Levels algorithm is executed on the Neptune Analytics graph,
    to achieve feature parity with the results generated by NetworkX.

    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/bfs-levels.html

    :param neptune_graph: A NeptuneGraph instance
    :param sources: Specify starting node for breadth-first levels search
    :param neptune_graph: A NeptuneGraph instance
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.

    :return: Yields list of nodes at the same distance from `sources`.
    """
    # Process all parameters
    parameters: dict[str, Any] = {}

    # Process NA specific parameters
    if traversal_direction is not None:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    query_str, para_map = bfs_layers_query(sources, parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)

    for json_item in json_result:
        yield json_item["id"]
