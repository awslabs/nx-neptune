import logging

from nx_neptune.clients import Edge
from nx_neptune.clients.neptune_constants import (
    PARAM_MAX_DEPTH,
    PARAM_TRAVERSAL_DIRECTION,
)
from nx_neptune.clients.opencypher_builder import bfs_query
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["bfs_edges"]


@configure_if_nx_active()
def bfs_edges(
    neptune_graph: NeptuneGraph,
    source,
    reverse=False,
    depth_limit=None,
    sort_neighbors=None,
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

    Yields
    ------
    edge
        Edges in the breadth-first search starting from `source`.
    """
    logger.debug(
        f"nx_neptune_analytics.bfs_edges() with: \nneptune_graph={neptune_graph}\nsource={source}\n"
        f"reverse={reverse}\n"
        f"depth_limit={depth_limit}\n"
        f"sort_neighbors={sort_neighbors}"
    )

    source_node = "n"

    parameters = {}
    # map parameters:
    if depth_limit:
        parameters[PARAM_MAX_DEPTH] = depth_limit
    parameters[PARAM_TRAVERSAL_DIRECTION] = neptune_graph.traversal_direction(reverse)
    # TODO: map sort_neighbours

    query_str, para_map = bfs_query(
        source_node, {f"{source_node}.name": source}, parameters
    )
    json_result = neptune_graph.execute_call(query_str, para_map)

    for json_edge in json_result:
        edge = Edge.from_neptune_response(json=json_edge)
        # Neptune returns a result with source node -> source node - skip it
        if edge.node_src == edge.node_dest:
            continue
        yield edge.to_list()
