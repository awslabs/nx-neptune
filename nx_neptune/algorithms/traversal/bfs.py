import logging

from nx_neptune.na_graph import Edge, NeptuneGraph
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
        f"nx_neptune_analytics.bfs() with: \nneptune_graph={neptune_graph}\nsource={source}\n"
        f"reverse={reverse}\n"
        f"depth_limit={depth_limit}\n"
        f"sort_neighbors={sort_neighbors}"
    )

    # determine source for match call
    if isinstance(source, str):
        where_clause = f"n.name='{source}'"
    else:
        where_clause = f"n.name={source}"

    # map parameters
    parameters = {}
    if depth_limit:
        parameters["maxDepth"] = depth_limit
    parameters["traversalDirection"] = neptune_graph.traversal_direction(reverse)

    # TODO: map sort_neighbours
    logger.debug(f"where_clause={where_clause}")
    json_result = neptune_graph.execute_algo_bfs("n", {"n.name": source}, parameters)

    for json_edge in json_result:
        edge = Edge.from_neptune_response(json=json_edge)
        if edge.node_src == edge.node_dest:
            continue
        yield edge.to_list()
