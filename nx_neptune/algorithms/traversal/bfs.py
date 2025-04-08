import logging

from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["bfs_edges"]


@configure_if_nx_active()
def bfs_edges(
    G: NeptuneGraph,
    source,
    reverse=False,
    depth_limit=None,
    sort_neighbors=None,
):
    """
    Executes a breadth first search from source.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/bfs-standard.html

    :param G:
    :param source:
    :param reverse:
    :param depth_limit:
    :param sort_neighbors:
    :return:
    """
    logger.debug(f"nx_neptune_analytics.bfs() with: \nG={G}\nsource={source}")
    if reverse:
        logger.debug(f"reverse={reverse}")
    if depth_limit:
        logger.debug(f"depth_limit={depth_limit}")
    if sort_neighbors:
        logger.debug(f"sort_neighbors={sort_neighbors}")

    where_clause = f"n.name='{source}'"
    parameters = {}
    # TODO: map all parameters
    if depth_limit:
        parameters["maxDepth"] = depth_limit
    if reverse:
        parameters["traversalDirection"] = '"inbound"'
    # TODO: map sort_neighbours
    logger.debug(f"where_clause={where_clause}")
    json_result = G.execute_algo_bfs("n", {"n.name": f"{source}"}, parameters)
    # TODO: move to object mapper
    nodes = list(map(lambda x: x["node"]["~properties"]["name"], json_result))
    return nodes
