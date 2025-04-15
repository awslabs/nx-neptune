import logging
from typing import Any, Dict, Optional

from nx_neptune.algorithms.util.algorithm_utils import process_unsupported_param
from nx_neptune.clients.opencypher_builder import Node
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["pagerank"]


@configure_if_nx_active()
def pagerank(
    G: NeptuneGraph,
    alpha: float,
    personalization: Optional[Dict],
    max_iter: int,
    tol: float,
    nstart: Optional[Dict],
    weight: str,
    dangling: Optional[Dict] = None,
):
    """
    Executes PageRank algorithm on the graph.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/page-rank.html

    :param G: A NeptuneGraph instance
    :param alpha: Damping parameter for PageRank
    :param personalization: Dict with nodes as keys and personalization values (not supported in Neptune Analytics)
    :param max_iter: Maximum number of iterations
    :param tol: Error tolerance to check convergence
    :param nstart: Dict with nodes as keys and initial PageRank values (not supported in Neptune Analytics)
    :param weight: Edge attribute to use as weight (not supported in Neptune Analytics)
    :param dangling: Dict with nodes as keys and dangling values (not supported in Neptune Analytics)
    :return: Dict of nodes with PageRank as value

    Note: The parameters personalization, nstart, weight, and dangling are not supported
    in the Neptune Analytics implementation and will be ignored if provided.
    """
    logger.debug(f"nx_neptune_analytics.pagerank() with: \nG={G}")

    # Process all parameters
    parameters: dict[str, Any] = {}

    if alpha and alpha != 0.85:
        parameters["dampingFactor"] = alpha

    # # 20 is Neptune default
    if max_iter and max_iter != 100:
        parameters["numOfIterations"] = max_iter

    if tol and tol != 1e-06:
        parameters["tolerance"] = tol

    # Process unsupported parameters (for warnings only)
    process_unsupported_param(
        {
            "weight": weight,
            "personalization": personalization,
            "nstart": nstart,
            "dangling": dangling,
        }
    )

    # Execute PageRank algorithm
    json_result = G.execute_algo_pagerank(parameters)

    # Convert the result to a dictionary of node:pagerank pairs
    result = {}
    for item in json_result:
        node = Node.from_neptune_response(item["n"])
        node_name = node.by_name()
        result[node_name] = item["rank"]

    return result
