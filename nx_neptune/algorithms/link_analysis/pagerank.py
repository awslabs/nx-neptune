import logging
from typing import Any, Dict, Optional

from nx_neptune.algorithms.util import process_unsupported_param
from nx_neptune.clients.neptune_constants import (
    PARAM_DAMPING_FACTOR,
    PARAM_DANGLING,
    PARAM_NSTART,
    PARAM_NUM_OF_ITERATIONS,
    PARAM_PERSONALIZATION,
    PARAM_TOLERANCE,
    PARAM_WEIGHT,
    RESPONSE_RANK,
)
from nx_neptune.clients.opencypher_builder import Node, pagerank_query
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["pagerank"]


@configure_if_nx_active()
def pagerank(
    neptune_graph: NeptuneGraph,
    alpha: float,
    personalization: Optional[Dict],
    max_iter: int,
    tol: float,
    nstart: Optional[Dict],
    weight: Optional[str] = None,
    dangling: Optional[Dict] = None,
):
    """
    Executes PageRank algorithm on the graph.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/page-rank.html

    :param neptune_graph: A NeptuneGraph instance
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
    logger.debug(f"nx_neptune.pagerank() with: \nneptune_graph={neptune_graph}")

    # Process all parameters
    parameters: dict[str, Any] = {}

    if alpha and alpha != 0.85:
        parameters[PARAM_DAMPING_FACTOR] = alpha

    # # 20 is Neptune default
    if max_iter and max_iter != 100:
        parameters[PARAM_NUM_OF_ITERATIONS] = max_iter

    if tol and tol != 1e-06:
        parameters[PARAM_TOLERANCE] = tol

    # Process unsupported parameters (for warnings only)
    process_unsupported_param(
        {
            PARAM_WEIGHT: weight,
            PARAM_PERSONALIZATION: personalization,
            PARAM_NSTART: nstart,
            PARAM_DANGLING: dangling,
        }
    )

    # Execute PageRank algorithm
    if parameters is None:
        parameters = {}
    query_str, para_map = pagerank_query(parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)

    # Convert the result to a dictionary of node:pagerank pairs
    result = {}
    for item in json_result:
        node = Node.from_neptune_response(item["n"])
        node_name = node.by_name()
        result[node_name] = item[RESPONSE_RANK]

    return result
