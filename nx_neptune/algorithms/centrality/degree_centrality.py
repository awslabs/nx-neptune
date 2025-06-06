import logging
from typing import Any, List, Optional

from nx_neptune.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    PARAM_VERTEX_LABEL,
    RESPONSE_DEGREE,
    RESPONSE_ID,
)
from nx_neptune.clients.opencypher_builder import degree_centrality_query
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["degree_centrality", "in_degree_centrality", "out_degree_centrality"]


@configure_if_nx_active()
def degree_centrality(
    neptune_graph: NeptuneGraph,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
):
    """
    Compute the degree centrality for nodes.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree.html

    :param neptune_graph: A NeptuneGraph instance
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    """
    return _degree_centrality(
        neptune_graph,
        None,
        vertex_label,
        edge_labels,
        concurrency,
    )


@configure_if_nx_active()
def in_degree_centrality(
    neptune_graph: NeptuneGraph,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
):
    """
    Executes Degree algorithm on the graph with inbound edges.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree.html

    :param neptune_graph: A NeptuneGraph instance
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    """
    return _degree_centrality(
        neptune_graph,
        PARAM_TRAVERSAL_DIRECTION_INBOUND,
        vertex_label,
        edge_labels,
        concurrency,
    )


@configure_if_nx_active()
def out_degree_centrality(
    neptune_graph: NeptuneGraph,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
):
    """
    Executes Degree algorithm on the graph with inbound edges.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree.html

    :param neptune_graph: A NeptuneGraph instance
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    """
    return _degree_centrality(
        neptune_graph,
        PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
        vertex_label,
        edge_labels,
        concurrency,
    )


def _degree_centrality(
    neptune_graph: NeptuneGraph,
    traversal_direction: Optional[str] = None,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
):
    """
    Compute the degree centrality for nodes.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree.html

    :param neptune_graph: A NeptuneGraph instance
    :param traversal_direction: The direction of edge to follow.
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    """
    logger.debug(
        f"nx_neptune.degree_centrality() with: \nneptune_graph={neptune_graph}"
    )

    # Process all parameters
    parameters: dict[str, Any] = {}

    # Process NA specific parameters
    if traversal_direction:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    # Execute PageRank algorithm
    if parameters is None:
        parameters = {}
    query_str, para_map = degree_centrality_query(parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)
    node_count = neptune_graph.graph.number_of_nodes()

    # Convert the result to a dictionary of node.id:degree pairs
    result = {}
    for item in json_result:
        # Normalised value to be compatible with NX implementation
        result[item[RESPONSE_ID]] = item[RESPONSE_DEGREE] / (node_count - 1)

    return result
