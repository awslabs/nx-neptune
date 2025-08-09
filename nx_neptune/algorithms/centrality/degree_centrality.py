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

from nx_neptune.algorithms.util.algorithm_utils import execute_mutation_query
from nx_neptune.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    PARAM_VERTEX_LABEL,
    PARAM_WRITE_PROPERTY,
    RESPONSE_DEGREE,
    RESPONSE_ID,
)
from nx_neptune.clients.opencypher_builder import (
    _DEGREE_MUTATE_ALG,
    degree_centrality_mutation_query,
    degree_centrality_query,
)
from nx_neptune.na_graph import NeptuneGraph, get_config
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["degree_centrality", "in_degree_centrality", "out_degree_centrality"]


@configure_if_nx_active()
def degree_centrality(
    neptune_graph: NeptuneGraph,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Compute the degree centrality for nodes.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree.html

    :param neptune_graph: A NeptuneGraph instance
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree-mutate.html
    """
    return _degree_centrality(
        neptune_graph, None, vertex_label, edge_labels, concurrency, write_property
    )


@configure_if_nx_active()
def in_degree_centrality(
    neptune_graph: NeptuneGraph,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Executes Degree algorithm on the graph with inbound edges.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree.html

    :param neptune_graph: A NeptuneGraph instance
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree-mutate.html
    """
    return _degree_centrality(
        neptune_graph,
        PARAM_TRAVERSAL_DIRECTION_INBOUND,
        vertex_label,
        edge_labels,
        concurrency,
        write_property,
    )


@configure_if_nx_active()
def out_degree_centrality(
    neptune_graph: NeptuneGraph,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Executes Degree algorithm on the graph with inbound edges.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree.html

    :param neptune_graph: A NeptuneGraph instance
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree-mutate.html
    """
    return _degree_centrality(
        neptune_graph,
        PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
        vertex_label,
        edge_labels,
        concurrency,
        write_property,
    )


def _degree_centrality(
    neptune_graph: NeptuneGraph,
    traversal_direction: Optional[str] = None,
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
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
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/degree-mutate.html
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

    if write_property:
        parameters[PARAM_WRITE_PROPERTY] = write_property
        return execute_mutation_query(
            neptune_graph,
            parameters,
            _DEGREE_MUTATE_ALG,
            degree_centrality_mutation_query,
        )
    else:
        query_str, para_map = degree_centrality_query(parameters)
        json_result = neptune_graph.execute_call(query_str, para_map)

        # Convert the result to a dictionary of node.id:degree pairs
        result = {}
        node_count = neptune_graph.graph.number_of_nodes()
        for item in json_result:
            # Normalised value to be compatible with NX implementation
            result[item[RESPONSE_ID]] = item[RESPONSE_DEGREE] / (node_count - 1)

        return result
