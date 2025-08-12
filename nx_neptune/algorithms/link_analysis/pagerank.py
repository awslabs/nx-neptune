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
from typing import Any, Dict, List, Optional

from nx_neptune.algorithms.util import process_unsupported_param
from nx_neptune.algorithms.util.algorithm_utils import execute_mutation_query
from nx_neptune.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_DAMPING_FACTOR,
    PARAM_DANGLING,
    PARAM_EDGE_LABELS,
    PARAM_EDGE_WEIGHT_PROPERTY,
    PARAM_EDGE_WEIGHT_TYPE,
    PARAM_NSTART,
    PARAM_NUM_OF_ITERATIONS,
    PARAM_PERSONALIZATION,
    PARAM_SOURCE_NODES,
    PARAM_SOURCE_WEIGHTS,
    PARAM_TOLERANCE,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_VERTEX_LABEL,
    PARAM_WRITE_PROPERTY,
    RESPONSE_RANK,
)
from nx_neptune.clients.opencypher_builder import (
    _PAGERANK_MUTATE_ALG,
    Node,
    pagerank_mutation_query,
    pagerank_query,
)
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
    vertex_label: Optional[str] = None,
    edge_labels: Optional[List] = None,
    concurrency: Optional[int] = None,
    traversal_direction: Optional[str] = None,
    edge_weight_property: Optional[str] = None,
    edge_weight_type: Optional[str] = None,
    source_nodes: Optional[List] = None,
    source_weights: Optional[List] = None,
    write_property: Optional[str] = None,
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
    :param vertex_label: A vertex label for vertex filtering.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param edge_weight_property: The weight property to consider for weighted pageRank computation.
    :param edge_weight_type: required if edgeWeightProperty is present, valid values: "int", "long", "float", "double".
    :param source_nodes: If a vertexLabel is provided, nodes that do not have the given vertexLabel are ignored.
    :param source_weights: A personalization weight list. The weight distribution among the personalized vertices.
    :param write_property: Specifies the name of the node property that will store the computed pageRank values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/page-rank-mutate.html

    :return: Computation result of pagerank algorithm, or an empty dictionary when `write_property` is specified.

    Note: The parameters personalization, nstart, and dangling are not supported
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

    # Process NA specific parameters
    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    if traversal_direction is not None:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    # AWS options always take precedence
    if edge_weight_property:
        parameters[PARAM_EDGE_WEIGHT_PROPERTY] = edge_weight_property
    elif weight and weight != "weight":
        # Submit the weight-related property only when a custom value is provided in NX's field,
        # ensuring backward compatibility.
        parameters[PARAM_EDGE_WEIGHT_PROPERTY] = weight

    # AWS options always take precedence
    if edge_weight_type:
        parameters[PARAM_EDGE_WEIGHT_TYPE] = edge_weight_type
    elif edge_weight_property or (weight and weight != "weight"):
        # Submit the weight-related property only when a custom value is provided in NX's field,
        # ensuring backward compatibility.
        parameters[PARAM_EDGE_WEIGHT_TYPE] = "float"

    if personalization and source_nodes and source_weights:
        logger.warning(
            "Since personalization and both source_nodes and source_weights are provided, "
            "Neptune Analytics options will take precedence."
        )

    if (source_nodes is None) != (source_weights is None):
        logger.warning(
            "source_nodes and source_weights must be provided together. "
            "If only one is specified, both parameters will be ignored"
        )

    if source_nodes and source_weights:
        parameters[PARAM_SOURCE_NODES] = source_nodes
        parameters[PARAM_SOURCE_WEIGHTS] = source_weights

    elif personalization:
        parameters[PARAM_SOURCE_NODES] = list(personalization.keys())
        parameters[PARAM_SOURCE_WEIGHTS] = list(personalization.values())

    # Process unsupported parameters (for warnings only)
    process_unsupported_param(
        {
            PARAM_NSTART: nstart,
            PARAM_DANGLING: dangling,
        }
    )

    # Execute PageRank algorithm
    if parameters is None:
        parameters = {}

    if write_property:
        parameters[PARAM_WRITE_PROPERTY] = write_property
        return execute_mutation_query(
            neptune_graph,
            parameters,
            _PAGERANK_MUTATE_ALG,
            pagerank_mutation_query,
        )

    query_str, para_map = pagerank_query(parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)

    # Convert the result to a dictionary of node:pagerank pairs
    result = {}
    for item in json_result:
        node = Node.from_neptune_response(item["n"])
        result[node.id] = item[RESPONSE_RANK]

    return result
