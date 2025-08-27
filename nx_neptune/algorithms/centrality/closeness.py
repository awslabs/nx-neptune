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

from nx_neptune.algorithms.util.algorithm_utils import (
    execute_mutation_query,
    process_unsupported_param,
)
from nx_neptune.clients.neptune_constants import (
    MAX_INT,
    PARAM_CONCURRENCY,
    PARAM_DISTANCE,
    PARAM_EDGE_LABELS,
    PARAM_NORMALIZE,
    PARAM_NUM_SOURCES,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_VERTEX_LABEL,
    PARAM_WRITE_PROPERTY,
)
from nx_neptune.clients.opencypher_builder import (
    _CLOSENESS_MUTATE_ALG,
    closeness_centrality_mutation_query,
    closeness_centrality_query,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["closeness_centrality"]


@configure_if_nx_active()
def closeness_centrality(
    neptune_graph: NeptuneGraph,
    u=None,
    distance=None,
    wf_improved=True,
    num_sources: Optional[int] = None,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Compute the closeness centrality for nodes.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/closeness-centrality.html

    :param neptune_graph: A NeptuneGraph instance
    :param u: Optional; limits the computation scope to the specified node.
    If omitted, the computation is performed over the entire graph.
    :param distance: (Unsupported) Specifies the edge attribute to use as the distance in the shortest path calculations.
    :param wf_improved: If True, scale by the fraction of nodes reachable.
    This gives the Wasserman and Faust improved formula.
    For single component graphs it is the same as the original formula.
    :param num_sources: The number of source nodes to use for approximating closeness centrality.
    If omitted, defaults to `maxInt`, resulting in exact closeness centrality computation.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed group id values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/closeness-centrality-mutate.html

    :return: Dictionary with the pair of node ID as key and closeness centrality score as value.

    Note: The parameter distance is not supported
    in the Neptune Analytics implementation and will be ignored if provided.
    """

    # Add ref
    parameters: dict[str, Any] = {}

    # Process unsupported parameters (for warnings only)
    process_unsupported_param({PARAM_DISTANCE: distance})

    # Process NA specific parameters
    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if traversal_direction:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    if num_sources:
        parameters[PARAM_NUM_SOURCES] = num_sources
    else:
        # Ref: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/closeness-centrality.html
        # To compute exact closeness centrality, set numSources to a number larger than number of nodes, such as maxInt.
        parameters[PARAM_NUM_SOURCES] = MAX_INT

    if wf_improved is not None:
        parameters[PARAM_NORMALIZE] = wf_improved

    if write_property:
        parameters[PARAM_WRITE_PROPERTY] = write_property
        return execute_mutation_query(
            neptune_graph,
            parameters,
            _CLOSENESS_MUTATE_ALG,
            closeness_centrality_mutation_query,
        )

    query_str, para_map = closeness_centrality_query(parameters, u)
    json_result = neptune_graph.execute_call(query_str, para_map)

    result = {}
    for item in json_result:
        result[item["nodeId"]] = item["score"]
    return result
