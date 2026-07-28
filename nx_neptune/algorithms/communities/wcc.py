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
    PARAM_VERTEX_LABEL,
    PARAM_WRITE_PROPERTY,
)
from nx_neptune.clients.opencypher_builder import (
    _WCC_MUTATE_ALG,
    wcc_mutation_query,
    wcc_query,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = [
    "weakly_connected_components",
]


@configure_if_nx_active()
def weakly_connected_components(
    neptune_graph: NeptuneGraph,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Find the weakly connected components in a directed graph.
    Link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/wcc.html

    A weakly-connected component is a group of nodes in which every node is reachable
    from every other node when edge directions are ignored.

    :param neptune_graph: A NeptuneGraph instance
    :param edge_labels: To filter on one or more edge labels, provide a list of the ones to filter on.
        If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering. If provided, only nodes matching
        the label are considered.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
        If set to 0, uses all available threads. If set to 1, uses a single thread.
    :param write_property: Specifies the name of the node property that will store the computed
        component ID values. When specified, runs the mutate variant of the algorithm.
        For details, see: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/wcc-mutate.html

    :return: A list of sets of nodes, one set per weakly connected component.
        Returns an empty dictionary when write_property is specified.
    """
    # Build parameters dictionary
    parameters: dict[str, Any] = {}

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    if write_property:
        parameters[PARAM_WRITE_PROPERTY] = write_property
        return execute_mutation_query(
            neptune_graph,
            parameters,
            _WCC_MUTATE_ALG,
            wcc_mutation_query,
        )

    query_str, para_map = wcc_query(parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)

    result = []
    for item in json_result:
        result.append(set(item["members"]))
    return result
