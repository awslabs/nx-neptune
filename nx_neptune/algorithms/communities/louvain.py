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

from nx_neptune.algorithms.util import process_unsupported_param
from nx_neptune.algorithms.util.algorithm_utils import execute_mutation_query
from nx_neptune.clients.neptune_constants import (
    PARAM_CONCURRENCY,
    PARAM_EDGE_LABELS,
    PARAM_EDGE_WEIGHT_PROPERTY,
    PARAM_EDGE_WEIGHT_TYPE,
    PARAM_ITERATION_TOLERANCE,
    PARAM_LEVEL_TOLERANCE,
    PARAM_MAX_ITERATIONS,
    PARAM_MAX_LEVEL,
    PARAM_RESOLUTION,
    PARAM_SEED,
    PARAM_WRITE_PROPERTY,
)
from nx_neptune.clients.opencypher_builder import (
    _LOUVAIN_MUTATE_ALG,
    louvain_mutation_query,
    louvain_query,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = [
    "louvain_communities",
]


@configure_if_nx_active()
def louvain_communities(
    neptune_graph: NeptuneGraph,
    weight: str,
    resolution: float,
    threshold: float,
    max_level: Optional[int],
    seed: Optional[int],
    edge_weight_property: Optional[str] = None,
    edge_weight_type: Optional[str] = None,
    edge_labels: Optional[List] = None,
    max_iterations: Optional[int] = None,
    concurrency: Optional[int] = None,
    level_tolerance: Optional[float] = None,
    write_property: Optional[str] = None,
):
    """
    Executes Louvain algorithm on the graph.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/louvain.html

    :param neptune_graph: A NeptuneGraph instance
    :param weight: The edge attribute representing a non-negative weight of an edge.
    :param resolution: (not supported in Neptune Analytics)
    If resolution is less than 1, the algorithm favors larger communities. Greater than 1 favors smaller communities
    :param threshold: Modularity gain threshold for each level.
    If the gain of modularity between 2 levels of the algorithm is less than the given threshold,
    then the algorithm stops and returns the resulting communities.
    :param max_level: The minimum change in modularity required to continue to the next level.
    :param seed: (not supported in Neptune Analytics) Indicator of random number generation state.
    :param edge_weight_property: The weight property to consider for weighted pageRank computation.
    :param edge_weight_type: required if edgeWeightProperty is present, valid values: "int", "long", "float", "double".
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param max_iterations: The maximum number of iterations to run.
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param level_tolerance: The minimum change in modularity required to continue to the next level.
    :param write_property: Specifies the name of the node property that will store the computed group id values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/louvain-mutate.html

    :return: Dictionary of community items to members, in the form of list of set.

    Note: The parameters resolution and seed are not supported
    in the Neptune Analytics implementation and will be ignored if provided.

    """
    # Process all parameters
    parameters: dict[str, Any] = {}

    # Process unsupported parameters (for warnings only)
    process_unsupported_param({PARAM_SEED: seed, PARAM_RESOLUTION: resolution})

    # NX parameters
    if max_level:
        parameters[PARAM_MAX_LEVEL] = max_level

    if threshold:
        parameters[PARAM_ITERATION_TOLERANCE] = threshold

    # AWS options always take precedence
    if edge_weight_property:
        parameters[PARAM_EDGE_WEIGHT_PROPERTY] = edge_weight_property
    elif weight and weight != "weight":
        # Submit the weight-related property only when a custom value is provided in NX's field,
        # ensuring backward compatibility.
        parameters[PARAM_EDGE_WEIGHT_PROPERTY] = weight

    if edge_weight_type:
        parameters[PARAM_EDGE_WEIGHT_TYPE] = edge_weight_type
    elif edge_weight_property or (weight and weight != "weight"):
        # Submit the weight-related property only when a custom value is provided in NX's field,
        # ensuring backward compatibility.
        parameters[PARAM_EDGE_WEIGHT_TYPE] = "float"

    # Process NA specific parameters
    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    if max_iterations is not None:
        parameters[PARAM_MAX_ITERATIONS] = max_iterations

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if level_tolerance:
        parameters[PARAM_LEVEL_TOLERANCE] = level_tolerance

    if write_property:
        parameters[PARAM_WRITE_PROPERTY] = write_property
        return execute_mutation_query(
            neptune_graph,
            parameters,
            _LOUVAIN_MUTATE_ALG,
            louvain_mutation_query,
        )

    query_str, para_map = louvain_query(parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)

    result = []
    for item in json_result:
        result.append(set(item["members"]))
    return result
