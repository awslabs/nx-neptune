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
    PARAM_MAX_ITERATIONS,
    PARAM_SEED,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_VERTEX_LABEL,
    PARAM_VERTEX_WEIGHT_PROPERTY,
    PARAM_VERTEX_WEIGHT_TYPE,
    PARAM_WRITE_PROPERTY,
)
from nx_neptune.clients.opencypher_builder import (
    _LABEL_MUTATE_ALG,
    label_propagation_mutation_query,
    label_propagation_query,
)
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = [
    "label_propagation_communities",
    "fast_label_propagation_communities",
    "asyn_lpa_communities",
]


@configure_if_nx_active()
def fast_label_propagation_communities(
    neptune_graph: NeptuneGraph,
    *,
    weight=None,
    seed=None,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    vertex_weight_property: Optional[str] = None,
    vertex_weight_type: Optional[str] = None,
    edge_weight_property: Optional[str] = None,
    edge_weight_type: Optional[str] = None,
    max_iterations: Optional[int] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Executes labelPropagation algorithm on the graph.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation.html

    :param neptune_graph: A NeptuneGraph instance
    :param weight: The edge attribute representing a non-negative weight of an edge.
    If None, each edge is assumed to have weight one.
    :param seed: Indicator of random number generation state.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param vertex_weight_property: The node weight used in Label Propagation.
    When vertexWeightProperty is not specified, each node's communityId is treated equally.
    When the vertexWeightProperty is specified without an edgeWeightProperty,
    the weight of the communityId for each node is the value of the node weight property.
    When both vertexWeightProperty and edgeWeightProperty are specified,
    the weight of the communityId is the product of the node property value and edge property value.
    :param vertex_weight_type: The type of the numeric values in the node property specified by vertexWeightProperty.
    If vertexWeightProperty is not provided, vertexWeightType is ignored.
    If a node contains a numeric property with the name specified by vertexWeightProperty,
    but its value is a different numeric type than is specified by vertexWeightType,
    the value is typecast to the type specified by vertexWeightType.
    If both vertexWeightType and edgeWeightType are given,
    the type specified by edgeWeightType is used for both node and edge properties.
    :param edge_weight_property: The weight property to consider for weighted pageRank computation.
    :param edge_weight_type: required if edgeWeightProperty is present, valid values: "int", "long", "float", "double".
    :param max_iterations: The maximum number of iterations to run.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation-mutate.html

    :return: Dictionary of community items to members, in the form of dict_value.

    """

    # Process unsupported parameters (for warnings only)
    process_unsupported_param({PARAM_SEED: seed})

    return _label_propagation_communities(
        weight=weight,
        neptune_graph=neptune_graph,
        edge_labels=edge_labels,
        vertex_label=vertex_label,
        vertex_weight_property=vertex_weight_property,
        vertex_weight_type=vertex_weight_type,
        edge_weight_property=edge_weight_property,
        edge_weight_type=edge_weight_type,
        max_iterations=max_iterations,
        traversal_direction=traversal_direction,
        concurrency=concurrency,
        write_property=write_property,
    )


@configure_if_nx_active()
def asyn_lpa_communities(
    neptune_graph: NeptuneGraph,
    weight=None,
    seed=None,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    vertex_weight_property: Optional[str] = None,
    vertex_weight_type: Optional[str] = None,
    edge_weight_property: Optional[str] = None,
    edge_weight_type: Optional[str] = None,
    max_iterations: Optional[int] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Executes labelPropagation algorithm on the graph.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation.html

    :param neptune_graph: A NeptuneGraph instance
    :param weight: The edge attribute representing a non-negative weight of an edge.
    If None, each edge is assumed to have weight one.
    :param seed: Indicator of random number generation state.
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param vertex_weight_property: The node weight used in Label Propagation.
    When vertexWeightProperty is not specified, each node's communityId is treated equally.
    When the vertexWeightProperty is specified without an edgeWeightProperty,
    the weight of the communityId for each node is the value of the node weight property.
    When both vertexWeightProperty and edgeWeightProperty are specified,
    the weight of the communityId is the product of the node property value and edge property value.
    :param vertex_weight_type: The type of the numeric values in the node property specified by vertexWeightProperty.
    If vertexWeightProperty is not provided, vertexWeightType is ignored.
    If a node contains a numeric property with the name specified by vertexWeightProperty,
    but its value is a different numeric type than is specified by vertexWeightType,
    the value is typecast to the type specified by vertexWeightType.
    If both vertexWeightType and edgeWeightType are given,
    the type specified by edgeWeightType is used for both node and edge properties.
    :param edge_weight_property: The weight property to consider for weighted pageRank computation.
    :param edge_weight_type: required if edgeWeightProperty is present, valid values: "int", "long", "float", "double".
    :param max_iterations: The maximum number of iterations to run.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation-mutate.html


    :return: Dictionary of community items to members, in the form of dict_value.

    """

    # Process unsupported parameters (for warnings only)
    process_unsupported_param({PARAM_SEED: seed})

    return _label_propagation_communities(
        weight=weight,
        neptune_graph=neptune_graph,
        edge_labels=edge_labels,
        vertex_label=vertex_label,
        vertex_weight_property=vertex_weight_property,
        vertex_weight_type=vertex_weight_type,
        edge_weight_property=edge_weight_property,
        edge_weight_type=edge_weight_type,
        max_iterations=max_iterations,
        traversal_direction=traversal_direction,
        concurrency=concurrency,
        write_property=write_property,
    )


@configure_if_nx_active()
def label_propagation_communities(
    neptune_graph: NeptuneGraph,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    vertex_weight_property: Optional[str] = None,
    vertex_weight_type: Optional[str] = None,
    edge_weight_property: Optional[str] = None,
    edge_weight_type: Optional[str] = None,
    max_iterations: Optional[int] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Executes labelPropagation algorithm on the graph.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation.html

    :param neptune_graph: A NeptuneGraph instance
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param vertex_weight_property: The node weight used in Label Propagation.
    When vertexWeightProperty is not specified, each node's communityId is treated equally.
    When the vertexWeightProperty is specified without an edgeWeightProperty,
    the weight of the communityId for each node is the value of the node weight property.
    When both vertexWeightProperty and edgeWeightProperty are specified,
    the weight of the communityId is the product of the node property value and edge property value.
    :param vertex_weight_type: The type of the numeric values in the node property specified by vertexWeightProperty.
    If vertexWeightProperty is not provided, vertexWeightType is ignored.
    If a node contains a numeric property with the name specified by vertexWeightProperty,
    but its value is a different numeric type than is specified by vertexWeightType,
    the value is typecast to the type specified by vertexWeightType.
    If both vertexWeightType and edgeWeightType are given,
    the type specified by edgeWeightType is used for both node and edge properties.
    :param edge_weight_property: The weight property to consider for weighted computation.
    :param edge_weight_type: required if edgeWeightProperty is present, valid values: "int", "long", "float", "double".
    :param max_iterations: The maximum number of iterations to run.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation-mutate.html


    :return: Dictionary of community items to members, in the form of dict_value.

    """

    return _label_propagation_communities(
        neptune_graph=neptune_graph,
        edge_labels=edge_labels,
        vertex_label=vertex_label,
        vertex_weight_property=vertex_weight_property,
        vertex_weight_type=vertex_weight_type,
        edge_weight_property=edge_weight_property,
        edge_weight_type=edge_weight_type,
        max_iterations=max_iterations,
        traversal_direction=traversal_direction,
        concurrency=concurrency,
        write_property=write_property,
    )


def _label_propagation_communities(
    neptune_graph: NeptuneGraph,
    weight=None,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    vertex_weight_property: Optional[str] = None,
    vertex_weight_type: Optional[str] = None,
    edge_weight_property: Optional[str] = None,
    edge_weight_type: Optional[str] = None,
    max_iterations: Optional[int] = None,
    traversal_direction: Optional[str] = None,
    concurrency: Optional[int] = None,
    write_property: Optional[str] = None,
):
    """
    Executes labelPropagation algorithm on the graph.
    link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation.html

    :param neptune_graph: A NeptuneGraph instance
    :param edge_labels: To filter on one more edge labels, provide a list of the ones to filter on.
    If no edgeLabels field is provided then all edge labels are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering.
    :param vertex_weight_property: The node weight used in Label Propagation.
    When vertexWeightProperty is not specified, each node's communityId is treated equally.
    When the vertexWeightProperty is specified without an edgeWeightProperty,
    the weight of the communityId for each node is the value of the node weight property.
    When both vertexWeightProperty and edgeWeightProperty are specified,
    the weight of the communityId is the product of the node property value and edge property value.
    :param vertex_weight_type: The type of the numeric values in the node property specified by vertexWeightProperty.
    If vertexWeightProperty is not provided, vertexWeightType is ignored.
    If a node contains a numeric property with the name specified by vertexWeightProperty,
    but its value is a different numeric type than is specified by vertexWeightType,
    the value is typecast to the type specified by vertexWeightType.
    If both vertexWeightType and edgeWeightType are given,
    the type specified by edgeWeightType is used for both node and edge properties.
    :param edge_weight_property: The weight property to consider for weighted computation.
    :param edge_weight_type: required if edgeWeightProperty is present, valid values: "int", "long", "float", "double".
    :param max_iterations: The maximum number of iterations to run.
    :param traversal_direction: The direction of edge to follow. Must be one of: "outbound" or "inbound".
    :param concurrency: Controls the number of concurrent threads used to run the algorithm.
    :param write_property: Specifies the name of the node property that will store the computed degree values.
    For comprehensive usage details,
    refer to: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/label-propagation-mutate.html

    :return: Dictionary of community items to members, in the form of dict_value.

    """

    # Process all parameters
    parameters: dict[str, Any] = {}

    # Process NA specific parameters
    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if vertex_weight_property:
        parameters[PARAM_VERTEX_WEIGHT_PROPERTY] = vertex_weight_property

    if vertex_weight_type:
        parameters[PARAM_VERTEX_WEIGHT_TYPE] = vertex_weight_type

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

    if max_iterations is not None:
        parameters[PARAM_MAX_ITERATIONS] = max_iterations

    if traversal_direction is not None:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    if concurrency is not None:
        parameters[PARAM_CONCURRENCY] = concurrency

    if write_property:
        parameters[PARAM_WRITE_PROPERTY] = write_property
        return execute_mutation_query(
            neptune_graph,
            parameters,
            _LABEL_MUTATE_ALG,
            label_propagation_mutation_query,
        )
    else:
        query_str, para_map = label_propagation_query(parameters)
        json_result = neptune_graph.execute_call(query_str, para_map)

        result = {}
        for item in json_result:
            result[item["community"]] = item["members"]
        # Return dict_value to match NX return type.
        return result.values()
