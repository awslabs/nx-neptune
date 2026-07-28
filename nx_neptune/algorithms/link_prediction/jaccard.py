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

from nx_neptune.clients.neptune_constants import (
    PARAM_EDGE_LABELS,
    PARAM_TRAVERSAL_DIRECTION,
    PARAM_VERTEX_LABEL,
)
from nx_neptune.clients.opencypher_builder import jaccard_coefficient_query
from nx_neptune.na_graph import NeptuneGraph
from nx_neptune.utils.decorators import configure_if_nx_active

logger = logging.getLogger(__name__)

__all__ = ["jaccard_coefficient"]


@configure_if_nx_active()
def jaccard_coefficient(
    neptune_graph: NeptuneGraph,
    ebunch=None,
    edge_labels: Optional[List] = None,
    vertex_label: Optional[str] = None,
    traversal_direction: Optional[str] = None,
):
    """
    Compute the Jaccard coefficient of all node pairs in ebunch.
    Link: https://docs.aws.amazon.com/neptune-analytics/latest/userguide/jaccard-similarity.html

    The Jaccard coefficient measures the similarity between two sets of neighbors.
    It is defined as |Γ(u) ∩ Γ(v)| / |Γ(u) ∪ Γ(v)| where Γ(u) denotes the set
    of neighbors of u.

    :param neptune_graph: A NeptuneGraph instance
    :param ebunch: An iterable of node pairs (u, v). Jaccard coefficient will be
        computed for each pair. The pairs must be given as 2-tuples (u, v) where
        u and v are nodes in the graph. If ebunch is None then all nonexistent
        edges in the graph will be used. Note: when ebunch is None, the non-edges
        are computed locally from the NetworkX graph object before being sent to
        Neptune Analytics. For large graphs, prefer passing an explicit ebunch.
    :param edge_labels: To filter on one or more edge labels, provide a list of the
        ones to filter on. If no edgeLabels field is provided then all edge labels
        are processed during traversal.
    :param vertex_label: A vertex label for vertex filtering. If a vertex label is
        provided, only nodes matching the label are considered neighbors.
    :param traversal_direction: The direction of edge to follow. Must be one of:
        "inbound", "outbound", or "both". Default: "outbound".

    :return: An iterator of 3-tuples in the form (u, v, p) where (u, v) is a
        pair of nodes and p is their Jaccard coefficient.
    """

    parameters: dict[str, Any] = {}

    if edge_labels:
        parameters[PARAM_EDGE_LABELS] = edge_labels

    if vertex_label:
        parameters[PARAM_VERTEX_LABEL] = vertex_label

    if traversal_direction:
        parameters[PARAM_TRAVERSAL_DIRECTION] = traversal_direction

    if ebunch is None:
        # When ebunch is None, compute for all non-existent edges in the graph
        ebunch = _generate_non_edges(neptune_graph)

    # Collect pairs into lists for batched execution
    pairs = list(ebunch)
    if not pairs:
        return iter([])

    first_nodes = [str(u) for u, v in pairs]
    second_nodes = [str(v) for u, v in pairs]

    query_str, para_map = jaccard_coefficient_query(first_nodes, second_nodes, parameters)
    json_result = neptune_graph.execute_call(query_str, para_map)

    # Map results back to original pairs
    results = []
    for i, (u, v) in enumerate(pairs):
        if json_result and i < len(json_result):
            score = json_result[i].get("score", 0.0)
        else:
            score = 0.0
        results.append((u, v, score))

    return iter(results)


def _generate_non_edges(neptune_graph: NeptuneGraph):
    """
    Generate all non-existent edges for the graph.
    This retrieves all nodes and yields pairs that are not connected.
    """
    graph = neptune_graph.graph
    if graph is not None:
        import networkx as nx

        return nx.non_edges(graph)
    return iter([])
