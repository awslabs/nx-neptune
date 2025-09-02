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
from .algorithms import louvain_communities
from .algorithms.centrality.closeness import closeness_centrality
from .algorithms.centrality.degree_centrality import (
    degree_centrality,
    in_degree_centrality,
    out_degree_centrality,
)
from .algorithms.communities.label_propagation import (
    asyn_lpa_communities,
    fast_label_propagation_communities,
    label_propagation_communities,
)
from .algorithms.link_analysis.pagerank import pagerank
from .algorithms.traversal.bfs import bfs_edges, bfs_layers, descendants_at_distance
from .clients import Edge, Node
from .instance_management import (
    TaskFuture,
    create_na_instance,
    export_csv_to_s3,
    import_csv_from_s3,
)
from .interface import BackendInterface
from .na_graph import NETWORKX_GRAPH_ID, NETWORKX_S3_IAM_ROLE_ARN, NeptuneGraph
from .utils.decorators import configure_if_nx_active

__version__ = "0.4.1"

__all__ = [
    # environment variables
    "NETWORKX_GRAPH_ID",
    "NETWORKX_S3_IAM_ROLE_ARN",
    # algorithms
    "bfs_edges",
    "bfs_layers",
    "descendants_at_distance",
    "pagerank",
    "degree_centrality",
    "in_degree_centrality",
    "out_degree_centrality",
    "label_propagation_communities",
    "asyn_lpa_communities",
    "fast_label_propagation_communities",
    "louvain_communities",
    # graphs
    "Node",
    "Edge",
    "NeptuneGraph",
    # decorators
    "configure_if_nx_active",
    "BackendInterface",
    # instance management
    "import_csv_from_s3",
    "export_csv_to_s3",
    "create_na_instance",
    "TaskFuture",
]
