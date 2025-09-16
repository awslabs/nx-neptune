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
from .config import NeptuneConfig, _config

__all__ = ["NeptuneConfig"]

# Entries between BEGIN and END are automatically generated
_info = {
    "backend_name": "neptune",
    "project": "nx-neptune",
    "package": "nx_neptune",
    "url": "https://github.com/awslabs/nx-neptune",
    "short_summary": "Neptune computation backend for NetworkX.",
    "description": "Scale graph algorithms on AWS Neptune Analytics platform.",
    "functions": {
        # BEGIN: functions
        "bfs_edges",
        "descendants_at_distance",
        "bfs_layers",
        "pagerank",
        "degree_centrality",
        "in_degree_centrality",
        "out_degree_centrality",
        "closeness_centrality",
        "label_propagation_communities",
        "fast_label_propagation_communities",
        "asyn_lpa_communities",
        "louvain_communities",
        # END: functions
    },
    "additional_docs": {
        # BEGIN: additional_docs
        "pagerank": """Neptune Analytics recommends using a max_iter value of 20 for PageRank
            calculations, which balances computational efficiency with result accuracy. This
            default setting is optimized for most graph workloads, though you can adjust it
            based on your specific convergence requirements. Please note that the
            personalization, nstart, weight, and dangling parameters are not supported at
            the moment.""",
        "asyn_lpa_communities": """
        The seed parameter is not supported at the moment.
        Also, label propagation in Neptune Analytics maps all NetworkX variants to the same algorithm,
        using a fixed label update strategy.
        Variant-specific control over the update method (e.g., synchronous vs. asynchronous) is not configurable.""",
        "fast_label_propagation_communities": """
        Please note that the seed parameter is not supported at the moment,
        also label propagation in Neptune Analytics maps all NetworkX variants to the same algorithm,
        using a fixed label update strategy.
        Variant-specific control over the update method (e.g., synchronous vs. asynchronous) is not configurable.""",
        "closeness_centrality": """
        Please note that the distance parameter is not supported.""",
        "louvain_communities": """
        Please note that the resolution and seed parameters are not supported at the moment.
        """,
        # END: additional_docs
    },
    "additional_parameters": {
        # BEGIN: additional_parameters
        "bfs_edges": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#bfs_edges)
        """,
        "descendants_at_distance": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#descendants_at_distance)
        """,
        "bfs_layers": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#bfs_layers)
        """,
        "pagerank": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#pagerank)
        """,
        "degree_centrality": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#degree_centrality)
        """,
        "in_degree_centrality": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#in_degree_centrality)
        """,
        "out_degree_centrality": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#out_degree_centrality)
        """,
        "closeness_centrality": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#closeness_centrality)
        """,
        "label_propagation_communities": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#label_propagation_communities)
        """,
        "fast_label_propagation_communities": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#fast_label_propagation_communities)
        """,
        "asyn_lpa_communities": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#asyn_lpa_communities)
        """,
        "louvain_communities": """
        For additional parameters, see [nx-neptune](https://github.com/awslabs/nx-neptune/blob/main/Algorithms.md#louvain_communities)
        """
        # END: additional_parameters
    },
}


def get_info():
    """
    Target of ``networkx.plugin_info`` entry point.
    This tells NetworkX about the Neptune Analytics backend without importing
    nx_neptune
    """

    d = _info.copy()
    info_keys = {"additional_docs", "additional_parameters"}
    d["functions"] = {
        func: {
            info_key: vals[func]
            for info_key in info_keys
            if func in (vals := d[info_key])
        }
        for func in d["functions"]
    }
    # Add keys for Networkx <3.3
    for func_info in d["functions"].values():
        if "additional_docs" in func_info:
            func_info["extra_docstring"] = func_info["additional_docs"]
        if "additional_parameters" in func_info:
            func_info["extra_parameters"] = func_info["additional_parameters"]

    for key in info_keys:
        del d[key]

    d["default_config"] = _config

    return d
