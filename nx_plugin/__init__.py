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

algorithms_url = "https://github.com/awslabs/nx-neptune/blob/main/algorithms.md"

_info = {
    "backend_name": "neptune",
    "project": "nx-neptune",
    "package": "nx_neptune",
    "url": "https://github.com/awslabs/nx-neptune",
    "short_summary": "Neptune computation backend for NetworkX.",
    "description": "Scale graph algorithms on AWS Neptune Analytics platform.",
    "functions": {
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
    },
    "additional_docs": {
        "bfs_edges": f"For additional details, see {algorithms_url}#bfs_edges",
        "descendants_at_distance": f"For additional details, see {algorithms_url}#descendants_at_distance",
        "bfs_layers": f"For additional details, see {algorithms_url}#bfs_layers",
        "pagerank": f"""- Neptune Analytics recommends using a `max_iter` value of `20` for PageRank calculations, \
which balances computational efficiency with result accuracy. This default setting is optimized for most graph \
workloads, though you can adjust it based on your specific convergence requirements. 
- Please note that the `personalization`, `nstart`, `weight`, and `dangling` parameters are not supported at the moment.
- For additional parameters, see {algorithms_url}#pagerank""",
        "degree_centrality": f"For additional details, see {algorithms_url}#degree_centrality",
        "in_degree_centrality": f"For additional details, see {algorithms_url}#in_degree_centrality",
        "out_degree_centrality": f"For additional details, see {algorithms_url}#out_degree_centrality",
        "asyn_lpa_communities": f"""- The `seed` parameter is not supported at the moment.
- Label propagation in Neptune Analytics maps all NetworkX variants to the same algorithm, using a fixed label update \
strategy. Variant-specific control over the update method (e.g., synchronous vs. asynchronous) is not configurable.  
- For additional parameters, see {algorithms_url}#asyn_lpa_communities""",
        "fast_label_propagation_communities": f"""- The `seed` parameter is not supported at the moment.
- Label propagation in Neptune Analytics maps all NetworkX variants to the same algorithm, using a fixed label update \
strategy. Variant-specific control over the update method (e.g., synchronous vs. asynchronous) is not configurable.
- For additional parameters, see {algorithms_url}#fast_label_propagation_communities""",
        "closeness_centrality": f"For additional details, see {algorithms_url}#closeness_centrality",
        "label_propagation_communities": f"For additional details, see {algorithms_url}#label_propagation_communities",
        "louvain_communities": f"""- Please note that the `resolution` and `seed` parameters are not supported at the \
moment.
- For additional parameters, see {algorithms_url}#louvain_communities""",
    },
    "additional_parameters": {},
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
