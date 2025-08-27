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
        "pagerank",
        "degree_centrality",
        "in_degree_centrality",
        "out_degree_centrality",
        "descendants_at_distance",
        "bfs_layers",
        "label_propagation_communities",
        "fast_label_propagation_communities",
        "closeness_centrality",
        "louvain_communities",
        "asyn_lpa_communities",
        # END: functions
    },
    "additional_docs": {
        # BEGIN: additional_docs
        "bfs_edges": "limited version of nx.shortest_path",
        "pagerank": """Neptune Analytics recommends using a max_iter value of 20 for PageRank
            calculations, which balances computational efficiency with result accuracy. This
            default setting is optimized for most graph workloads, though you can adjust it
            based on your specific convergence requirements. Please note that the
            personalization, nstart, weight, and dangling parameters are not supported at
            the moment.""",
        "asyn_lpa_communities": """
        Please note that the seed parameter is not supported at the moment,
        also label propagation in Neptune Analytics maps all NetworkX variants to the same algorithm,
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
        "bfs": {
            "edgeLabels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertexLabel : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
            including vertices in the input list.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
        },
        "pagerank": {
            "edgeLabels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertexLabel : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
            including vertices in the input list.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "traversalDirection : str, optional": "The direction of edge to follow.; "
            """Must be one of: "outbound" or "inbound".""",
            "edgeWeightProperty : str, optional": "The weight property to consider for weighted pageRank computation.; "
            """""",
            "edgeWeightType : str, optional": "required if edgeWeightProperty is present; "
            """The type of values associated with the edgeWeightProperty argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the edgeWeightProperty is not given,
            the algorithm runs unweighted no matter if the edgeWeightType is given or not.
            Note that if multiple properties exist on the edge with the name specified by edgeWeightProperty,
            one of those property values will be sampled at random.""",
            "sourceNodes : list[str], optional": "required if running personalized PageReank; "
            """A personalization vertex list ["101", ...]
            Can include 1 to 8192 vertices.
            If a vertexLabel is provided, nodes that do not have the given vertexLabel are ignored.
            """,
            "sourceWeights : list[numeric], optional": "A personalization weight list.; "
            """The weight distribution among the personalized vertices.
            If not provided, the default behavior is uniform distribution among the vertices given in sourceNodes.
            There must be at least one non-zero weight in the list.
            The length of the sourceWeights list must match the sourceNodes list.
            The mapping of personalization vertex and weight lists are one to one.
            The first value in the weight list corresponds to the weight of first vertex in the vertex list,
            second value is for the second vertex, etc.
            The weights can be one of int, long, float, or double types.
            """,
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutated version will be used.
            In this mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in this mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "degree_centrality": {
            "edgeLabels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertexLabel : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
            including vertices in the input list.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutated version will be used.
            In this mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in this mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "in_degree_centrality": {
            "edgeLabels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertexLabel : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
            including vertices in the input list.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutated version will be used.
            In this mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in this mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "out_degree_centrality": {
            "edgeLabels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertexLabel : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
            including vertices in the input list.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutated version will be used.
            In this mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in this mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "descendants_at_distance": {
            "traversalDirection : str, optional": "The direction of edge to follow.; "
            """Must be one of: "outbound" or "inbound".""",
            "edgeLabels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertexLabel : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
            including vertices in the input list.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
        },
        "bfs_layers": {
            "traversalDirection : str, optional": "The direction of edge to follow.; "
            """Must be one of: "outbound" or "inbound".""",
            "edgeLabels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertexLabel : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
            including vertices in the input list.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
        },
        "label_propagation_communities": {
            "edge_labels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertex_label : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
           including vertices in the input list.""",
            "vertex_weight_property : str, optional": "The vertex's weight property for algorithm computation.; "
            """""",
            "vertex_weight_type : str, optional": "required if vertex_weight_property is present; "
            """The type of values associated with the vertex_weight_property argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the vertex_weight_property is not given,
            the algorithm runs unweighted no matter if the vertex_weight_type is given or not.
            Note that if multiple properties exist on the edge with the name specified by vertex_weight_property,
            one of those property values will be sampled at random.""",
            "edge_weight_property : str, optional": "The weight property to consider for weighted computation.; "
            """""",
            "edge_weight_type : str, optional": "required if edgeWeightProperty is present; "
            """The type of values associated with the edgeWeightProperty argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the edgeWeightProperty is not given,
            the algorithm runs unweighted no matter if the edgeWeightType is given or not.
            Note that if multiple properties exist on the edge with the name specified by edgeWeightProperty,
            one of those property values will be sampled at random.""",
            "max_iterations : int, optional": "default: 10.; "
            """The maximum number of iterations to run.""",
            "traversal_direction : str, optional": "The direction of edge to follow.; "
            """Must be one of: "outbound" or "inbound".""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutated version will be used.
            In this mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in this mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "asyn_lpa_communities": {
            "edge_labels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertex_label : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
           including vertices in the input list.""",
            "vertex_weight_property : str, optional": "The vertex's weight property for algorithm computation.; "
            """""",
            "vertex_weight_type : str, optional": "required if vertex_weight_property is present; "
            """The type of values associated with the vertex_weight_property argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the vertex_weight_property is not given,
            the algorithm runs unweighted no matter if the vertex_weight_type is given or not.
            Note that if multiple properties exist on the edge with the name specified by vertex_weight_property,
            one of those property values will be sampled at random.""",
            "edge_weight_property : str, optional": "The weight property to consider for weighted computation.; "
            """""",
            "edge_weight_type : str, optional": "required if edgeWeightProperty is present; "
            """The type of values associated with the edgeWeightProperty argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the edgeWeightProperty is not given,
            the algorithm runs unweighted no matter if the edgeWeightType is given or not.
            Note that if multiple properties exist on the edge with the name specified by edgeWeightProperty,
            one of those property values will be sampled at random.""",
            "max_iterations : int, optional": "default: 10.; "
            """The maximum number of iterations to run.""",
            "traversal_direction : str, optional": "The direction of edge to follow.; "
            """Must be one of: "outbound" or "inbound".""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutated version will be used.
            In this mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in this mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "fast_label_propagation_communities": {
            "edge_labels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertex_label : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
           including vertices in the input list.""",
            "vertex_weight_property : str, optional": "The vertex's weight property for algorithm computation.; "
            """""",
            "vertex_weight_type : str, optional": "required if vertex_weight_property is present; "
            """The type of values associated with the vertex_weight_property argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the vertex_weight_property is not given,
            the algorithm runs unweighted no matter if the vertex_weight_type is given or not.
            Note that if multiple properties exist on the edge with the name specified by vertex_weight_property,
            one of those property values will be sampled at random.""",
            "edge_weight_property : str, optional": "The weight property to consider for weighted computation.; "
            """""",
            "edge_weight_type : str, optional": "required if edgeWeightProperty is present; "
            """The type of values associated with the edgeWeightProperty argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the edgeWeightProperty is not given,
            the algorithm runs unweighted no matter if the edgeWeightType is given or not.
            Note that if multiple properties exist on the edge with the name specified by edgeWeightProperty,
            one of those property values will be sampled at random.""",
            "max_iterations : int, optional": "default: 10.; "
            """The maximum number of iterations to run.""",
            "traversal_direction : str, optional": "The direction of edge to follow.; "
            """Must be one of: "outbound" or "inbound".""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutated version will be used.
            In this mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in this mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "closeness_centrality": {
            "numSources : int, optional, default to maxInt": "The number of sources to compute approximate Closeness result.;"
            """To compute exact closeness centrality, set numSources to a number larger than number of nodes,
            such as maxInt.""",
            "edge_labels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "vertex_label : str, optional": "A vertex label for vertex filtering.; "
            """If a vertex label is provided, vertices matching the label are the only vertices that are included,
           including vertices in the input list.""",
            "traversal_direction : str, optional": "The direction of edge to follow.; "
            """Must be one of: "outbound" or "inbound".""",
            "normalize : Boolean, optional": "Normalization feature switch always overrides wf_improved when enabled."
            """You can use this field to turn off normalization, which is on by default. Without normalization,
            only centrality scores of nodes within the same component can be meaningfully compared.
            Normalized scores can be compared across different connected components.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
            If set to 1, uses a single thread.
            This can be useful when requiring the invocation of many algorithms concurrently.""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutation version will be used.
            In mutation mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in mutation mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
        "louvain_communities": {
            "edge_labels : list[str], optional": "A list of edge label strings; "
            """To filter on one more edge labels, provide a list of the ones to filter on.
            If no edgeLabels field is provided then all edge labels are processed during traversal.""",
            "edge_weight_property : str, optional": "The weight property to consider for weighted computation.; "
            """""",
            "edge_weight_type : str, optional": "required if edgeWeightProperty is present; "
            """The type of values associated with the edgeWeightProperty argument, specified as a string.
            valid values: "int", "long", "float", "double".
            If the edgeWeightProperty is not given,
            the algorithm runs unweighted no matter if the edgeWeightType is given or not.
            Note that if multiple properties exist on the edge with the name specified by edgeWeightProperty,
            one of those property values will be sampled at random.""",
            "max_iterations : int, optional": "default: 10.; "
            """The maximum number of iterations to run.""",
            "concurrency : int, optional": "Controls the number of concurrent threads used to run the algorithm.; "
            """If set to 0, uses all available threads to complete execution of the individual algorithm invocation.
           If set to 1, uses a single thread.
           This can be useful when requiring the invocation of many algorithms concurrently.""",
            "level_tolerance : float, optional": "Minimum modularity change to continue to next level.; "
            """""",
            "write_property : string, optional": "Determines whether to execute the standard or mutated version "
            + "of the algorithm.; "
            """If `write_property` is specified, the mutation version will be used.
            In mutation mode, the algorithm writes the result directly into the remote graph under the specified property name.

            **Important:** No execution result will be returned to the user in mutation mode.
            To preserve the mutated graph state, you must either avoid setting `write_property`,
            or ensure the option `nx.config.backends.neptune.export_s3_bucket` is properly configured for automatic export.""",
        },
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
