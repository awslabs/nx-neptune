from .config import NeptuneConfig, _config

__all__ = ["NeptuneConfig"]

# Entries between BEGIN and END are automatically generated
_info = {
    "backend_name": "neptune",
    "project": "nx-neptune",
    "package": "nx_neptune",
    "url": "https://github.com/",
    "short_summary": "Neptune computation backend for NetworkX.",
    "description": "Scale graph algorithms on AWS Neptune Analytics platform.",
    "functions": {
        # BEGIN: functions
        "bfs",
        "pagerank",
        # END: functions
    },
    "additional_docs": {
        # BEGIN: additional_docs
        "bfs": "limited version of nx.shortest_path",
        "pagerank": """Neptune Analytics recommends using a max_iter value of 20 for PageRank
            calculations, which balances computational efficiency with result accuracy. This
            default setting is optimized for most graph workloads, though you can adjust it
            based on your specific convergence requirements. Please note that the
            personalization, nstart, weight, and dangling parameters are not supported at
            the moment.""",
        # END: additional_docs
    },
    "additional_parameters": {
        # BEGIN: additional_parameters
        "bfs": {
            "nodeList : test additional parameters ",
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
