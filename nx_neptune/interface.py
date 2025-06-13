from __future__ import annotations

import asyncio
import logging
import os
from operator import attrgetter

import networkx as nx

import nx_neptune.algorithms
from nx_plugin import NeptuneConfig
from nx_plugin.config import (
    NETWORKX_GRAPH_ID,
    NETWORKX_S3_IAM_ROLE_ARN,
)

from .clients import Edge, Node
from .instance_management import (
    create_na_instance,
    delete_na_instance,
    export_csv_to_s3,
    import_csv_from_s3,
)
from .na_graph import NeptuneGraph, get_config, set_config_graph_id

logger = logging.getLogger(__name__)

# Avoid infinite recursion when testing
_IS_TESTING = os.environ.get("NETWORKX_TEST_BACKEND") in {"test"}

__all__ = ["BackendInterface", "NETWORKX_GRAPH_ID", "NETWORKX_S3_IAM_ROLE_ARN"]

ALGORITHMS = [
    "bfs_edges",
    "bfs_layers",
    "descendants_at_distance",
    "pagerank",
    "degree_centrality",
    "in_degree_centrality",
    "out_degree_centrality",
]


def assign_algorithms(cls):
    """Class decorator to assign algorithms to the class attributes."""
    # TODO: network call to fetch the list of algorithms from Neptune?
    for attr in ALGORITHMS:
        # get the function name by parsing the module hierarchy
        func_name = attr.rsplit(".", 1)[-1]
        setattr(cls, func_name, attrgetter(attr)(nx_neptune.algorithms))

    return cls


@assign_algorithms
class BackendInterface:

    @staticmethod
    def convert_from_nx(graph: nx.Graph, *args, **kwargs):
        logger.debug("nx_neptune.convert_from_nx()")
        return graph

    @staticmethod
    def convert_to_nx(g, *args, **kwargs):
        logger.debug("nx_neptune.convert_to_nx()")
        if isinstance(g, NeptuneGraph):
            return g.graph_object()
        return g
