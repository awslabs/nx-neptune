from __future__ import annotations

import logging
import os
from operator import attrgetter

import networkx as nx

import nx_neptune.algorithms

from .clients import Edge, Node
from .na_graph import NeptuneGraph

logger = logging.getLogger(__name__)

# Avoid infinite recursion when testing
_IS_TESTING = os.environ.get("NETWORKX_TEST_BACKEND") in {"test"}

__all__ = ["BackendInterface"]

ALGORITHMS = [
    "bfs_edges",
    "pagerank",
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
    def convert_from_nx(graph: nx.Graph, *args, **kwargs) -> NeptuneGraph:

        logger.debug("nx_neptune.convert_from_nx()")
        logger.debug("graph=" + str(graph))
        logger.debug(f"is_directed={graph.is_directed()}")
        logger.debug("kwargs=" + str(args))

        na_graph = NeptuneGraph(graph=graph)

        """
        Push all Nodes from NetworkX into Neptune Analytics
        """
        for node in graph.nodes().data():
            na_node = Node.convert_from_nx(node)
            logger.debug(f"add_node={na_node}")
            na_graph.add_node(na_node)

        """
        Push all Edges from NetworkX into Neptune Analytics
        """
        for edge in graph.edges().data():
            na_edge = Edge.convert_from_nx(edge)
            logger.debug(f"add_edge={na_edge}")
            na_graph.add_edge(na_edge)

            # Push the reverse direction edge if the graph is un-directed
            if not graph.is_directed():
                na_reverse_edge = Edge.convert_from_nx(edge).to_reverse_edge()
                logger.debug(f"add_edge={na_reverse_edge}")
                na_graph.add_edge(na_reverse_edge)

        return na_graph

    @staticmethod
    def convert_to_nx(g, *args, **kwargs) -> nx.Graph | nx.DiGraph:
        logger.debug("nx_neptune_analytics.convert_to_nx()")
        if isinstance(g, NeptuneGraph):
            return g.graph_object()
        return g
