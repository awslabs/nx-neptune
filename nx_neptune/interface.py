from __future__ import annotations

import logging
import os
from operator import attrgetter

import networkx as nx

from nx_neptune import NeptuneGraph, algorithms
from nx_neptune.clients import Edge, Node

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
        setattr(cls, func_name, attrgetter(attr)(algorithms))

    return cls


@assign_algorithms
class BackendInterface:

    @staticmethod
    def convert_from_nx(graph: nx.Graph, *args, **kwargs) -> NeptuneGraph:
        logger.debug("nx_neptune.convert_from_nx()")
        logger.debug("graph=" + str(graph))
        logger.debug("kwargs=" + str(args))

        # TODO: add nodes and edges from G to the instance
        na_graph = NeptuneGraph(graph=graph)
        na_graph.clear_graph()

        """
        Push all Nodes from NetworkX into Neptune Analytics
        """
        for node in graph.nodes:
            # TODO: Encapsulate nx.node -> Node object conversion logic inside nx_neptune.clients.Node.convert_from_nx( )
            na_node = Node(labels=["Node"], properties={"name": node})
            logger.debug(f"add_node={na_node}")
            na_graph.add_node(na_node)

        for edge in graph.edges:
            # TODO: Encapsulate the conversion logic into Edge.convert_from_nx
            node_zero = Node(labels=["Node"], properties={"name": edge[0]})
            node_one = Node(labels=["Node"], properties={"name": edge[1]})
            friend_edge = Edge(
                label="FRIEND_WITH",
                properties={},
                node_src=node_zero,
                node_dest=node_one,
            )
            logger.debug(f"add_edge={friend_edge}")
            na_graph.add_edge(friend_edge)

        return na_graph

    @staticmethod
    def convert_to_nx(g, *args, **kwargs) -> nx.Graph:
        logger.debug("nx_neptune_analytics.convert_to_nx()")
        if isinstance(g, NeptuneGraph):
            return g.graph_object
        return g
