from __future__ import annotations
import os
from operator import attrgetter
import logging
import networkx as nx
from nx_neptune import algorithms, NeptuneGraph

logger = logging.getLogger(__name__)

# Avoid infinite recursion when testing
_IS_TESTING = os.environ.get("NETWORKX_TEST_BACKEND") in {"test"}

__all__ = ["BackendInterface"]

ALGORITHMS = [
    "bfs_edges",
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
            node_string = f"a:Node {{name: '{node}'}}"
            logger.debug(f"add_node={node_string}")
            na_graph.add_node(node_string)

        for edge in graph.edges:
            create_clause = "(a)-[:FRIEND_WITH]->(b)"
            match_clause = (
                f"(a:Node {{name: '{edge[0]}'}}), (b:Node {{name: '{edge[1]}'}})"
            )
            logger.debug(f"add_edge={create_clause}, {match_clause}")
            na_graph.add_edge(create_clause, match_clause)

        return na_graph

    @staticmethod
    def convert_to_nx(g, *args, **kwargs) -> nx.Graph:
        logger.debug("nx_neptune_analytics.convert_to_nx()")
        if isinstance(g, NeptuneGraph):
            return g.graph_object
        return g
