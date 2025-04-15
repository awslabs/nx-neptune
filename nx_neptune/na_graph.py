import logging
from typing import Any, Dict, List

from networkx import Graph

from .clients import (
    Edge,
    NeptuneAnalyticsClient,
    Node,
    bfs_query,
    clear_query,
    delete_edge,
    delete_node,
    insert_edge,
    insert_node,
    match_all_edges,
    match_all_nodes,
    pagerank_query,
    update_edge,
    update_node,
)


class NeptuneGraph:
    """
    The NeptuneGraph is the core component of this plugin,
    responsible for interacting with the AWS Neptune Analytics service.
    It facilitates various actions, including CRUD operations on graphs
    and the execution of supported algorithms.
    TODO: To add test on CRUD operation,
    when hard-coded openCypher syntax being replaced by Python lib.
    """

    NAME = "nx_neptune"

    def __init__(
        self,
        graph=None,
        logger=None,
        client=None,
    ):
        """
        Constructs a NeptuneGraph object for AWS service interaction,
        with optional custom logger and boto client.
        TODO: To have a create_client || connect_to_na_instance
        for networkX backend integration.
        TODO: Save a boto3 session instance once the client
        has connected
        """
        self.logger = logger or logging.getLogger(__name__)
        self.client = client or NeptuneAnalyticsClient()
        self.graph = graph or Graph()

    def graph_object(self) -> Graph:
        return self.graph

    def traversal_direction(self, reverse: bool) -> str:
        if not self.graph_object().is_directed():
            # 'reverse' parameter has no effect for non-directed graphs
            return '"both"'
        elif reverse is False:
            return '"outbound"'
        else:
            return '"inbound"'

    def create_na_instance(self):
        """
        TODO: Connect to Boto3 and create a Neptune Analytic instance,
        then return the graph ID.
        """
        return self.graph

    def add_node(self, node: Node):
        """
        Method to add additional nodes into the existing graph,
        which this client hold references to.
        """
        query_str, para_map = insert_node(node)
        return self.client.execute_generic_query(query_str, para_map)

    def update_nodes(
        self,
        match_labels: str,
        ref_name: str,
        where_filters: dict,
        properties_set: dict,
    ):
        """
        Perform an update on node's property for nodes with matching condition,
        which presented within the graph.
        """
        query_str, para_map = update_node(
            match_labels, ref_name, where_filters, properties_set
        )
        return self.client.execute_generic_query(query_str, para_map)

    def delete_nodes(self, node: Node):
        """
        To delete note from the graph with provided condition.

        Args:
            node (Node): The node to delete.

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str, para_map = delete_node(node)
        return self.client.execute_generic_query(query_str, para_map)

    def clear_graph(self):
        """
        To perform truncation to clear all nodes and edges on the graph.
        """
        query_str = clear_query()
        return self.client.execute_generic_query(query_str)

    def add_edge(self, edge: Edge):
        """
        Perform an insertion to add a relationship between two nodes.

        Args:
            edge: Edge (Edge object)

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str, para_map = insert_edge(edge)
        return self.client.execute_generic_query(query_str, para_map)

    def update_edges(
        self,
        ref_name_src: str,
        ref_name_edge: str,
        ref_name_des: str,
        edge: Edge,
        where_filters: dict,
        properties_set: dict,
    ):
        """
        Update existing edge's properties with provided condition and values.

        Args:
            ref_name_src: Reference name for the source node
            ref_name_edge: Reference name for the edge
            edge: Edge (Edge object)
            ref_name_des: Reference name for the destination node
            where_filters: Filters to apply in the WHERE clause
            properties_set: Properties to set

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str, para_map = update_edge(
            ref_name_src,
            ref_name_edge,
            edge,
            ref_name_des,
            where_filters,
            properties_set,
        )
        return self.client.execute_generic_query(query_str, para_map)

    def delete_edges(self, edge: Edge):
        """
        Delete one or more edges from NA graph,
        with provided conditions and values.

        Args:
            edge: Edge (Edge object) with source and destination nodes

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str, para_map = delete_edge(edge)
        return self.client.execute_generic_query(query_str, para_map)

    def get_all_nodes(self):
        """
        Helper method to return all nodes from the graph,
        in Python List object format.

        Returns:
            _type_: Nodes in JSON format.
        """
        query_str = match_all_nodes()
        return self.client.execute_generic_query(query_str)

    def get_all_edges(self):
        """
        Helper method to return all edges from the graph,
        in Python List object format.

        Returns:
            _type_: Edges in JSON format.
        """
        query_str = match_all_edges()
        return self.client.execute_generic_query(query_str)

    def execute_algo_bfs(
        self, source_node_list: str, where_filters: dict, parameters=None
    ):
        """
        Compose an OpenCypher `CALL` statement for BFS algorithm run,
        then execute over the remote NA cluster.
        TODO: add additional (optional) arguments for the BFS algorithm
        TODO: add example

        Args:
            source_node_list (str): The variable which will be used
            under MATCH and CALL clause.
            where_clause (str): The condition statement to be used
            under WHERE clause for filtering.

        Returns:
            _type_: The execution result of BFS algorithm.
            :param source_node_list:
            :param where_clause:
            :param parameters:
        """
        if parameters is None:
            parameters = {}
        query_str, para_map = bfs_query(source_node_list, where_filters, parameters)
        return self.client.execute_generic_query(query_str, para_map)

    def execute_algo_pagerank(self, parameters=None):
        """
        Execute PageRank algorithm on Neptune Analytics.

        Args:
            parameters (dict, optional): Parameters for the PageRank algorithm.
                Supported parameters include:
                - dampingFactor: The damping factor (default: 0.85)
                - maxIterations: Maximum number of iterations (default: 20)
                - tolerance: Error tolerance to check convergence (default: 1.0e-6)

        Returns:
            list: The execution result of PageRank algorithm, containing nodes and their PageRank scores.
        """
        if parameters is None:
            parameters = {}
        query_str, para_map = pagerank_query(parameters)
        return self.client.execute_generic_query(query_str, para_map)
