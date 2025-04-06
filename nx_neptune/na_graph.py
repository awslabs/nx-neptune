import logging

from .clients import (
    NeptuneAnalyticsClient,
    insert_query,
    update_query,
    delete_query,
    clear_query,
    match_all_nodes,
    match_all_edges,
    bfs_query,
)
from networkx import Graph


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

    def create_na_instance(self):
        """
        TODO: Connect to Boto3 and create a Neptune Analytic instance,
        then return the graph ID.
        """
        return self.graph

    def add_node(self, node_content: str):
        """
        Method to add additional nodes into the existing graph,
        which this client hold references to.
        """
        query_str = insert_query(node_content)
        return self.client.execute_generic_query(query_str)

    def update_nodes(self, match_clause: str, where_clause: str, set_clause: str):
        """
        Perform an update on node's property for nodes with matching condition,
        which presented within the graph.
        """
        query_str = update_query(match_clause, where_clause, set_clause)
        return self.client.execute_generic_query(query_str)

    def delete_nodes(self, match_clause: str, delete_clause: str):
        """
        To delete note from the graph with provided condition.

        Args:
            match_clause (str): The OpenCypher MATCH clause.
            delete_clause (str): The OpenCypher DELETE clause.

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str = delete_query(match_clause, delete_clause)
        return self.client.execute_generic_query(query_str)

    def clear_graph(self):
        """
        To perform truncation to clear all nodes and edges on the graph.
        """
        query_str = clear_query()
        return self.client.execute_generic_query(query_str)

    def add_edge(self, create_clause: str, match_clause: str):
        """
        Perform an insertion to add a relationship between two nodes.

        Args:
            create_clause (str): The OpenCypher CREATE clause.
            match_clause (str): The OpenCypher MATCH clause.

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str = insert_query(create_clause, match_clause)
        return self.client.execute_generic_query(query_str)

    def update_edges(self, match_clause: str, where_clause: str, set_clause: str):
        """
        Update existing edge's properties with provided condition and values.

        Args:
            match_clause (str): The OpenCypher MATCH clause.
            where_clause (str): The OpenCypher WHERE clause.
            set_clause (str): The OpenCypher VALUE clause.

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str = update_query(match_clause, where_clause, set_clause)
        return self.client.execute_generic_query(query_str)

    def delete_edges(self, match_clause: str, delete_clause: str):
        """
        Delete one or more edges from NA graph,
        with provided conditions and values.

        Args:
            match_clause (str): The OpenCypher MATCH clause.
            delete_clause (str): The OpenCypher VALUE clause.

        Returns:
            _type_: N/A
        """
        query_str = delete_query(match_clause, delete_clause)
        return self.client.execute_generic_query(query_str)

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
        self, source_node_list: str, where_clause: str, parameters=None
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
        query_str = bfs_query(source_node_list, where_clause, parameters)
        return self.client.execute_generic_query(query_str)
