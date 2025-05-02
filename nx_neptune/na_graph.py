import logging
from typing import Any, Dict, List, Optional

from networkx import DiGraph, Graph

from .clients import (
    PARAM_TRAVERSAL_DIRECTION_BOTH,
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    Edge,
    IamClient,
    NeptuneAnalyticsClient,
    Node,
    clear_query,
    delete_edge,
    delete_node,
    insert_edge,
    insert_node,
    match_all_edges,
    match_all_nodes,
    update_edge,
    update_node,
)


class NeptuneGraph:
    """
    The NeptuneGraph is the core component of this plugin,
    responsible for interacting with the AWS Neptune Analytics service.
    It facilitates various actions, including CRUD operations on graphs
    and the execution of supported algorithms.
    """

    NAME = "nx_neptune"

    def __init__(
        self,
        graph=None,
        logger=None,
        client=None,
        iam_client=None,
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
        self.client = client or NeptuneAnalyticsClient(logger=self.logger)
        if graph is None:
            self.graph = Graph()
        else:
            self.graph = graph
        self.current_jobs = set()
        self.iam_client = iam_client or IamClient(logger=self.logger)

    def graph_object(self) -> Graph | DiGraph:
        return self.graph

    def traversal_direction(self, reverse: bool) -> str:
        if not self.graph_object().is_directed():
            # 'reverse' parameter has no effect for non-directed graphs
            return PARAM_TRAVERSAL_DIRECTION_BOTH
        elif reverse is False:
            return PARAM_TRAVERSAL_DIRECTION_OUTBOUND
        else:
            return PARAM_TRAVERSAL_DIRECTION_INBOUND

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

    def execute_call(
        self, query_string: str, parameter_map: Optional[dict] = None
    ) -> Any:
        """
        Helper method to call a Neptune Function.

        Returns:
            dict: Result from Boto client.
        """
        return self.client.execute_generic_query(query_string, parameter_map)
