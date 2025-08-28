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
import logging
from asyncio import Task
from typing import Any, List, Optional

import boto3
import networkx
from botocore.config import Config
from networkx import DiGraph, Graph

from nx_plugin import NeptuneConfig
from nx_plugin.config import NETWORKX_GRAPH_ID, NETWORKX_S3_IAM_ROLE_ARN

from .clients import (
    PARAM_TRAVERSAL_DIRECTION_BOTH,
    PARAM_TRAVERSAL_DIRECTION_INBOUND,
    PARAM_TRAVERSAL_DIRECTION_OUTBOUND,
    SERVICE_IAM,
    SERVICE_NA,
    SERVICE_STS,
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

__all__ = [
    "NeptuneGraph",
    "NETWORKX_GRAPH_ID",
    "NETWORKX_S3_IAM_ROLE_ARN",
    "get_config",
    "set_config_graph_id",
]

from .clients.neptune_constants import APP_ID_NX
from .clients.opencypher_builder import insert_edges, insert_nodes


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
        na_client: NeptuneAnalyticsClient,
        iam_client: IamClient,
        graph: Graph,
        logger: logging.Logger | None = None,
    ):
        """
        Constructs a NeptuneGraph object for AWS service interaction,
        with optional custom logger and boto client.
        """
        self.logger = logger or logging.getLogger(__name__)
        self.na_client = na_client
        self.iam_client = iam_client
        self.graph = graph
        self.current_jobs: set[Task] = set()

    @classmethod
    def from_config(cls, config: NeptuneConfig | None = None, graph=None, logger=None):
        if config is None:
            config = get_config()

        assert config.graph_id is not None

        if graph is None:
            graph = Graph()
        if logger is None:
            logger = logging.getLogger(__name__)
        s3_iam_role = config.s3_iam_role
        if s3_iam_role is None:
            s3_iam_role = boto3.client(SERVICE_STS).get_caller_identity()["Arn"]

        na_client = NeptuneAnalyticsClient(
            config.graph_id,
            boto3.client(
                service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
            ),
            logger,
        )
        iam_client = IamClient(s3_iam_role, boto3.client(SERVICE_IAM), logger)
        return cls(
            graph=graph,
            logger=logger,
            na_client=na_client,
            iam_client=iam_client,
        )

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
        return self.na_client.execute_generic_query(query_str, para_map)

    def add_nodes(self, nodes: List[Node]):
        """
        Method to add nodes into the existing graph,
        which this client hold references to.
        """
        query_strs, para_maps = insert_nodes(nodes)
        for query, params in zip(query_strs, para_maps):
            self.na_client.execute_generic_query(query, params)

    def update_node(
        self,
        match_labels: str,
        ref_name: str,
        node: Node,
        properties_set: dict,
    ):
        """
        Perform an update on node's properties.
        """
        query_str, para_map = update_node(
            match_labels, ref_name, [node.id], properties_set
        )
        return self.na_client.execute_generic_query(query_str, para_map)

    def update_nodes(
        self,
        match_labels: str,
        ref_name: str,
        nodes: List[Node],
        properties_set: dict,
    ):
        """
        Perform an update on node's property for nodes with matching condition,
        which presented within the graph.
        """
        node_ids = [n.id for n in nodes]
        query_str, para_map = update_node(
            match_labels, ref_name, node_ids, properties_set
        )
        return self.na_client.execute_generic_query(query_str, para_map)

    def delete_nodes(self, node: Node):
        """
        To delete note from the graph with provided condition.

        Args:
            node (Node): The node to delete.

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str, para_map = delete_node(node)
        return self.na_client.execute_generic_query(query_str, para_map)

    def clear_graph(self):
        """
        To perform truncation to clear all nodes and edges on the graph.

        """
        query_str = clear_query()
        return self.na_client.execute_generic_query(query_str)

    def add_edge(self, edge: Edge):
        """
        Perform an insertion to add a relationship between two nodes.

        Args:
            edge: Edge (Edge object)

        Returns:
            _type_: Result from boto client in string format.
        """
        query_str, para_map = insert_edge(edge)
        return self.na_client.execute_generic_query(query_str, para_map)

    def add_edges(self, edges: List[Edge]):
        """
        Perform an insertion to add a list of  relationships into the graph.

        Args:
            edges: List of Edges (Edge object)

        """
        query_strs, para_maps = insert_edges(edges)

        for query, params in zip(query_strs, para_maps):
            self.na_client.execute_generic_query(query, params)

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
        return self.na_client.execute_generic_query(query_str, para_map)

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
        return self.na_client.execute_generic_query(query_str, para_map)

    def get_all_nodes(self):
        """
        Helper method to return all nodes from the graph,
        in Python List object format.

        Returns:
            _type_: Nodes in JSON format.
        """
        query_str = match_all_nodes()
        all_nodes = self.na_client.execute_generic_query(query_str)
        return [node["n"] for node in all_nodes]

    def get_all_edges(self):
        """
        Helper method to return all edges from the graph,
        in Python List object format.

        Returns:
            _type_: Edges in JSON format.
        """
        query_str = match_all_edges()
        all_edges = self.na_client.execute_generic_query(query_str)
        return [edge["r"] for edge in all_edges]

    def execute_call(
        self, query_string: str, parameter_map: Optional[dict] = None
    ) -> Any:
        """
        Helper method to call a Neptune Function.

        Returns:
            dict: Result from the Boto client.
        """
        return self.na_client.execute_generic_query(query_string, parameter_map)


def get_config() -> NeptuneConfig:
    """
    Helper method to retrieve the Neptune configuration with global variable overrides.

    Returns:
        dict: The Neptune configuration.
    """
    config = networkx.config.backends.neptune

    if NETWORKX_GRAPH_ID is not None:
        config.graph_id = NETWORKX_GRAPH_ID

    if NETWORKX_S3_IAM_ROLE_ARN is not None:
        config.role_arn = NETWORKX_S3_IAM_ROLE_ARN

    return config


def set_config_graph_id(graph_id: str | None) -> NeptuneConfig:
    """
    Helper method to set the graph_id in the Neptune configuration.

    Returns:
        dict: The updated Neptune configuration.
    """
    networkx.config.backends.neptune["graph_id"] = graph_id

    # if graph_id is cleared, then create_new_instance is True, else False
    networkx.config.backends.neptune["create_new_instance"] = graph_id is None

    return get_config()
