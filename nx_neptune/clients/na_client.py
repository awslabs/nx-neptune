import json
import logging
import os
from typing import Optional

import boto3


class NeptuneAnalyticsClient:
    """
    Neptune Analytics (neptune-graph) Client is used to fetch/execute queries to the Neptune Analytics
    backend data source using the AWS boto3 client.
    """

    SERVICE_NA = "neptune-graph"
    GRAPH_ID = "GRAPH_ID"

    def __init__(
        self,
        graph_id=None,
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
        self.graph_id = graph_id or os.getenv(self.GRAPH_ID)
        self.logger = logger or logging.getLogger(__name__)
        self.client = client or boto3.client(self.SERVICE_NA)

    def create_na_instance(self):
        """
        TODO: Connect to Boto3 and create a Neptune Analytic instance,
        then return the graph ID.
        """
        return self.graph_id

    def connect_to_na_instance(self):
        """
        TODO: Connect to Boto3 then return the graph ID.
        """
        return self.graph_id

    def execute_generic_query(
        self, query_string: str, parameter_map: Optional[dict] = None
    ):
        """
        Wrapper method to execute incoming openCypher query,
        and return the result from Boto client.

        Args:
            query_string (str): OpenCypher query in string format.
            parameter_map (dict, optional): Parameter map for parameterized queries. Defaults to None.

        Returns:
            dict: Result from Boto client.
        """
        query_params = {
            "graphIdentifier": self.graph_id,
            "queryString": query_string,
            "language": "OPEN_CYPHER",
        }

        # Add parameters if provided
        if parameter_map:
            query_params["parameters"] = parameter_map

        self.logger.debug(
            f"Executing generic query [{query_string}] on graph [{self.graph_id}]"
        )
        response = self.client.execute_query(**query_params)

        return json.loads(response["payload"].read())["results"]
