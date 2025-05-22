import json
import logging
from typing import Optional

from botocore.client import BaseClient


class NeptuneAnalyticsClient:
    """
    Neptune Analytics (neptune-graph) Client is used to fetch/execute queries to the Neptune Analytics
    backend data source using the AWS boto3 client.

    Args:
            graph_id (str): The id of the Neptune Analytics graph.
            client (BaseClient): Custom boto3 client.
            logger (logging.Logger): Custom logger. Creates a default logger if None is provided.
    """

    def __init__(
        self,
        graph_id: str,
        client: BaseClient,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Constructs a NeptuneGraph object for AWS service interaction,
        with optional custom logger and boto client.
        """
        self.graph_id = graph_id
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

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
        Wrapper method to execute an incoming OpenCypher query
        and return the result from the Boto client.

        Args:
            query_string (str): OpenCypher query in string format.
            parameter_map (dict, optional): Parameter map for parameterized queries. Defaults to None.

        Returns:
            dict: Result from the Boto client.
        """
        query_params = {
            "graphIdentifier": self.graph_id,
            "queryString": query_string,
            "language": "OPEN_CYPHER",
            "parameters": {},
        }

        # Add parameters if provided
        if parameter_map:
            query_params["parameters"] = parameter_map

        self.logger.debug(
            f"Executing generic query [{query_string}] on graph [{self.graph_id}]"
        )
        response = self.client.execute_query(**query_params)  # type: ignore[attr-defined]

        return json.loads(response["payload"].read())["results"]
