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
import json
import logging
from typing import Optional

import boto3
from botocore.client import BaseClient
from botocore.config import Config

from .neptune_constants import APP_ID_NX, SERVICE_NA


class NeptuneAnalyticsClient:
    """
    Neptune Analytics (neptune-graph) Client is used to fetch/execute queries to the Neptune Analytics
    backend data source using the AWS boto3 client.

    This class represents a Neptune Analytics graph database instance, providing access to its
    metadata and methods for executing queries against the graph database.

    Args:
            graph_id (str): The id of the Neptune Analytics graph.
            client (BaseClient): Custom boto3 client.
            logger (logging.Logger): Custom logger. Creates a default logger if None is provided.
            name (str, optional): The human-readable name of the graph instance.
            status (str, optional): The current status of the graph (e.g., 'AVAILABLE', 'CREATING').
            details (dict, optional): Complete response dictionary from Neptune Analytics API.
    """

    def __init__(
        self,
        graph_id: str,
        client: Optional[BaseClient] = None,
        name: Optional[str] = None,
        status: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        details: Optional[dict] = None,
    ):
        """
        Constructs a NeptuneAnalyticsClient object for AWS service interaction,
        with optional custom logger and boto client.
        """
        self.graph_id = graph_id
        self.client = client or boto3.client(
            service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
        )
        self.logger = logger or logging.getLogger(__name__)
        self.name = name or ""
        self.status = status or ""
        self.details = details or {}

    @classmethod
    def from_response(
        cls,
        response: dict,
        client: Optional[BaseClient],
        logger: Optional[logging.Logger] = None,
    ):
        """Create a NeptuneAnalyticsClient instance from a Neptune Analytics API response.

        Args:
            response (dict): Response dictionary from Neptune Analytics list_graphs or
                           get_graph API calls. Must contain 'id', 'name', and 'status' fields.
            client (BaseClient, optional): Boto3 Neptune Analytics client to use for this instance.
            logger (logging.Logger, optional): Custom logger.

        Returns:
            NeptuneAnalyticsClient: A new instance populated with data from the response.

        Example:
            >>> response = {"id": "g-123", "name": "my-graph", "status": "AVAILABLE"}
            >>> graph = NeptuneAnalyticsClient.from_response(response, boto3_client)
        """
        name = response["name"]
        graph_id = response["id"]
        status = response["status"]
        return cls(
            graph_id,
            client=client,
            name=name,
            status=status,
            logger=logger,
            details=response,
        )

    def __str__(self):
        """Return a string representation of the NeptuneAnalyticsClient.

        Returns:
            str: A formatted string containing the graph's id, name, and status.
        """
        return f"NeptuneAnalyticsClient(id='{self.graph_id}', name='{self.name}', status='{self.status}')"

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

    def execute_query(self, opencypher: str):
        """Execute an openCypher query against this Neptune Analytics graph instance.

        Args:
            opencypher (str): openCypher query string to execute against the graph.

        Returns:
            dict: Query results from Neptune Analytics containing the result set.

        Example:
            >>> graph = NeptuneAnalyticsClient(graph_id="g-123", client=boto3_client)
            >>> result = graph.execute_query("MATCH (n) RETURN n LIMIT 10")
        """
        return self.execute_generic_query(opencypher)

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
