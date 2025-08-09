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
import pytest
import json
from io import BytesIO
from unittest.mock import MagicMock, patch

from nx_neptune.clients.na_client import NeptuneAnalyticsClient


class TestNeptuneAnalyticsClient:
    """Test suite for the NeptuneAnalyticsClient class."""

    @pytest.fixture
    def mock_na_client(self):
        """Create a mock NeptuneAnalyticsClient"""
        boto3_client = MagicMock()
        graph_id = "test-graph-id"
        logger = MagicMock()
        mock = NeptuneAnalyticsClient(
            graph_id=graph_id,
            logger=logger,
            client=boto3_client,
        )
        return mock

    def test_init_with_explicit_params(self):
        """Test initialization with explicitly provided parameters."""
        boto3_client = MagicMock()
        graph_id = "test-graph-id"
        logger = MagicMock()

        # Exercise
        na_client = NeptuneAnalyticsClient(
            graph_id=graph_id,
            logger=logger,
            client=boto3_client,
        )

        # Verify
        assert na_client.graph_id == graph_id
        assert na_client.logger == logger
        assert na_client.client == boto3_client

    @patch("botocore.client.BaseClient")
    def test_init_with_default_boto_client(self, mock_boto3_client):
        """Test initialization with default boto3 client."""
        mock_logger = MagicMock()
        mock_graph_id = "test-graph-id"

        # Exercise
        client = NeptuneAnalyticsClient(
            logger=mock_logger, graph_id=mock_graph_id, client=mock_boto3_client
        )

        # Verify
        assert client.client == mock_boto3_client

    @patch("logging.getLogger")
    def test_init_with_default_logger(self, mock_get_logger):
        """Test initialization with default logger."""
        mock_boto3_client = MagicMock()
        mock_logger = MagicMock()
        mock_graph_id = "test-graph-id"
        mock_get_logger.return_value = mock_logger

        # Exercise
        client = NeptuneAnalyticsClient(
            graph_id=mock_graph_id, client=mock_boto3_client
        )

        # Verify
        mock_get_logger.assert_called_once_with("nx_neptune.clients.na_client")
        assert client.logger == mock_logger

    def test_create_na_instance(self, mock_na_client):
        # Exercise
        result = mock_na_client.create_na_instance()

        # Verify
        assert result == mock_na_client.graph_id

    def test_connect_to_na_instance(self, mock_na_client):
        # Exercise
        result = mock_na_client.connect_to_na_instance()

        # Verify
        assert result == mock_na_client.graph_id

    def test_execute_generic_query_without_params(self, mock_na_client):
        """Test execute_generic_query method without parameter map."""
        # Setup mock response
        mock_response = {
            "payload": BytesIO(
                json.dumps({"results": [{"data": "test_data"}]}).encode()
            )
        }
        mock_na_client.client.execute_query.return_value = mock_response

        # Execute the method
        query = "MATCH (n) RETURN n"
        result = mock_na_client.execute_generic_query(query)

        # Verify the result
        assert result == [{"data": "test_data"}]

        # Verify the client was called correctly
        mock_na_client.client.execute_query.assert_called_once_with(
            graphIdentifier=mock_na_client.graph_id,
            queryString=query,
            language="OPEN_CYPHER",
            parameters={},
        )

        # Verify logging
        mock_na_client.logger.debug.assert_called_once_with(
            f"Executing generic query [{query}] on graph [{mock_na_client.graph_id}]"
        )

    def test_execute_generic_query_with_params(self, mock_na_client):
        """Test execute_generic_query method with parameter map."""
        # Setup mock response
        mock_response = {
            "payload": BytesIO(
                json.dumps({"results": [{"data": "test_data"}]}).encode()
            )
        }
        mock_na_client.client.execute_query.return_value = mock_response

        # Execute the method
        query = "MATCH (n) WHERE n.name = $name RETURN n"
        params = {"name": "test"}
        result = mock_na_client.execute_generic_query(query, params)

        # Verify the result
        assert result == [{"data": "test_data"}]

        # Verify the client was called correctly
        mock_na_client.client.execute_query.assert_called_once_with(
            graphIdentifier=mock_na_client.graph_id,
            queryString=query,
            language="OPEN_CYPHER",
            parameters=params,
        )
