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
        name = "test-name"
        status = "test-status"
        details = {"test": "test-details"}

        # Exercise
        na_client = NeptuneAnalyticsClient(
            graph_id=graph_id,
            logger=logger,
            client=boto3_client,
            name=name,
            status=status,
            details=details,
        )

        # Verify
        assert na_client.graph_id == graph_id
        assert na_client.logger == logger
        assert na_client.client == boto3_client
        assert na_client.name == name
        assert na_client.status == status
        assert na_client.details == details

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

    def test_execute_generic_query_with_timeout(self):
        """Test that timeout_seconds is converted to milliseconds in the query."""
        boto3_client = MagicMock()
        mock_response = {
            "payload": BytesIO(json.dumps({"results": [{"data": "test"}]}).encode())
        }
        boto3_client.execute_query.return_value = mock_response

        na_client = NeptuneAnalyticsClient(
            graph_id="test-graph-id",
            client=boto3_client,
            timeout_seconds=30,
        )

        query = "MATCH (n) RETURN n"
        na_client.execute_generic_query(query)

        boto3_client.execute_query.assert_called_once_with(
            graphIdentifier="test-graph-id",
            queryString=query,
            language="OPEN_CYPHER",
            parameters={},
            queryTimeoutMilliseconds=30000,
        )

    def test_execute_generic_query_without_timeout(self, mock_na_client):
        """Test that queryTimeoutMilliseconds is not sent when timeout is None."""
        mock_response = {
            "payload": BytesIO(json.dumps({"results": [{"data": "test"}]}).encode())
        }
        mock_na_client.client.execute_query.return_value = mock_response

        query = "MATCH (n) RETURN n"
        mock_na_client.execute_generic_query(query)

        call_kwargs = mock_na_client.client.execute_query.call_args[1]
        assert "queryTimeoutMilliseconds" not in call_kwargs

    @patch("nx_neptune.clients.na_client.boto3")
    def test_init_with_timeout_sets_read_timeout(self, mock_boto3):
        """Test that timeout_seconds sets read_timeout on the boto3 Config."""
        NeptuneAnalyticsClient(graph_id="g-123", timeout_seconds=120)

        mock_boto3.client.assert_called_once()
        config = mock_boto3.client.call_args[1]["config"]
        assert config.read_timeout == 120

    @patch("nx_neptune.clients.na_client.boto3")
    def test_init_without_timeout_uses_default(self, mock_boto3):
        """Test that no timeout_seconds leaves read_timeout at boto3 default (60s)."""
        NeptuneAnalyticsClient(graph_id="g-123")

        mock_boto3.client.assert_called_once()
        config = mock_boto3.client.call_args[1]["config"]
        assert config.read_timeout == 60

    @patch("nx_neptune.clients.na_client.boto3")
    def test_init_with_client_ignores_timeout_seconds(self, mock_boto3):
        """Test that providing a client uses it directly; timeout_seconds does not create a new client."""
        mock_client = MagicMock()
        na = NeptuneAnalyticsClient(graph_id="g-123", client=mock_client, timeout_seconds=120)
        assert na.client == mock_client
        mock_boto3.client.assert_not_called()
