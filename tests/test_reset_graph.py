import pytest
from unittest.mock import MagicMock

import nx_neptune.instance_management as InstanceUtil
from botocore.exceptions import EndpointConnectionError


def test_reset_graph_success():
    """Test that reset_graph returns True when the operation is successful"""
    # Create a mock client
    mock_client = MagicMock()

    # Mock the reset_graph response
    mock_client.reset_graph.return_value = {
        "ResponseMetadata": {
            "RequestId": "cf920a09-bfc4-4b02-8f98-5f836a46093f",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "date": "Thu, 24 Apr 2025 23:33:57 GMT",
                "content-type": "application/json",
                "content-length": "391",
                "connection": "keep-alive",
                "x-amzn-requestid": "cf920a09-bfc4-4b02-8f98-5f836a46093f",
                "x-amz-apigw-id": "JjSP-FwHvHcEXPQ=",
                "x-amzn-trace-id": "Root=1-680aca65-35900b60338bc10a1dd5e419",
            },
            "RetryAttempts": 0,
        },
        "id": "xxx",
        "name": "andy-test-graph",
        "arn": "arn:aws:neptune-graph:us-west-2:613481987977:graph/xxx",
        "status": "RESETTING",
        "createTime": "xxx",
        "provisionedMemory": 16,
        "endpoint": "xxx.us-west-2.neptune-graph.amazonaws.com",
        "publicConnectivity": True,
        "replicaCount": 0,
        "kmsKeyIdentifier": "AWS_OWNED_KEY",
        "deletionProtection": True,
        "buildNumber": "1.1.1742699706",
    }

    # Call the method with test parameters
    result = InstanceUtil._reset_graph(mock_client, "test-graph-id")

    # Verify the result is True
    assert result is True


def test_reset_graph_endpoint_connection_error():
    """Test that reset_graph returns False when an EndpointConnectionError occurs"""
    # Create a mock client
    mock_client = MagicMock()

    # Mock the reset_graph method to raise an EndpointConnectionError
    mock_client.reset_graph.side_effect = EndpointConnectionError(
        endpoint_url="https://neptune-graph.us-east-1.amazonaws.com"
    )

    with pytest.raises(EndpointConnectionError, match="Could not connect"):
        InstanceUtil._reset_graph(mock_client, "test-graph-id")
