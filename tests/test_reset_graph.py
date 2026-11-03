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
from unittest.mock import AsyncMock, MagicMock, patch

import nx_neptune.instance_management as InstanceUtil
from botocore.exceptions import ClientError, EndpointConnectionError


def _managed_client(tags=None):
    client = MagicMock()
    client.get_graph.return_value = {"arn": "arn:graph/test-graph-id"}
    client.list_tags_for_resource.return_value = {
        "tags": (
            {"agent": "nx-neptune", "nx-neptune:managed": "true"}
            if tags is None
            else tags
        )
    }
    client.reset_graph.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200},
        "status": "RESETTING",
    }
    return client


@pytest.mark.asyncio
async def test_reset_graph_success():
    """Test that reset_graph returns True when the operation is successful"""
    # Create a mock client for a graph created by nx-neptune
    mock_client = _managed_client()

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
        "arn": "arn:aws:neptune-graph:us-west-2:123456789000:graph/xxx",
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
    result = await InstanceUtil.reset_graph(
        "test-graph-id", mock_client, check_permissions=False
    )

    # Verify the result is True
    assert result is "test-graph-id"


@pytest.mark.asyncio
async def test_reset_graph_endpoint_connection_error():
    """Test that reset_graph returns False when an EndpointConnectionError occurs"""
    # Create a mock client for a graph created by nx-neptune
    mock_client = _managed_client()

    # Mock the reset_graph method to raise an EndpointConnectionError
    mock_client.reset_graph.side_effect = EndpointConnectionError(
        endpoint_url="https://neptune-graph.us-east-1.amazonaws.com"
    )

    with pytest.raises(EndpointConnectionError, match="Could not connect"):
        await InstanceUtil.reset_graph(
            "test-graph-id", mock_client, check_permissions=False
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("tags", [{}, {"agent": "other"}, {"Name": "my-graph"}])
async def test_reset_refuses_graph_not_created_by_library(tags):
    """A graph without the nx-neptune tags is not reset."""
    client = _managed_client(tags)

    with pytest.raises(ValueError, match="not created by nx-neptune"):
        await InstanceUtil.reset_graph("test-graph-id", client, check_permissions=False)

    client.reset_graph.assert_not_called()


@pytest.mark.asyncio
async def test_reset_allow_unmanaged_skips_tag_check():
    """allow_unmanaged=True resets a graph without reading its tags."""
    client = _managed_client({})
    client.get_graph.side_effect = [{"status": "AVAILABLE"}]

    with patch(
        "nx_neptune.instance_management.TaskFuture.wait_until_complete",
        new_callable=AsyncMock,
    ):
        result = await InstanceUtil.reset_graph(
            "test-graph-id", client, check_permissions=False, allow_unmanaged=True
        )

    assert result == "test-graph-id"
    client.list_tags_for_resource.assert_not_called()
    client.reset_graph.assert_called_once()


@pytest.mark.asyncio
async def test_reset_tag_read_error_raises_without_reset():
    """If the tags cannot be read, the AWS error is raised and nothing is reset."""
    client = _managed_client()
    client.list_tags_for_resource.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
        "ListTagsForResource",
    )

    with pytest.raises(ClientError, match="AccessDeniedException"):
        await InstanceUtil.reset_graph("test-graph-id", client, check_permissions=False)

    client.reset_graph.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("check_permissions", [True, False])
async def test_reset_permission_check_knob(check_permissions):
    """check_permissions controls the IAM check."""
    client = _managed_client()
    with (
        patch("nx_neptune.instance_management._create_iam_wrapper") as mock_wrapper,
        patch(
            "nx_neptune.instance_management.TaskFuture.wait_until_complete",
            new_callable=AsyncMock,
        ),
    ):
        await InstanceUtil.reset_graph(
            "test-graph-id", client, check_permissions=check_permissions
        )

    check = mock_wrapper.return_value.has_reset_na_permissions
    assert check.called is check_permissions


@pytest.mark.asyncio
async def test_reset_permission_denied_raises_before_reset():
    """A failed permission check raises and nothing is reset."""
    client = _managed_client()
    with patch("nx_neptune.instance_management._create_iam_wrapper") as mock_wrapper:
        mock_wrapper.return_value.has_reset_na_permissions.side_effect = ValueError(
            "Insufficient permission, neptune-graph:ResetGraph"
        )
        with pytest.raises(ValueError, match="Insufficient permission"):
            await InstanceUtil.reset_graph("test-graph-id", client)

    client.list_tags_for_resource.assert_not_called()
    client.reset_graph.assert_not_called()
