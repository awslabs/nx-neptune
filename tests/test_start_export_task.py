import pytest
from unittest.mock import MagicMock

import nx_neptune.instance_management as InstanceUtil
from botocore.exceptions import ClientError


def test_start_export_task_success():
    """Test that start_export_task returns the task ID when successful"""
    # Create a mock client
    mock_client = MagicMock()

    # Mock the start_export_task response
    mock_client.start_export_task.return_value = {
        "taskId": "test-task-id",
        "status": "PENDING",
    }

    # Call the method with test parameters
    result = InstanceUtil._start_export_task(
        mock_client,
        "test-graph-id",
        "s3://test-bucket/export/",
        "arn:aws:iam::123456789012:role/test-role",
        "arn:aws:kms:us-east-1:123456789012:key/test-key",
    )

    # Verify the result is the task ID
    assert result == "test-task-id"


def test_start_export_task_client_error():
    """Test that start_export_task returns None when a ClientError occurs"""
    # Create a mock client
    mock_client = MagicMock()

    # Mock the start_export_task method to raise a ClientError
    mock_client.start_export_task.side_effect = ClientError(
        {"Error": {"Code": "InvalidGraphId", "Message": "Graph ID not found"}},
        "StartExportTask",
    )

    with pytest.raises(ClientError, match="InvalidGraphId"):
        InstanceUtil._start_export_task(
            mock_client,
            "invalid-graph-id",
            "s3://test-bucket/export/",
            "arn:aws:iam::123456789012:role/test-role",
            "arn:aws:kms:us-east-1:123456789012:key/test-key",
        )
