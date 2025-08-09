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
