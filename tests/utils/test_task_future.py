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
from botocore.exceptions import ClientError

from nx_neptune.utils.task_future import TaskFuture, TaskType


@pytest.mark.asyncio
async def test_max_attempts_reached_sets_exception():
    """Test that future sets exception when max_attempts is reached."""
    mock_client = MagicMock()
    mock_client.get_import_task.return_value = {"status": "IMPORTING"}

    future = TaskFuture(
        "test-task-id", TaskType.IMPORT, polling_interval=0.01, max_attempts=2
    )

    await future.wait_until_complete(mock_client)

    assert future.done()
    assert future.exception() is not None

    exception = future.exception()
    assert isinstance(exception, ClientError)
    assert exception.response["Error"]["Code"] == "MaxAttemptsReached"


@pytest.mark.asyncio
async def test_max_attempts_exception_not_raised():
    """Test that exception is set on future instead of being raised."""
    mock_client = MagicMock()
    mock_client.get_export_task.return_value = {"status": "EXPORTING"}

    future = TaskFuture(
        "test-export-id", TaskType.EXPORT, polling_interval=0.01, max_attempts=1
    )

    # Should not raise exception
    await future.wait_until_complete(mock_client)

    assert future.done()
    assert future.exception() is not None


@pytest.mark.asyncio
async def test_unexpected_status_sets_exception():
    """Test that unexpected status sets exception on future."""
    mock_client = MagicMock()
    mock_client.get_graph.return_value = {"status": "FAILED"}

    future = TaskFuture("test-graph-id", TaskType.CREATE, polling_interval=0.01)

    await future.wait_until_complete(mock_client)

    assert future.done()
    assert future.exception() is not None

    exception = future.exception()
    assert isinstance(exception, ClientError)
    assert exception.response["Error"]["Code"] == "UnexpectedStatus"


@pytest.mark.asyncio
async def test_client_error_sets_exception():
    """Test that ClientError from API call sets exception on future."""
    mock_client = MagicMock()
    mock_client.get_import_task.side_effect = ClientError(
        {"Error": {"Code": "InvalidTaskId", "Message": "Task not found"}},
        "get_import_task",
    )

    future = TaskFuture("invalid-task-id", TaskType.IMPORT, polling_interval=0.01)

    await future.wait_until_complete(mock_client)

    assert future.done()
    assert future.exception() is not None

    exception = future.exception()
    assert isinstance(exception, ClientError)
    assert exception.response["Error"]["Code"] == "InvalidTaskId"


@pytest.mark.asyncio
async def test_successful_completion():
    """Test that future completes successfully when task reaches completion status."""
    mock_client = MagicMock()
    mock_client.get_import_task.return_value = {"status": "SUCCEEDED"}

    future = TaskFuture("test-task-id", TaskType.IMPORT, polling_interval=0.01)

    await future.wait_until_complete(mock_client)

    assert future.done()
    assert future.exception() is None
    assert future.result() == "test-task-id"
