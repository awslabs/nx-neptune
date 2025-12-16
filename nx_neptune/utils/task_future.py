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
import asyncio
import logging
from asyncio import Future
from datetime import datetime
from enum import Enum

from botocore.client import BaseClient
from botocore.exceptions import ClientError

__all__ = ["TaskType", "TaskFuture"]

logger = logging.getLogger(__name__)

_ASYNC_POLLING_INTERVAL = 30
_ASYNC_MAX_ATTEMPTS = 60


class TaskType(Enum):
    # Allow import to run against an "INITIALIZING" state - the graph is sometimes in this state after creating graph
    IMPORT = (1, ["INITIALIZING", "ANALYZING_DATA", "IMPORTING"], "SUCCEEDED")
    # Allow export to run against an "INITIALIZING" state - the graph is sometimes in this state after running algorithms
    EXPORT = (2, ["INITIALIZING", "EXPORTING"], "SUCCEEDED")
    CREATE = (3, ["CREATING"], "AVAILABLE")
    DELETE = (4, ["DELETING"], "DELETED")
    NOOP = (5, ["N/A"], "AVAILABLE")
    START = (6, ["STARTING"], "AVAILABLE")
    STOP = (7, ["STOPPING"], "STOPPED")
    EXPORT_SNAPSHOT = (8, ["SNAPSHOTTING"], "AVAILABLE")
    DELETE_SNAPSHOT = (9, ["DELETING"], "DELETED")
    RESET_GRAPH = (10, ["RESETTING"], "AVAILABLE")
    EXPORT_ATHENA_TABLE = (11, ["QUEUED", "RUNNING"], "SUCCEEDED")
    UPDATE = (12, ["UPDATING"], "AVAILABLE")

    def __init__(self, num_value, permitted_statuses, status_complete):
        self._value_ = num_value
        self.permitted_statuses = permitted_statuses
        self.status_complete = status_complete


def _delete_status_check_wrapper(client, graph_id):
    """
    Wrapper method to suppress error when graph_id not found,
    as this is an indicator of successful deletion.

    Args:
        client (client): The boto client
        graph_id (str): The String identify for the remote Neptune Analytics graph

    Returns:
        str: The original response from Boto or a mocked response to represent
        successful deletion, in the case of resource not found.
    """
    try:
        return client.get_graph(graphIdentifier=graph_id)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            return {"status": "DELETED"}
        else:
            raise e


def _delete_snapshot_status_check_wrapper(client, snapshot_id: str):
    """
    Wrapper method to suppress error when snapshot_id not found,
    as this is an indicator of successful deletion.

    Args:
        client (client): The boto client
        snapshot_id (str): The String identifier for the Neptune Analytics snapshot

    Returns:
        str: The original response from Boto or a mocked response to represent
        successful deletion, in the case of resource not found.
    """
    try:
        return client.get_graph_snapshot(snapshotIdentifier=snapshot_id)
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            return {"status": "DELETED"}
        else:
            raise e


def _get_task_action_map(client, task_id):
    return {
        TaskType.IMPORT: lambda: client.get_import_task(taskIdentifier=task_id),  # type: ignore[attr-defined]
        TaskType.EXPORT: lambda: client.get_export_task(taskIdentifier=task_id),  # type: ignore[attr-defined]
        TaskType.CREATE: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
        TaskType.DELETE: lambda: _delete_status_check_wrapper(client, task_id),
        TaskType.START: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
        TaskType.STOP: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
        TaskType.RESET_GRAPH: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
        TaskType.EXPORT_SNAPSHOT: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
        TaskType.DELETE_SNAPSHOT: lambda: _delete_snapshot_status_check_wrapper(
            client, task_id
        ),
        TaskType.EXPORT_ATHENA_TABLE: lambda: {
            "status": client.get_query_execution(QueryExecutionId=task_id)[
                "QueryExecution"
            ]["Status"]["State"]
        },  # type: ignore[attr-defined]
        TaskType.UPDATE: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
    }


async def wait_until_all_complete(
    task_ids: list[str],
    task_type,
    client: BaseClient,
    polling_interval=None,
    max_attempts=None,
):
    attempt = 0

    tasks = [
        TaskFuture(task_id, task_type, polling_interval, max_attempts)
        for task_id in task_ids
    ]
    completed_tasks = []

    while True:
        try:
            resolved_tasks = []
            for task in tasks:
                task.check_status(client, attempt)
                if task.done():
                    resolved_tasks.append(task)

            for resolved_task in resolved_tasks:
                completed_tasks.append(resolved_task)
                tasks.remove(resolved_task)

            if len(tasks) <= 0:
                return completed_tasks

            attempt += 1

            # sleep for the polling interval
            await asyncio.sleep(polling_interval)

        except ClientError as e:
            raise e


class TaskFuture(Future):
    """A Future subclass that tracks Neptune Analytics task information."""

    def __init__(
        self,
        task_id,
        task_type,
        polling_interval=None,
        max_attempts=None,
    ):
        super().__init__()
        self.task_id = task_id
        self.task_type = task_type
        self.current_status = None
        self.polling_interval = (
            polling_interval
            if polling_interval is not None
            else _ASYNC_POLLING_INTERVAL
        )
        self.max_attempts = (
            max_attempts if max_attempts is not None else _ASYNC_MAX_ATTEMPTS
        )

    def check_status(self, client: BaseClient, attempt: int) -> bool:

        response = _get_task_action_map(client, self.task_id)[self.task_type]()
        self.current_status = response.get("status")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            f"[{current_time}] Task [{self.task_id}] Current status: {self.current_status}"
        )

        if self.current_status == self.task_type.status_complete:
            logger.info(f"Task [{self.task_id}] completed at [{current_time}]")
            self.set_result(self.task_id)
            # done with result
            return True

        # check max attempts
        if attempt >= self.max_attempts:
            logger.error(
                f"Maximum number of attempts reached: status is {self.current_status} on type: {self.task_type}"
            )
            self.set_exception(
                ClientError(
                    {
                        "Error": {
                            "Code": "MaxAttemptsReached",
                            "Message": "Maximum attempts reached",
                        }
                    },
                    "wait_until_complete",
                )
            )
            # done with error
            return True

        if self.current_status not in self.task_type.permitted_statuses:
            logger.error(
                f"Unexpected status: {self.current_status} on type: {self.task_type}"
            )
            self.set_exception(
                ClientError(
                    {
                        "Error": {
                            "Code": "UnexpectedStatus",
                            "Message": "Unexpected status",
                        }
                    },
                    "wait_until_complete",
                )
            )
            # done with error
            return True

        # else not done
        return False

    async def wait_until_complete(self, client: BaseClient):
        """Asynchronously monitor a Neptune Analytics task until completion.

        This function polls the status of an import or export task until it completes
        or fails, then resolves the provided Future accordingly.

        Args:
            client (boto3.client): The Neptune Analytics boto3 client

        Raises:
            ClientError: If there's an issue with the AWS API call
            ValueError: If an unknown task type is provided
        """
        logger.debug(
            f"Perform Neptune Analytics job status check on Type: [{self.task_type}] with ID: [{self.task_id}]"
        )

        attempt = 0

        while True:
            try:
                self.check_status(client, attempt)
                if self.done():
                    return

                attempt += 1

                # sleep for the polling interval
                await asyncio.sleep(self.polling_interval)

            except ClientError as e:
                raise e
