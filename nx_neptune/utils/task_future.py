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

_ASYNC_MAX_ATTEMPTS = 60


class TaskType(Enum):
    IMPORT = (1, ["INI", "INITIALIZING", "ANALYZING_DATA", "IMPORTING"], "SUCCEEDED")
    EXPORT = (2, ["INI", "INITIALIZING", "EXPORTING"], "SUCCEEDED")
    CREATE = (3, ["INI", "CREATING"], "AVAILABLE")
    DELETE = (4, ["INI", "DELETING"], "DELETED")
    NOOP = (5, ["INI"], "AVAILABLE")
    START = (6, ["INI", "STARTING"], "AVAILABLE")
    STOP = (7, ["INI", "STOPPING"], "STOPPED")
    EXPORT_SNAPSHOT = (8, ["INI", "SNAPSHOTTING"], "AVAILABLE")
    DELETE_SNAPSHOT = (9, ["INI", "DELETING"], "DELETED")

    def __init__(self, num_value, permitted_statuses, status_complete):
        self._value_ = num_value
        self.permitted_statuses = permitted_statuses
        self.status_complete = status_complete


def _delete_status_check_wrapper(client, graph_id):
    try:
        return client.get_graph(graphIdentifier=graph_id)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"status": "DELETED"}
        raise


def _delete_snapshot_status_check_wrapper(client, snapshot_id: str):
    try:
        return client.get_graph_snapshot(snapshotIdentifier=snapshot_id)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"status": "DELETED"}
        raise


def _get_task_action_map(client, task_id):
    return {
        TaskType.IMPORT: lambda: client.get_import_task(taskIdentifier=task_id),
        TaskType.EXPORT: lambda: client.get_export_task(taskIdentifier=task_id),
        TaskType.CREATE: lambda: client.get_graph(graphIdentifier=task_id),
        TaskType.DELETE: lambda: _delete_status_check_wrapper(client, task_id),
        TaskType.START: lambda: client.get_graph(graphIdentifier=task_id),
        TaskType.STOP: lambda: client.get_graph(graphIdentifier=task_id),
        TaskType.EXPORT_SNAPSHOT: lambda: client.get_graph(graphIdentifier=task_id),
        TaskType.DELETE_SNAPSHOT: lambda: _delete_snapshot_status_check_wrapper(
            client, task_id
        ),
    }


class TaskFuture(Future):
    """A Future subclass that tracks Neptune Analytics task information."""

    def __init__(
        self, task_id, task_type, polling_interval=10, max_attempts=_ASYNC_MAX_ATTEMPTS
    ):
        super().__init__()
        self.task_id = task_id
        self.task_type = task_type
        self.polling_interval = polling_interval
        self.max_attempts = max_attempts

    async def wait_until_complete(self, client: BaseClient):
        """Asynchronously monitor a Neptune Analytics task until completion."""
        logger.debug(
            f"Perform Neptune Analytics job status check on Type: [{self.task_type}] with ID: [{self.task_id}]"
        )

        status_list = self.task_type.permitted_statuses
        status = "INI"
        task_max_attempts = self.max_attempts
        task_action_map = _get_task_action_map(client, self.task_id)

        while status in status_list:
            try:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"[{current_time}] Current status: {status}")

                response = task_action_map[self.task_type]()
                status = response.get("status")

                if status == self.task_type.status_complete:
                    logger.info(f"Task [{self.task_id}] completed at [{current_time}]")
                    self.set_result(self.task_id)
                    return
                elif status in status_list:
                    task_max_attempts -= 1
                    if task_max_attempts <= 0:
                        logger.error(
                            f"Maximum number of attempts reached: status is {status} on type: {self.task_type}"
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
                        return
                    await asyncio.sleep(self.polling_interval)
                else:
                    logger.error(
                        f"Unexpected status: {status} on type: {self.task_type}"
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
                    return
            except ClientError as e:
                self.set_exception(e)
                return
