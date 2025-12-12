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
import os
import uuid
from enum import Enum
from typing import Any, Optional, Tuple

import boto3
import jmespath
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError
from sqlglot import exp, parse_one

from .clients import SERVICE_IAM, SERVICE_NA, SERVICE_STS, IamClient
from .clients.neptune_constants import APP_ID_NX, SERVICE_ATHENA, SERVICE_S3
from .na_graph import NeptuneGraph

__all__ = [
    "import_csv_from_s3",
    "export_csv_to_s3",
    "create_na_instance",
    "create_na_instance_with_s3_import",
    "create_na_instance_from_snapshot",
    "create_graph_snapshot",
    "delete_na_instance",
    "export_athena_table_to_s3",
    "create_csv_table_from_s3",
    "create_iceberg_table_from_table",
    "validate_athena_query",
    "validate_permissions",
    "start_na_instance",
    "stop_na_instance",
    "delete_graph_snapshot",
]

from .utils.task_future import (
    _ASYNC_POLLING_INTERVAL,
    TaskFuture,
    TaskType,
    wait_until_all_complete,
)

logger = logging.getLogger(__name__)

_PROJECT_IDENTIFIER = "nx-neptune"

_ENV_SIZE_LIMIT = "NETWORKX_GRAPH_SIZE_LIMIT"


async def create_na_instance(
    config: Optional[dict] = None,
    na_client: Optional[BaseClient] = None,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    graph_name_prefix: Optional[str] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """
    Creates a new graph instance for Neptune Analytics.

    Args:
        config (Optional[dict]): Optional dictionary of custom configuration parameters
            to use when creating the Neptune Analytics instance. If not provided,
            default settings will be applied.
            All options listed under boto3 documentations are supported.

            Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune-graph/client/create_graph.html
        na_client (Optional[BaseClient]): Optional boto3 client for neptune-graph service
        sts_client (Optional[BaseClient]): Optional boto3 client for sts service
        iam_client (Optional[BaseClient]): Optional boto3 client for iam service
        graph_name_prefix (Optional[str]): Optional prefix for the generated graph name
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The graph ID of the created Neptune Analytics instance

    Raises:
        Exception: If the Neptune Analytics instance creation fails
    """
    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)
    iam_client.has_create_na_permissions()

    response = _create_na_instance_task(na_client, config, graph_name_prefix)
    prospective_graph_id = _get_graph_id(response)
    status_code = _get_status_code(response)

    if status_code == 201:
        await _get_status_check_future(
            na_client,
            TaskType.CREATE,
            prospective_graph_id,
            polling_interval,
            max_attempts,
        )
        return prospective_graph_id
    else:
        raise Exception(
            f"Neptune instance creation failure with graph name {prospective_graph_id}"
        )


async def create_na_instance_with_s3_import(
    s3_arn: str,
    config: Optional[dict] = None,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
) -> tuple[str, str]:
    """Creates a new Neptune Analytics graph instance and imports data from S3.

    This function creates a new Neptune Analytics graph instance and immediately starts
    importing data from the specified S3 location. It handles the complete workflow:
    1. Validates required permissions
    2. Creates a new graph instance and trigger the import task
    3. Waits for both import and instance creation to complete

    Args:
        s3_arn (str): The S3 location containing CSV data (e.g., 's3://bucket-name/prefix/')
        config (Optional[dict]): Optional dictionary of custom configuration parameters
            to use when creating the Neptune Analytics instance. If not provided,
            default settings will be applied.
            All options listed under boto3 documentations are supported.

            Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune-graph/client/create_graph_using_import_task.html
        sts_client (Optional[IamClient]): Optional StsClient instance. If not provided,
            a new one will be created using the current user's credentials.
        iam_client (Optional[IamClient]): Optional IamClient instance. If not provided,
            a new one will be created using the current user's credentials.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided,
            a new one will be created.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        (str, str): A tuple with the graph and task ids. The graph ID is the new instance graph identifier.
        The task ID is the execution task when the import completes and instance is available for computation work.

    Raises:
        Exception: If the Neptune Analytics instance creation or import task fails
        ValueError: If the role lacks required permissions
    """

    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)
    # Retrieve key_arn for the bucket and permission check if present
    key_arn = _get_bucket_encryption_key_arn(s3_arn)
    # Permission checks
    iam_client.has_create_na_permissions()
    iam_client.has_import_from_s3_permissions(s3_arn, key_arn)

    graph_name = _create_random_graph_name()
    kwargs = _get_create_instance_with_import_config(
        graph_name, s3_arn, iam_client.role_arn, config
    )
    response = na_client.create_graph_using_import_task(**kwargs)
    graph_id = response.get("graphId")
    task_id = response.get("taskId")

    if _get_status_code(response) == 201:
        # Import task status check
        await _get_status_check_future(
            na_client, TaskType.IMPORT, task_id, polling_interval, max_attempts
        )

        # Wait for instance creation
        await _get_status_check_future(
            na_client, TaskType.CREATE, graph_id, polling_interval, max_attempts
        )

        return graph_id, task_id
    else:
        raise Exception(
            f"Neptune instance creation failure with import task ID: {task_id}"
        )


async def create_na_instance_from_snapshot(
    snapshot_id: str,
    config: Optional[dict] = None,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """
    Creates a new Neptune Analytics graph instance from an existing snapshot.

    Args:
        snapshot_id (str): The ID of the snapshot to restore from
        config (Optional[dict]): Optional dictionary of custom configuration parameters
            to use when creating the Neptune Analytics instance. If not provided,
            default settings will be applied.
            All options listed under boto3 documentations are supported.

            Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune-graph/client/restore_graph_from_snapshot.html
        sts_client (Optional[BaseClient]): Optional STS boto3 client. If not provided,
            a new one will be created.
        iam_client (Optional[BaseClient]): Optional IAM boto3 client. If not provided,
            a new one will be created using the current user's credentials.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided,
            a new one will be created.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The graph ID when the graph creation completes

    Raises:
        Exception: If the Neptune Analytics instance creation fails
        ValueError: If the role lacks required permissions
    """
    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)

    # Permissions check
    iam_client.has_create_na_from_snapshot_permissions()

    response = _create_na_instance_from_snapshot_task(na_client, snapshot_id, config)
    prospective_graph_id = _get_graph_id(response)

    if _get_status_code(response) == 201:
        await _get_status_check_future(
            na_client,
            TaskType.CREATE,
            prospective_graph_id,
            polling_interval,
            max_attempts,
        )
        return prospective_graph_id

    raise Exception(
        f"Neptune instance creation failure with graph identifier {prospective_graph_id}"
    )


async def delete_graph_snapshot(
    snapshot_id: str,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """
    Delete a Neptune Analytics graph snapshot.

    Args:
        snapshot_id (str): The ID of the snapshot to delete
        sts_client (Optional[BaseClient]): Optional STS boto3 client. If not provided,
            a new one will be created.
        iam_client (Optional[BaseClient]): Optional IAM boto3 client. If not provided,
            a new one will be created using the current user's credentials.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided,
            a new one will be created.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The snapshot ID when the deletion completes

    Raises:
        Exception: If the snapshot deletion fails
        ValueError: If the role lacks required permissions
    """
    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)

    # Permissions check
    iam_client.has_delete_snapshot_permissions()
    response = na_client.delete_graph_snapshot(snapshotIdentifier=snapshot_id)

    status_code = _get_status_code(response)
    if status_code == 200:
        await _get_status_check_future(
            na_client,
            TaskType.DELETE_SNAPSHOT,
            snapshot_id,
            polling_interval,
            max_attempts,
        )
        return snapshot_id

    raise Exception(
        f"Invalid response status code: {status_code} with full response:\n {response}"
    )


async def start_na_instance(
    graph_id: str,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """
    Start a stopped Neptune Analytics graph instance.

    Args:
        graph_id (str): The ID of the Neptune Analytics graph to start
        sts_client (Optional[BaseClient]): Optional STS boto3 client. If not provided,
            a new one will be created.
        iam_client (Optional[BaseClient]): Optional IAM boto3 client. If not provided,
            a new one will be created using the current user's credentials.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided,
            a new one will be created.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The graph ID of the started instance

    Raises:
        Exception: If the start operation fails with an invalid status code
        ValueError: If the role lacks required permissions or if graph is not in STOPPED state
    """
    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)
    iam_client.has_start_na_permissions()

    if status_exception := _graph_status_check(na_client, graph_id, "STOPPED"):
        await status_exception  # This will raise the exception

    response = na_client.start_graph(graphIdentifier=graph_id)
    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.START, polling_interval, max_attempts)
        await fut.wait_until_complete(na_client)
        return graph_id
    else:
        raise Exception(
            f"Invalid response status code: {status_code} with full response:\n {response}"
        )


async def stop_na_instance(
    graph_id: str,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """Stop a running Neptune Analytics graph instance.

    Args:
        graph_id (str): The ID of the Neptune Analytics graph to stop
        sts_client (Optional[BaseClient]): Optional STS boto3 client. If not provided,
            a new one will be created.
        iam_client (Optional[BaseClient]): Optional IAM boto3 client. If not provided,
            a new one will be created using the current user's credentials.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided,
            a new one will be created.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The graph ID of the stopped instance

    Raises:
        Exception: If the stop operation fails with an invalid status code
        ValueError: If the role lacks required permissions or if graph is not in AVAILABLE state
    """
    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)
    iam_client.has_stop_na_permissions()

    if status_exception := _graph_status_check(na_client, graph_id, "AVAILABLE"):
        await status_exception  # This will raise the exception

    response = na_client.stop_graph(graphIdentifier=graph_id)
    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.STOP, polling_interval, max_attempts)
        await fut.wait_until_complete(na_client)
        return graph_id
    else:
        raise Exception(
            f"Invalid response status code: {status_code} with full response:\n {response}"
        )


async def delete_na_instance(
    graph_id: str,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """
    Delete a Neptune Analytics graph instance.

    Args:
        graph_id (str): The ID of the Neptune Analytics graph to delete
        sts_client (Optional[BaseClient]): Optional STS boto3 client. If not provided,
            a new one will be created.
        iam_client (Optional[BaseClient]): Optional IAM boto3 client. If not provided,
            a new one will be created using the current user's credentials.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided,
            a new one will be created.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The graph ID of the deleted instance

    Raises:
        Exception: If the deletion fails with an invalid status code
        ValueError: If the role lacks required permissions
    """

    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)
    # Permission check
    iam_client.has_delete_na_permissions()

    response = _delete_na_instance_task(na_client, graph_id)
    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.DELETE, polling_interval, max_attempts)
        await fut.wait_until_complete(na_client)
        return graph_id
    else:
        raise Exception(
            f"Invalid response status code: {status_code} with full response:\n {response}"
        )


async def create_graph_snapshot(
    graph_id: str,
    snapshot_name: str,
    tag: Optional[dict] = None,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """Create a snapshot of a Neptune Analytics graph.

    Args:
        graph_id (str): The ID of the Neptune Analytics graph to snapshot
        snapshot_name (str): Name to assign to the snapshot
        tag (Optional[dict]): Optional tags to apply to the snapshot
        sts_client (Optional[BaseClient]): Optional STS boto3 client. If not provided,
            a new one will be created.
        iam_client (Optional[BaseClient]): Optional IAM boto3 client. If not provided,
            a new one will be created using the current user's credentials.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided,
            a new one will be created.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The graph ID when the snapshot completes

    Raises:
        Exception: If the snapshot creation fails with an invalid status code
        ValueError: If the role lacks required permissions
    """
    # Permission check
    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)
    iam_client.has_create_na_snapshot_permissions()

    kwargs: dict[str, Any] = {
        "graphIdentifier": graph_id,
        "snapshotName": snapshot_name,
    }
    if tag:
        kwargs["tags"] = tag

    response = na_client.create_graph_snapshot(**kwargs)
    status_code = _get_status_code(response)
    if status_code == 201:
        await _get_status_check_future(
            na_client,
            TaskType.EXPORT_SNAPSHOT,
            graph_id,
            polling_interval,
            max_attempts,
        )
        return graph_id
    raise Exception(
        f"Invalid response status code: {status_code} with full response:\n {response}"
    )


async def import_csv_from_s3(
    na_graph: NeptuneGraph,
    s3_arn,
    reset_graph_ahead=True,
    skip_snapshot=True,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """Import CSV data from S3 into a Neptune Analytics graph.

    This function handles the complete workflow for importing graph data:
    1. Checks required permissions
    2. Optionally resets the graph
    3. Starts the import task
    4. Waits for completion

    Args:
        na_graph (NeptuneGraph): The Neptune Analytics graph instance
        s3_arn (str): The S3 location containing CSV data (e.g., 's3://bucket-name/prefix/')
        reset_graph_ahead (bool, optional): Whether to reset the graph before import. Defaults to True.
        skip_snapshot (bool, optional): Whether to skip creating a snapshot when resetting. Defaults to True.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The task ID of the completed import

    Raises:
        ValueError: If the role lacks required permissions
    """
    graph_id = na_graph.na_client.graph_id
    na_client = na_graph.na_client.client
    iam_client = na_graph.iam_client
    role_arn = iam_client.role_arn

    # Retrieve key_arn for the bucket and permission checks if present
    key_arn = _get_bucket_encryption_key_arn(s3_arn)

    # Run permission check
    iam_client.has_import_from_s3_permissions(s3_arn, key_arn)

    # Run reset
    if reset_graph_ahead:
        await reset_graph(graph_id, na_client, skip_snapshot)

    # Run Import
    task_id = _start_import_task(na_client, graph_id, s3_arn, role_arn)

    # Wait for completion
    future = TaskFuture(task_id, TaskType.IMPORT, polling_interval, max_attempts)
    await future.wait_until_complete(na_client)
    return task_id


async def export_csv_to_s3(
    na_graph: NeptuneGraph,
    s3_arn: str,
    export_filter=None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """Export graph data from Neptune Analytics to S3 in CSV format.

    This function handles the complete workflow for exporting graph data:
    1. Checks required permissions
    2. Starts the export task
    3. Waits for completion

    Args:
        na_graph (NeptuneGraph): The Neptune Analytics graph instance
        s3_arn (str): The S3 destination location (e.g., 's3://bucket-name/prefix/')
        export_filter (dict, optional): Filter criteria for the export. Defaults to None.
        polling_interval (int): Time interval in seconds to perform job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The task ID of the completed export

    Raises:
        ValueError: If the role lacks required permissions
    """
    graph_id = na_graph.na_client.graph_id
    na_client = na_graph.na_client.client
    iam_client = na_graph.iam_client
    role_arn = iam_client.role_arn

    # Retrieve key_arn for the bucket and permission check if present
    key_arn = _get_bucket_encryption_key_arn(s3_arn)

    # Run permission check
    iam_client.has_export_to_s3_permissions(s3_arn, key_arn)

    # Run Export
    task_id = _start_export_task(
        na_client, graph_id, s3_arn, role_arn, key_arn, export_filter=export_filter
    )

    # Wait for completion
    future = TaskFuture(task_id, TaskType.EXPORT, polling_interval, max_attempts)
    await future.wait_until_complete(na_client)
    return task_id


async def reset_graph(
    graph_id: str,
    na_client: BaseClient = None,
    skip_snapshot: bool = True,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """Reset the Neptune Analytics graph by clearing all data while preserving the graph configuration.

    Args:
        na_client (BaseClient): The Neptune Analytics boto3 client. If None, a new client will be created.
        graph_id (str): The ID of the Neptune Analytics graph to reset
        skip_snapshot (bool, optional): Whether to skip creating a snapshot before resetting. Defaults to True.
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        str: The graph ID of the reset instance

    Raises:
        ClientError: If there's an issue with the AWS API call
        Exception: If an invalid status code is returned
    """
    if na_client is None:
        na_client = boto3.client(
            service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
        )

    logger.info(
        f"Perform reset_graph action on graph: [{graph_id}] with skip_snapshot: [{skip_snapshot}]"
    )
    response = na_client.reset_graph(graphIdentifier=graph_id, skipSnapshot=skip_snapshot)  # type: ignore[attr-defined]
    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.RESET_GRAPH, polling_interval, max_attempts)
        await fut.wait_until_complete(na_client)
        return graph_id
    else:
        raise Exception(
            f"Invalid response status code: {status_code} with full response:\n {response}"
        )


async def update_na_instance_size(
    graph_id: str,
    prospect_size: int,
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
    polling_interval=None,
    max_attempts=None,
):
    """Update the provisioned memory size of a Neptune Analytics graph instance.

    This function handles updating the memory size of an existing Neptune Analytics graph instance.
    The new size must be a valid memory size supported by Neptune Analytics.

    Args:
        graph_id (str): The ID of the Neptune Analytics graph to resize
        prospect_size (int): The desired new memory size in GB
        sts_client (Optional[BaseClient]): Optional STS boto3 client. If not provided, a new one will be created.
        iam_client (Optional[BaseClient]): Optional IAM boto3 client. If not provided, a new one will be created.
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client. If not provided, a new one will be created.
        polling_interval (Optional[int]): Time interval in seconds for job status query
        max_attempts (Optional[int]): Maximum attempts for status checks

    Returns:
        str: The graph ID when the resize operation completes

    Raises:
        Exception: If the resize operation fails
        ValueError: If the role lacks required permissions
    """
    (iam_client, na_client) = _get_or_create_clients(sts_client, iam_client, na_client)

    # Permission check
    iam_client.has_update_na_permissions()

    logger.info(f"Resizing graph: {graph_id} with size: {prospect_size}")
    response = na_client.update_graph(
        graphIdentifier=graph_id, provisionedMemory=prospect_size
    )
    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.UPDATE, polling_interval, max_attempts)
        await fut.wait_until_complete(na_client)
        return graph_id
    else:
        raise Exception(
            f"Invalid response status code: {status_code} with full response:\n {response}"
        )


def _get_create_instance_config(graph_name, config=None):
    """
    Build and sanitize the configuration dictionary for creating a graph instance.

    This function filters the provided `config` to include only permitted keys,
    fills in default values for required parameters if they are missing, and
    ensures the presence of the 'agent' tag and the graph name.

    Args:
        graph_name (str): The name of the graph to create. This is always included in the result.
        config (dict, optional): An optional dictionary of user-provided configuration values.

    Returns:
        dict: A sanitized and completed configuration dictionary with required keys and values.
    """

    config = config or {}
    # Ensure mandatory config present.
    config.setdefault("publicConnectivity", True)
    config.setdefault("replicaCount", 0)
    config.setdefault("deletionProtection", False)
    config.setdefault("provisionedMemory", 16)

    # Make sure agent tag shows regardless
    config["graphName"] = graph_name
    config.setdefault("tags", {}).setdefault("agent", _PROJECT_IDENTIFIER)

    return config


def _get_create_instance_with_import_config(
    graph_name, s3_location, role_arn, config=None
):
    """
    Build and sanitize the configuration dictionary for creating a graph instance with import.

    This function filters the provided `config` to include only permitted keys,
    fills in default values for required parameters if they are missing, and
    ensures the presence of required parameters for graph creation with import.

    Args:
        graph_name (str): The name of the graph to create
        s3_location (str): The S3 location containing data to import
        role_arn (str): The IAM role ARN with permissions to read from S3
        config (dict, optional): An optional dictionary of user-provided configuration values.
            Supported keys include:
            - publicConnectivity (bool): Whether the graph has public connectivity
            - replicaCount (int): Number of read replicas
            - deletionProtection (bool): Whether deletion protection is enabled
            - minProvisionedMemory (int): Minimum provisioned memory in GB
            - maxProvisionedMemory (int): Maximum provisioned memory in GB
            - format (str): Import data format (e.g. "CSV")
            - tags (dict): Resource tags

    Returns:
        dict: A sanitized and completed configuration dictionary with required keys and values
            for creating a graph with import
    """

    config = config or {}
    # Ensure mandatory config present.
    config.setdefault("publicConnectivity", True)
    config.setdefault("replicaCount", 0)
    config.setdefault("deletionProtection", False)
    config.setdefault("minProvisionedMemory", 16)
    config.setdefault("maxProvisionedMemory", 32)
    config.setdefault("format", "CSV")

    # Make sure agent tag shows regardless
    config["graphName"] = graph_name
    config["source"] = s3_location
    config["roleArn"] = role_arn
    config.setdefault("tags", {}).setdefault("agent", _PROJECT_IDENTIFIER)

    return config


def _create_na_instance_task(
    client, config: Optional[dict] = None, graph_name_prefix: Optional[str] = None
):
    """Create a new Neptune Analytics graph instance with default settings.

    This function generates a unique name for the graph using a UUID suffix and
    creates a new Neptune Analytics graph instance with public connectivity.

    Args:
        client (boto3.client): The Neptune Analytics boto3 client
        config (Optional[dict]): Optional configuration parameters
        graph_name_prefix (Optional[str]): Optional prefix for the generated graph name

    Returns:
        dict: The API response containing information about the created graph

    Raises:
        ClientError: If there's an issue with the AWS API call
    """

    graph_name = _create_random_graph_name(graph_name_prefix)
    kwargs = _get_create_instance_config(graph_name, config)
    response = client.create_graph(**kwargs)
    return response


def _create_na_instance_from_snapshot_task(
    client, snapshot_identifier: str, config: Optional[dict] = None
):
    """Create a new Neptune Analytics graph instance with default settings.

    This function generates a unique name for the graph using a UUID suffix and
    creates a new Neptune Analytics graph instance with public connectivity.

    Args:
        client (boto3.client): The Neptune Analytics boto3 client

    Returns:
        dict: The API response containing information about the created graph

    Raises:
        ClientError: If there's an issue with the AWS API call
    """
    # Permission check
    graph_name = _create_random_graph_name()
    kwargs = _get_create_instance_config(graph_name, config)
    kwargs["snapshotIdentifier"] = snapshot_identifier
    response = client.restore_graph_from_snapshot(**kwargs)
    return response


def _delete_na_instance_task(client, graph_id: str):
    """Issue a Boto request to delete Neptune Analytics graph instance with graph_id.

    Args:
        client (boto3.client): The Neptune Analytics boto3 client
        graph_id (str): The graph ID to Identify the remote Neptune Analytics instance

    Returns:
        dict: The API response containing information about the deleted graph

    Raises:
        ClientError: If there's an issue with the AWS API call
    """
    response = client.delete_graph(graphIdentifier=graph_id, skipSnapshot=True)
    return response


def _start_import_task(
    client: BaseClient,
    graph_id: str,
    s3_location: str,
    role_arn: Optional[str],
    format_type: str = "CSV",
) -> str:
    """Start an import task for the Neptune Analytics graph.

    Args:
        client: The Neptune Analytics boto3 client
        graph_id (str): The ID of the Neptune Analytics graph
        s3_location (str): The S3 source location (e.g., 's3://bucket-name/prefix/')
        role_arn (str): The IAM role ARN with permissions to read from the S3 bucket
        format_type (str, optional): The format of the data to import ('CSV' or 'OPENCYPHER'). Defaults to "CSV".

    Returns:
        str: The import task ID if successful

    Raises:
        ClientError: If there's an issue with the AWS API call
    """
    logger.debug(f"Import S3 graph data [{s3_location}] into Graph [{graph_id}]")
    try:
        response = client.start_import_task(  # type: ignore[attr-defined]
            graphIdentifier=graph_id,
            source=s3_location,
            format=format_type,
            roleArn=role_arn,
        )
        task_id = response.get("taskId")
        return task_id
    except ClientError as e:
        raise e


def _start_export_task(
    client: BaseClient,
    graph_id: str,
    s3_destination: str,
    role_arn: str,
    kms_key_identifier: str,
    filetype: str = "CSV",
    export_filter: dict | None = None,
) -> str:
    """Export graph data to an S3 bucket in CSV format.

    Args:
        client: The Neptune Analytics boto3 client
        graph_id (str): The ID of the Neptune Analytics graph
        s3_destination (str): The S3 destination location (e.g., 's3://bucket-name/prefix/')
        role_arn (str): The IAM role ARN with permissions to write to the S3 bucket
        kms_key_identifier (str): KMS key ARN for encrypting the exported data
        filetype (str, optional): The format of the export data. Defaults to "CSV".
        export_filter (dict, optional): Filter criteria for the export. Defaults to None.
            Example filter to export only vertices with label "Person" and edges with label "FOLLOWS":
            {
                "vertexFilter": {"Person": {}},
                "edgeFilter": {"FOLLOWS": {}},
            }
            Example filter to export vertices with property "age":
            {
                "vertexFilter": {"Person": {"age": {"sourcePropertyName": "age"}}},
            }
            For more details on export filter syntax, see:
            https://docs.aws.amazon.com/neptune-analytics/latest/userguide/export-filter.html


    Returns:
        str: The export task ID if successful

    Raises:
        ClientError: If there's an issue with the AWS API call
    """
    logger.debug(f"Export S3 Graph [{graph_id}] data to S3 [{s3_destination}]")
    try:
        kwargs_export: dict[str, Any] = {
            "graphIdentifier": graph_id,
            "roleArn": role_arn,
            "format": filetype,
            "destination": s3_destination,
            "kmsKeyIdentifier": kms_key_identifier,
        }
        # Optional filter
        if export_filter:
            kwargs_export["exportFilter"] = export_filter

        response = client.start_export_task(  # type: ignore[attr-defined]
            **kwargs_export
        )
        task_id = response.get("taskId")
        return task_id

    except ClientError as e:
        raise e


def _get_bucket_encryption_key_arn(s3_arn):
    """
    Retrieve the KMS key ARN used for S3 bucket encryption.

    Args:
        s3_arn (str): S3 path in the format 's3://bucket-name/path/to/folder'

    Returns:
        str or None: KMS key ARN if the bucket uses KMS encryption, None otherwise
    """
    try:
        # Create an S3 client
        s3_client = boto3.client(SERVICE_S3)

        # Get the bucket encryption configuration
        bucket_name = _clean_s3_path(s3_arn)
        response = s3_client.get_bucket_encryption(Bucket=bucket_name)
        # Use jmespath to extract the KMS key ARN
        key_arn = jmespath.search(
            "ServerSideEncryptionConfiguration.Rules"
            "[?ApplyServerSideEncryptionByDefault.SSEAlgorithm=='aws:kms']"
            ".ApplyServerSideEncryptionByDefault.KMSMasterKeyID | [0]",
            response,
        )
        if key_arn is not None:
            logger.debug(f"Bucket: {bucket_name} with key_arn: {key_arn}")
            return key_arn
        else:
            logger.debug(
                f"Bucket: {bucket_name} has no client encryption key configured"
            )
            return None
    except Exception as e:
        logger.error(f"Error retrieving bucket encryption: {e}")
        return None


def _clean_s3_path(s3_path):
    """
    Extract the bucket name from an S3 path.

    Args:
        s3_path (str): S3 path in the format 's3://bucket-name/path/to/folder'

    Returns:
        str: The bucket name extracted from the path
    """
    # Remove 's3://' prefix
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    s3_path = s3_path.rstrip("/")
    parts = s3_path.split("/")
    # If there's at least one '/', remove the last part (folder at suffix)
    if len(parts) > 1:
        return "/".join(parts[:-1])

    # If there's no '/', return the bucket name
    return parts[0]


def _get_status_code(response: dict):
    """
    Extract the HTTP status code from an AWS API response.

    Args:
        response (dict): The AWS API response dictionary

    Returns:
        int or None: The HTTP status code if available, None otherwise
    """
    return (response.get("ResponseMetadata") or {}).get("HTTPStatusCode")


def _get_graph_id(response: dict):
    """
    Extract the graph ID from a Neptune Analytics API response.

    Args:
        response (dict): The Neptune Analytics API response dictionary

    Returns:
        str or None: The graph ID if available, None otherwise
    """
    return response.get("id")


def _create_random_graph_name(graph_name_prefix: Optional[str] = None) -> str:
    """Generate a unique name for a Neptune Analytics graph instance.

    This function creates a random graph name by combining the project identifier
    with a UUID to ensure uniqueness across all graph instances.

    Args:
        graph_name_prefix (Optional[str]): Optional prefix for the graph name.
            If not provided, uses _PROJECT_IDENTIFIER.

    Returns:
        str: A unique graph name in the format '{prefix}-{uuid}'
    """
    prefix = graph_name_prefix if graph_name_prefix is not None else _PROJECT_IDENTIFIER
    uuid_suffix = str(uuid.uuid4())
    return f"{prefix}-{uuid_suffix}"


# TODO: provide an alternative to sql_queries - instead take a JSON import to map types
async def export_athena_table_to_s3(
    sql_queries: list,
    s3_bucket: str,
    catalog: str = None,  # type: ignore[assignment]
    database: str = None,  # type: ignore[assignment]
    polling_interval=None,
    max_attempts=None,
) -> list[str]:
    """Export Athena table data to S3 by executing SQL queries.

    Args:
        :param sql_queries: List of SQL query strings to execute
        :param s3_bucket: S3 bucket path for query results
        :param catalog: (str, optional) catalog namespace to run the sql_query
        :param database: (str, optional) the database to run the sql_query
        :param polling_interval: Polling interval for status checks
        :param max_attempts: Maximum attempts for status checks

    Returns:
        list: True if all queries succeeded, False otherwise
    """
    client = boto3.client(SERVICE_ATHENA)

    # TODO: validate permissions - or fail
    # TODO: check s3 bucket location is empty - or fail

    query_execution_ids = []
    for query in sql_queries:
        query_execution_id = _execute_athena_query(
            client, query, s3_bucket, catalog=catalog, database=database
        )
        query_execution_ids.append(query_execution_id)

    # Wait on all query execution IDs
    s3_client = boto3.client(SERVICE_S3)
    bucket_name = s3_bucket.replace("s3://", "").split("/")[0]
    bucket_prefix = "/".join(s3_bucket.replace("s3://", "").split("/")[1:])

    await wait_until_all_complete(
        query_execution_ids,
        TaskType.EXPORT_ATHENA_TABLE,
        client,
        polling_interval,
        max_attempts,
    )

    # sanity check for files - and delete the metadata files
    for query_execution_id in query_execution_ids:
        # Sanity check that the CSV file exists
        csv_key = f"{bucket_prefix}{query_execution_id}.csv"
        s3_client.head_object(Bucket=bucket_name, Key=csv_key)
        logger.info(f"Confirmed CSV file exists: {csv_key}")

        # Remove metadata file
        metadata_key = f"{bucket_prefix}{query_execution_id}.csv.metadata"
        s3_client.delete_object(Bucket=bucket_name, Key=metadata_key)
        logger.info(f"Deleted metadata file: {metadata_key}")

    logger.info(
        f"Successfully completed execution of {len(query_execution_ids)} queries"
    )
    return query_execution_ids


async def create_csv_table_from_s3(
    s3_bucket: str,
    s3_output_bucket: str,
    table_name: str,
    catalog: str = None,  # type: ignore[assignment]
    database: str = None,  # type: ignore[assignment]
    table_columns: Optional[list[str]] = None,
    polling_interval=None,
    max_attempts=None,
) -> list[str]:
    """Create an external CSV table from S3 data using Athena queries.

    Args:
        :param s3_bucket: S3 bucket path containing csv data
        :param s3_output_bucket: S3 path to print results
        :param table_name: the table name to create iceberg-formatted data
        :param catalog: (str, optional) catalog namespace to run the sql_query
        :param database: (str, optional) the database to run the sql_query
        :param table_columns: (list, optional) table columns to include in the newly created query
        :param polling_interval: Polling interval for status checks
        :param max_attempts: Maximum attempts for status checks

    Returns:
        list[str]: List of query execution IDs if all queries succeeded

    Raises:
        Exception: If any query fails
    """
    # TODO check if s3_bucket is empty
    # TODO validate table_schema
    # TODO validate if table exists already
    # TODO check is skip drop table is False and table exists

    # Wait on all query execution IDs
    s3_client = boto3.client(SERVICE_S3)
    bucket_path = s3_bucket.replace("s3://", "").split("/")
    bucket_name = bucket_path.pop(0)
    bucket_prefix = "/".join(bucket_path)

    logger.debug(f"Inspecting files from {s3_bucket}")

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix)
    file_paths = response.get("Contents", [])

    logger.info(f"Moving 'Edge_*.csv' files to folder {s3_bucket}/Edge")
    edge_sql_statement = _build_sql_statement(
        s3_client,
        bucket_name,
        bucket_prefix,
        "Edge",
        file_paths,
        {
            "~id": "string",
            "~from": "string",
            "~to": "string",
            "~label": "string",
        },
        f"{table_name}_edges",
    )

    logger.info(f"Moving 'Vertex_*.csv' files to folder {s3_bucket}/Vertex")
    vertex_sql_statement = _build_sql_statement(
        s3_client,
        bucket_name,
        bucket_prefix,
        "Vertex",
        file_paths,
        {
            "~id": "string",
            "~label": "string",
        },
        f"{table_name}_vertices",
    )

    athena_client = boto3.client(SERVICE_ATHENA)
    query_execution_ids = []
    for sql_statement in [edge_sql_statement, vertex_sql_statement]:
        logger.info(f"SQL_CREATE_TABLE:\n{sql_statement}")
        query_execution_id = _execute_athena_query(
            athena_client,
            sql_statement,
            s3_output_bucket,
            catalog=catalog,
            database=database,
        )
        query_execution_ids.append(query_execution_id)

    await wait_until_all_complete(
        query_execution_ids,
        TaskType.EXPORT_ATHENA_TABLE,
        athena_client,
        polling_interval,
        max_attempts,
    )

    logger.info(
        f"Successfully completed execution of {len(query_execution_ids)} queries"
    )
    return query_execution_ids


def _build_sql_statement(
    s3_client,
    bucket_name: str,
    bucket_folder: str,
    prefix: str,
    file_paths: list[dict],
    table_columns: dict[str, str],
    table_name: str,
) -> str:

    # Move all the files with _prefix_ into a subfolder called _prefix_
    subfolder_file_paths = []
    for obj in file_paths:
        orig_key = obj["Key"]
        folder_path = orig_key.split("/")
        filename = folder_path.pop()
        if not filename.endswith(".csv") or not filename.startswith(prefix):
            continue

        # if we have already run this, the files are already moved to subfolder
        if folder_path[-1] == prefix:
            subfolder_file_paths.append(f"{'/'.join(folder_path)}/{filename}")

        else:
            # move files to new bucket subfolder
            dest_key = f"{'/'.join(folder_path)}/{prefix}/{filename}"
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": orig_key},
                Key=dest_key,
            )
            s3_client.delete_object(Bucket=bucket_name, Key=orig_key)

            subfolder_file_paths.append(dest_key)

    # reader the header line of each file in _subfolder_file_paths_ and determine the table schema
    for fp in subfolder_file_paths:
        response = s3_client.get_object(Bucket=bucket_name, Key=fp)
        first_line = response["Body"].readline().decode("utf-8").strip()
        header_fields = first_line.split(",")
        for field in header_fields:
            field = (
                field[1:-1] if field.startswith('"') and field.endswith('"') else field
            )
            if field in ["~id", "~from", "~to", "~label"]:
                continue
            if ":" not in field:
                table_columns[field] = "string"
                continue
            (field, datatype) = field.split(":")
            if datatype == "Vector":
                # skip vectors:
                continue
            if datatype == "Long":
                table_columns[field] = "bigint"
                continue
            table_columns[field] = datatype.lower()

    table_schema = ",\n    ".join(f"`{k}` {v}" for k, v in table_columns.items())
    if bucket_folder:
        s3_location = f"s3://{bucket_name}/{bucket_folder}/{prefix}"
    else:
        s3_location = f"s3://{bucket_name}/{prefix}"

    return f"""CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
    {table_schema}
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '{s3_location}'
TBLPROPERTIES ('classification' = 'csv', 'skip.header.line.count'='1');
"""


async def create_iceberg_table_from_table(
    s3_output_bucket: str,
    table_name: str,
    csv_table_name: str,
    catalog: str = None,  # type: ignore[assignment]
    database: str = None,  # type: ignore[assignment]
    table_columns: Optional[list[str]] = None,
    polling_interval=None,
    max_attempts=None,
) -> str:
    """Create an Iceberg table from an existing CSV table using Athena.

    Args:
        :param s3_output_bucket: S3 path for query results
        :param table_name: Name for the new Iceberg table
        :param csv_table_name: Name of the source CSV table
        :param catalog: (str, optional) catalog namespace to run the sql_query
        :param database: (str, optional) the database to run the sql_query
        :param table_columns: (list, optional) table columns to include
        :param polling_interval: Polling interval for status checks
        :param max_attempts: Maximum attempts for status checks

    Returns:
        str: Returns the query_execution_id if successful
    """
    select_columns = "*"
    if table_columns:
        select_columns = '"' + '","'.join(table_columns) + '"'

    sql_statement = f"""
CREATE TABLE {table_name}
  WITH (
      table_type = 'ICEBERG',
      is_external = false
      )
AS SELECT {select_columns} FROM {csv_table_name};
"""

    logger.info(f"SQL_CREATE_TABLE:\n{sql_statement}")

    athena_client = boto3.client(SERVICE_ATHENA)
    query_execution_id = _execute_athena_query(
        athena_client,
        sql_statement,
        s3_output_bucket,
        catalog=catalog,
        database=database,
    )

    # Wait for query to complete using TaskFuture
    future = TaskFuture(
        query_execution_id, TaskType.EXPORT_ATHENA_TABLE, polling_interval, max_attempts
    )
    await future.wait_until_complete(athena_client)

    # Check the final status
    final_status = future.current_status

    if final_status != "SUCCEEDED":
        raise Exception(
            f"Query {query_execution_id} failed with status: {final_status}"
        )

    logger.info(f"Successfully completed execution of query [{query_execution_id}]")
    return query_execution_id


async def create_table_schema_from_s3(
    s3_bucket: str,
    table_schema: str,
    catalog: str = None,  # type: ignore[assignment]
    database: str = None,  # type: ignore[assignment]
    polling_interval=None,
    max_attempts=None,
) -> str:
    """Create external table in Athena from S3 data.

    Args:
        :param table_schema: SQL CREATE EXTRNAL TABLE statement
        :param s3_bucket: S3 bucket path containing data
        :param catalog: (str, optional) catalog namespace to run the sql_query
        :param database: (str, optional) the database to run the sql_query
        :param polling_interval: Polling interval for status checks
        :param max_attempts: Maximum attempts for status checks

    Returns:
        str: Returns the query_execution_id if successful
    """
    # TODO check if s3_bucket is empty
    # TODO validate table_schema
    # TODO validate if table exists already
    # TODO check is skip drop table is False and table exists

    athena_client = boto3.client(SERVICE_ATHENA)
    query_execution_id = _execute_athena_query(
        athena_client, table_schema, s3_bucket, catalog=catalog, database=database
    )

    # Wait for query to complete using TaskFuture
    future = TaskFuture(
        query_execution_id, TaskType.EXPORT_ATHENA_TABLE, polling_interval, max_attempts
    )
    await future.wait_until_complete(athena_client)

    # Check the final status
    final_status = future.current_status

    if final_status != "SUCCEEDED":
        raise Exception(
            f"Query {query_execution_id} failed with status: {final_status}"
        )

    logger.info(f"Successfully completed execution of query [{query_execution_id}]")
    return query_execution_id


def _execute_athena_query(
    client,
    sql_statement: str,
    output_location: str,
    catalog: str = None,  # type: ignore[assignment]
    database: str = None,  # type: ignore[assignment]
) -> str:
    """
    :param client: boto3 Athena client to run a query
    :param sql_statement: SQL query to execute
    :param output_location: S3 bucket path for query results
    :param catalog: (str, optional) catalog namespace to run the sql_query
    :param database: (str, optional) the database to run the sql_query
    :return: string with the execution id
    """

    query_execution_params = {
        "QueryString": sql_statement,
        "ResultConfiguration": {"OutputLocation": output_location},
    }
    if catalog or database:
        query_execution_context = {}
        if catalog:
            query_execution_context["Catalog"] = catalog
        if database:
            query_execution_context["Database"] = database
        query_execution_params["QueryExecutionContext"] = query_execution_context

    logger.info(f"Creating table using statement:{sql_statement}")

    try:
        response = client.start_query_execution(**query_execution_params)
        logger.info(f"Executing query: {response['QueryExecutionId']}")
        query_execution_id = response["QueryExecutionId"]
    except ClientError as e:
        logger.error(f"Error creating table: {e}")
        raise

    return query_execution_id


def empty_s3_bucket(s3_bucket: str):
    # TODO Empty bucket and delete folder?
    pass


def validate_permissions():
    user_arn = boto3.client(SERVICE_STS).get_caller_identity()["Arn"]
    iam_client = IamClient(role_arn=user_arn, client=boto3.client(SERVICE_IAM))

    s3_import = os.getenv("NETWORKX_S3_IMPORT_BUCKET_PATH")
    s3_export = os.getenv("NETWORKX_S3_EXPORT_BUCKET_PATH")

    kms_key_import = _get_bucket_encryption_key_arn(s3_import) if s3_import else None
    kms_key_export = _get_bucket_encryption_key_arn(s3_export) if s3_export else None

    return iam_client.validate_permissions(
        s3_import, kms_key_import, s3_export, kms_key_export
    )


class ProjectionType(Enum):
    """Enum representing the type of graph projection for validating Athena SQL queries.

    Attributes:
        NODE: Projection type for node queries that require ~id field
        EDGE: Projection type for edge queries that require ~id, ~from, and ~to fields
    """

    NODE = "node"
    EDGE = "edge"


def validate_athena_query(query: str, projection_type: ProjectionType):
    """Validates that an Athena SQL SELECT query contains the required fields for node or edge projections.

    Args:
        query (str): The SQL SELECT query to validate
        projection_type (ProjectionType): The type of projection (NODE or EDGE) to validate against

    Returns:
        bool: True if query contains required fields for the projection type, False otherwise

    The function checks that:
    - Query is a SELECT query
    - For NODE projections: Query must return the "~id" field
    - For EDGE projections: Query must return the "~from" and "~to" fields
    - The "~label" return field is optional
    - Wildcard (*) selects are allowed but generate a warning
    - Invalid SQL syntax returns False
    """
    parsed_queries = parse_one(query).find(exp.Select)
    if parsed_queries is None:
        logger.error(f"SQL query not a SELECT query: {query}")
        return False
    try:
        column_names = {column.alias_or_name for column in parsed_queries}
    except Exception as e:
        logger.error(f"Invalid SQL query: {e}")
        return False

    if "*" in column_names:
        logger.warning(
            "Cannot validate required fields due to wildcard (*) in SELECT projection"
        )
        return True

    match projection_type:
        case ProjectionType.NODE:
            mandate_fields_node = {"~id"}
            if not mandate_fields_node.issubset(column_names):
                logger.warning(
                    f"Missing required fields for node projection. Required fields: {mandate_fields_node}"
                )
            return mandate_fields_node.issubset(column_names)
        case ProjectionType.EDGE:
            mandate_fields_edge = {"~from", "~to"}
            if not mandate_fields_edge.issubset(column_names):
                logger.warning(
                    f"Missing required fields for edge projection. Required fields: {mandate_fields_edge}"
                )
            return mandate_fields_edge.issubset(column_names)
        case _:
            logger.warning(f"Unknown projection type: {projection_type}")
            return False


def _graph_status_check(na_client, graph_id, expected_state):
    """Check if a Neptune Analytics graph is in the expected state.

    Args:
        na_client (boto3.client): The Neptune Analytics boto3 client
        graph_id (str): The ID of the graph to check
        expected_state (str): The expected state of the graph (e.g. 'AVAILABLE', 'STOPPED')

    Returns:
        asyncio.Future: A failed Future if the graph is not in the expected state,
                       None otherwise. The Future's exception will contain details
                       about the invalid state.
    """
    response_status = na_client.get_graph(graphIdentifier=graph_id)
    current_status = response_status.get("status")
    if current_status != expected_state:
        fut = TaskFuture("-1", TaskType.NOOP)
        fut.set_exception(
            Exception(f"Invalid graph ({graph_id}) instance state: {current_status}")
        )
        return asyncio.wrap_future(fut)
    return None


def _invalid_status_code(status_code, response):
    """Create a failed Future for an invalid API response status code.

    Args:
        status_code (int): The HTTP status code from the API response.
        response (dict): The full API response.

    Returns:
        asyncio.Future: A failed Future containing an exception with details about
                       the invalid status code and response
    """
    fut = TaskFuture("-1", TaskType.NOOP)
    fut.set_exception(
        Exception(
            f"Invalid response status code: {status_code} with full response:\n {response}"
        )
    )
    return asyncio.wrap_future(fut)


def _get_status_check_future(
    na_client, task_type: TaskType, object_id, polling_interval=None, max_attempts=None
):
    """Creates and returns a Future for monitoring Neptune Analytics task status.

    Args:
        na_client: The Neptune Analytics boto3 client
        task_type (TaskType): The type of task being monitored (e.g. CREATE, DELETE, etc)
        object_id (str): The identifier for the object being monitored (e.g. graph ID)
        polling_interval (int): Time interval in seconds for job status query
        max_attempts: Maximum attempts for status checks

    Returns:
        asyncio.Future: A Future that resolves when the task completes

    The returned Future will monitor the task status by polling at regular intervals
    for a maximum number of attmpts. The Future resolves when the task reaches
    its completion state as defined by the TaskType.
    """
    fut = TaskFuture(object_id, task_type, polling_interval, max_attempts)
    asyncio.create_task(fut.wait_until_complete(na_client), name=object_id)
    return asyncio.wrap_future(fut)


def _get_or_create_clients(
    sts_client: Optional[BaseClient] = None,
    iam_client: Optional[BaseClient] = None,
    na_client: Optional[BaseClient] = None,
):
    """
    Create or reuse provided AWS clients.

    Args:
        sts_client (Optional[BaseClient]): Optional STS boto3 client
        iam_client (Optional[BaseClient]): Optional IAM boto3 client
        na_client (Optional[BaseClient]): Optional Neptune Analytics boto3 client

    Returns:
        Tuple[BaseClient, IamClient, BaseClient]: Tuple containing (sts_client, iam_client, na_client)
    """
    if sts_client is None:
        sts_client = boto3.client(SERVICE_STS)
    user_arn = sts_client.get_caller_identity()["Arn"]

    # Create IAM client if not provided
    if iam_client is None:
        iam_client = IamClient(role_arn=user_arn, client=boto3.client(SERVICE_IAM))

    # Create Neptune Analytics client if not provided
    if na_client is None:
        na_client = boto3.client(
            service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
        )

    return iam_client, na_client
