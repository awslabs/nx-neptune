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
import os
import logging
import uuid

from asyncio import Future
from datetime import datetime
from enum import Enum
from typing import Optional

import boto3
import jmespath
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from .clients import SERVICE_IAM, SERVICE_NA, SERVICE_STS, IamClient
from .clients.neptune_constants import APP_ID_NX
from .na_graph import NeptuneGraph
import time

__all__ = [
    "import_csv_from_s3",
    "export_csv_to_s3",
    "TaskFuture",
    "TaskType",
    "create_na_instance",
    "delete_na_instance",
    "export_athena_table_to_s3",
    "create_table_from_s3",
    "validate_permissions",
    "start_na_instance",
    "stop_na_instance",
]

logger = logging.getLogger(__name__)

_PROJECT_IDENTIFIER = "nx-neptune"

_PERMISSIONS_CREATE = ["neptune-graph:CreateGraph", "neptune-graph:TagResource"]

class TaskType(Enum):
    # Allow import to run against an "INITIALIZING" state - the graph is sometimes in this state after creating graph
    IMPORT = (1, ["INI", "INITIALIZING", "IMPORTING"], "SUCCEEDED")
    # Allow export to run against an "INITIALIZING" state - the graph is sometimes in this state after running algorithms
    EXPORT = (2, ["INI", "INITIALIZING", "EXPORTING"], "SUCCEEDED")
    CREATE = (3, ["INI", "CREATING"], "AVAILABLE")
    DELETE = (4, ["INI", "DELETING"], "DELETED")
    NOOP = (5, ["INI"], "AVAILABLE")
    START = (6, ["INI", "STARTING"], "AVAILABLE")
    STOP = (7, ["INI", "STOPPING"], "STOPPED")


    def __init__(self, num_value, permitted_statuses, status_complete):
        self._value_ = num_value
        self.permitted_statuses = permitted_statuses
        self.status_complete = status_complete


class TaskFuture(Future):
    """A Future subclass that tracks Neptune Analytics task information.

    This class extends the standard Future to include task-specific identifiers
    for Neptune Analytics import and export operations.

    Args:
        task_id (str): The Neptune Analytics task identifier
        task_type (TaskType): The type of task ('import' or 'export')
        polling_interval(int): Time interval in seconds to perform job status query
    """

    def __init__(self, task_id, task_type, polling_interval=10, max_attempts=60):
        super().__init__()
        self.task_id = task_id
        self.task_type = task_type
        self.polling_interval = polling_interval
        # TODO: add max attempts


async def _wait_until_task_complete(client: BaseClient, future: TaskFuture):
    """Asynchronously monitor a Neptune Analytics task until completion.

    This function polls the status of an import or export task until it completes
    or fails, then resolves the provided Future accordingly.

    Args:
        client (boto3.client): The Neptune Analytics boto3 client
        future (TaskFuture): The Future object tracking the task

    Raises:
        ClientError: If there's an issue with the AWS API call
        ValueError: If an unknown task type is provided
    """
    task_id = future.task_id
    task_type = future.task_type
    task_polling_interval = future.polling_interval
    logger.debug(
        f"Perform Neptune Analytics job status check on Type: [{task_type}] with ID: [{task_id}]"
    )

    status_list = task_type.permitted_statuses
    status = "INI"

    while status in status_list:
        try:
            task_action_map = {
                TaskType.IMPORT: lambda: client.get_import_task(taskIdentifier=task_id),  # type: ignore[attr-defined]
                TaskType.EXPORT: lambda: client.get_export_task(taskIdentifier=task_id),  # type: ignore[attr-defined]
                TaskType.CREATE: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
                TaskType.DELETE: lambda: delete_status_check_wrapper(client, task_id),  # type: ignore[attr-defined],
                TaskType.START: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
                TaskType.STOP: lambda: client.get_graph(graphIdentifier=task_id),  # type: ignore[attr-defined]
            }

            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[{current_time}] Current status: {status}")

            response = task_action_map[task_type]()
            status = response.get("status")

            if status == task_type.status_complete:
                logger.info(f"Task [{task_id}] completed at [{current_time}]")
                future.set_result(task_id)
                return
            elif status in status_list:
                await asyncio.sleep(task_polling_interval)
            else:
                logger.error(f"Unexpected status: {status} on type: {task_type}")
        except ClientError as e:
            raise e


def import_csv_from_s3(
    na_graph: NeptuneGraph,
    s3_arn,
    reset_graph_ahead=True,
    skip_snapshot=True,
    polling_interval=30,
) -> Future:
    """Import CSV data from S3 into a Neptune Analytics graph.

    This function handles the complete workflow for importing graph data:
    1. Checks required permissions
    2. Optionally resets the graph
    3. Starts the import task
    4. Returns a Future that can be awaited for completion

    Args:
        na_graph (NeptuneGraph): The Neptune Analytics graph instance
        s3_arn (str): The S3 location containing CSV data (e.g., 's3://bucket-name/prefix/')
        reset_graph_ahead (bool, optional): Whether to reset the graph before import. Defaults to True.
        skip_snapshot (bool, optional): Whether to skip creating a snapshot when resetting. Defaults to True.
        polling_interval (int): Time interval in seconds for job status query

    Returns:
        asyncio.Future: A Future that resolves when the import completes

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
    iam_client.has_import_from_s3_permissions(s3_arn, key_arn)

    if reset_graph_ahead:
        # Run reset
        _reset_graph(na_client, graph_id, skip_snapshot)

    # Run Import
    task_id = _start_import_task(na_client, graph_id, s3_arn, role_arn)

    # Packaging future
    future = TaskFuture(task_id, TaskType.IMPORT, polling_interval)
    task = asyncio.create_task(
        _wait_until_task_complete(na_client, future), name=task_id
    )
    na_graph.current_jobs.add(task)
    task.add_done_callback(na_graph.current_jobs.discard)

    return asyncio.wrap_future(future)


def export_csv_to_s3(na_graph: NeptuneGraph, s3_arn: str, polling_interval=30) -> Future:
    """Export graph data from Neptune Analytics to S3 in CSV format.

    This function handles the complete workflow for exporting graph data:
    1. Checks required permissions
    2. Starts the export task
    3. Returns a Future that can be awaited for completion

    Args:
        na_graph (NeptuneGraph): The Neptune Analytics graph instance
        s3_arn (str): The S3 destination location (e.g., 's3://bucket-name/prefix/')
        polling_interval(int): Time interval in seconds to perform job status query

    Returns:
        asyncio.Future: A Future that resolves when the export completes

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

    # Run Import
    task_id = _start_export_task(na_client, graph_id, s3_arn, role_arn, key_arn)

    # Packaging future
    future = TaskFuture(task_id, TaskType.EXPORT, polling_interval)
    task = asyncio.create_task(
        _wait_until_task_complete(na_client, future), name=task_id
    )
    na_graph.current_jobs.add(task)
    task.add_done_callback(na_graph.current_jobs.discard)
    return asyncio.wrap_future(future)


def create_na_instance(config: Optional[dict] = None):
    """
    Creates a new graph instance for Neptune Analytics.

    Args:
        config (Optional[dict]): Optional dictionary of custom configuration parameters
            to use when creating the Neptune Analytics instance. If not provided,
            default settings will be applied.
            All options listed under boto3 documentations are supported.

            Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/neptune-graph/client/create_graph.html

    Raises:
        Exception: If the Neptune Analytics instance creation fails
    """
    na_client = boto3.client(
        service_name=SERVICE_NA, config=Config(user_agent_appid=APP_ID_NX)
    )

    # Permissions check
    user_arn = boto3.client(SERVICE_STS).get_caller_identity()["Arn"]
    iam_client = IamClient(role_arn=user_arn, client=boto3.client(SERVICE_IAM))
    iam_client.has_create_na_permissions()

    response = _create_na_instance_task(na_client, config)
    prospective_graph_id = _get_graph_id(response)

    if _get_status_code(response) == 201:
        fut = TaskFuture(prospective_graph_id, TaskType.CREATE, 30)
        asyncio.create_task(
            _wait_until_task_complete(na_client, fut), name=prospective_graph_id
        )
        return asyncio.wrap_future(fut)
    else:
        raise Exception(
            f"Neptune instance creation failure with graph name {prospective_graph_id}"
        )



def start_na_instance(graph_id: str):
    """
    Attempt to resume a remote Neptune Analytics instance with the provided graph_id,
    on behalf of the configured IAM user.

    Returns:
        asyncio.Future: A Future that resolves with the graph_id,
        represent the instance being deleted, or String literal Fail
        in the case of exception.

    Raises:
        Exception: If the Neptune Analytics instance creation fails
    """
    # Instance deletion
    na_client = boto3.client("neptune-graph")
    response = na_client.start_graph(graphIdentifier=graph_id)

    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.START, 10)
        asyncio.create_task(_wait_until_task_complete(na_client, fut), name=graph_id)
        return asyncio.wrap_future(fut)
    else:
        fut = TaskFuture("-1", TaskType.NOOP, 30)
        fut.set_exception(Exception(f"Invalid response status code: {status_code}"))
        return asyncio.wrap_future(fut)



def stop_na_instance(graph_id: str):
    """
    Attempt to stop a remote Neptune Analytics instance with the provided graph_id,
    on behalf of the configured IAM user.

    Returns:
        asyncio.Future: A Future that resolves with the graph_id,
        represent the instance being deleted, or String literal Fail
        in the case of exception.

    Raises:
        Exception: If the Neptune Analytics instance creation fails
    """
    # Instance deletion
    na_client = boto3.client("neptune-graph")
    response = na_client.stop_graph(graphIdentifier=graph_id)

    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.STOP, 10)
        asyncio.create_task(_wait_until_task_complete(na_client, fut), name=graph_id)
        return asyncio.wrap_future(fut)
    else:
        fut = TaskFuture("-1", TaskType.NOOP, 30)
        fut.set_exception(Exception(f"Invalid response status code: {status_code}"))
        return asyncio.wrap_future(fut)


def delete_na_instance(graph_id: str):
    """
    Attempt to delete a remote Neptune Analytics instance with the provided graph_id,
    on behalf of the configured IAM user.

    Returns:
        asyncio.Future: A Future that resolves with the graph_id,
        represent the instance being deleted, or String literal Fail
        in the case of exception.

    Raises:
        Exception: If the Neptune Analytics instance creation fails
    """
    # Permission check
    user_arn = boto3.client("sts").get_caller_identity()["Arn"]
    iam_client = IamClient(role_arn=user_arn, client=boto3.client(SERVICE_IAM))
    iam_client.has_delete_na_permissions()

    # Instance deletion
    na_client = boto3.client("neptune-graph")
    response = _delete_na_instance_task(na_client, graph_id)

    status_code = _get_status_code(response)
    if status_code == 200:
        fut = TaskFuture(graph_id, TaskType.DELETE, 30)
        asyncio.create_task(_wait_until_task_complete(na_client, fut), name=graph_id)
        return asyncio.wrap_future(fut)
    else:
        fut = TaskFuture("-1", TaskType.NOOP, 30)
        fut.set_exception(Exception(f"Invalid response status code: {status_code}"))
        return asyncio.wrap_future(fut)


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


def _create_na_instance_task(client, config: Optional[dict] = None):
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

    graph_name = _create_random_graph_name()
    kwargs = _get_create_instance_config(graph_name, config)
    response = client.create_graph(**kwargs)
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
    logger.debug(
        f"Import S3 graph data [{s3_location}] into Graph [{graph_id}], under IAM role [{role_arn}]"
    )
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
) -> str:
    """Export graph data to an S3 bucket in CSV format.

    Args:
        client: The Neptune Analytics boto3 client
        graph_id (str): The ID of the Neptune Analytics graph
        s3_destination (str): The S3 destination location (e.g., 's3://bucket-name/prefix/')
        role_arn (str): The IAM role ARN with permissions to write to the S3 bucket
        kms_key_identifier (str): KMS key ARN for encrypting the exported data
        filetype (str): CSV

    Returns:
        str or None: The export task ID if successful, None otherwise
    """
    logger.debug(
        f"Export S3 Graph [{graph_id}] data to S3 [{s3_destination}], under IAM role [{role_arn}]"
    )
    try:
        response = client.start_export_task(  # type: ignore[attr-defined]
            graphIdentifier=graph_id,
            roleArn=role_arn,
            format=filetype,
            destination=s3_destination,
            kmsKeyIdentifier=kms_key_identifier,
        )
        task_id = response.get("taskId")
        return task_id

    except ClientError as e:
        raise e


def _reset_graph(client: BaseClient, graph_id: str, skip_snapshot: bool = True, polling_interval=10, max_attempts=60) -> bool:
    """Reset the Neptune Analytics graph.

    Args:
        client: The Neptune Analytics boto3 client
        graph_id (str): The ID of the Neptune Analytics graph
        skip_snapshot:

    Returns:
        bool: True if reset was successful, False otherwise
    """
    try:
        logger.info(
            f"Perform reset_graph action on graph: [{graph_id}] with skip_snapshot: [{skip_snapshot}]"
        )
        client.reset_graph(graphIdentifier=graph_id, skipSnapshot=skip_snapshot)  # type: ignore[attr-defined]
        waiter = client.get_waiter("graph_available")
        waiter.wait(
            graphIdentifier=graph_id, WaiterConfig={"Delay": polling_interval, "MaxAttempts": max_attempts}
        )
        return True
    except ClientError as e:
        logger.error(f"Error resetting graph: {e}")
        return False


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
        s3_client = boto3.client("s3")

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


def _create_random_graph_name() -> str:
    """Generate a unique name for a Neptune Analytics graph instance.

    This function creates a random graph name by combining the project identifier
    with a UUID to ensure uniqueness across all graph instances.

    Returns:
        str: A unique graph name in the format '{PROJECT_IDENTIFIER}-{uuid}'
    """
    uuid_suffix = str(uuid.uuid4())
    return f"{_PROJECT_IDENTIFIER}-{uuid_suffix}"


def delete_status_check_wrapper(client, graph_id):
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

# TODO: provide an alternative to sql_queries - instead take a JSON import to map types
def export_athena_table_to_s3(sql_queries: list, s3_bucket: str, polling_interval=10, max_attempts=60):
    """Export Athena table data to S3 by executing SQL queries.
    
    Args:
        :param s3_bucket: S3 bucket path for query results
        :param sql_queries: List of SQL query strings to execute
        :param max_attempts:
        :param polling_interval:
    """
    client = boto3.client('athena')

    # TODO: validate permissions - or fail
    # TODO: check s3 bucket location is empty - or fail

    query_execution_ids = []
    for query in sql_queries:
        try:
            response = client.start_query_execution(
                QueryString=query,
                ResultConfiguration={'OutputLocation': s3_bucket}
            )
            logger.info(f"Started query execution: {response['QueryExecutionId']}")
            query_execution_ids.append(response['QueryExecutionId'])
        except ClientError as e:
            logger.error(f"Error executing query: {e}")
            return False
    
    # Wait on all query execution IDs
    s3_client = boto3.client('s3')
    bucket_name = s3_bucket.replace('s3://', '').split('/')[0]
    bucket_prefix = '/'.join(s3_bucket.replace('s3://', '').split('/')[1:])
    
    for query_execution_id in query_execution_ids:
        # TODO use TaskFuture instead
        for _ in range(1, max_attempts):
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status != 'SUCCEEDED':
                    logger.error(f"Query {query_execution_id} failed with status: {status}")
                    logger.error(f"Query error: {response['QueryExecution']['Status']['StateChangeReason']}")
                    return False
                
                # Sanity check that the CSV file exists
                csv_key = f"{bucket_prefix}{query_execution_id}.csv"
                try:
                    s3_client.head_object(Bucket=bucket_name, Key=csv_key)
                    logger.info(f"Confirmed CSV file exists: {csv_key}")
                except ClientError:
                    logger.error(f"CSV file not found: {csv_key}")
                    return False
                
                # Remove metadata file
                metadata_key = f"{bucket_prefix}{query_execution_id}.csv.metadata"
                try:
                    s3_client.delete_object(Bucket=bucket_name, Key=metadata_key)
                    logger.info(f"Deleted metadata file: {metadata_key}")
                except ClientError as e:
                    logger.warning(f"Could not delete metadata file {metadata_key}: {e}")

                # TODO: remove execution id from the list

                break
            time.sleep(polling_interval)
    logger.info(f"Successfully completed execution of {len(query_execution_ids)} queries")

    return True

def create_table_from_s3(s3_bucket: str, s3_output_bucket: str, table_name: str, table_columns=None, polling_interval=10, max_attempts=60):
    """Create external table in Athena from S3 data.

    Args:
        :param s3_bucket: S3 bucket path containing data
        :param s3_output_bucket: S3 bucket path to print results
        :param table_name:
        :param table_columns:
        :param max_attempts:
        :param polling_interval:
    """
    # TODO check if s3_bucket is empty
    # TODO validate table_schema
    # TODO validate if table exists already
    # TODO check is skip drop table is False and table exists

    # Wait on all query execution IDs
    s3_client = boto3.client('s3')
    bucket_name = s3_bucket.replace('s3://', '').split('/')[0]
    bucket_prefix = '/'.join(s3_bucket.replace('s3://', '').split('/')[1:])

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix)
    file_paths = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

    table_columns = {
        "~id": "string",
        "~from": "string",
        "~to": "string",
        "~label": "string",
    }
    for fp in file_paths:
        response = s3_client.get_object(Bucket=bucket_name, Key=fp)
        first_line = response['Body'].readline().decode('utf-8').strip()
        header_fields = first_line.split(',')
        for field in header_fields:
            field = field[1:-1] if field.startswith('"') and field.endswith('"') else field
            if field in ["~id", "~from", "~to", "~label"]:
                continue
            if ":" not in field:
                table_columns[field] = "string"
                continue
            (field, datatype) = field.split(':')
            if datatype in ["String", "Int"]:
                table_columns[field] = datatype.lower()
            elif datatype == "vector":
                # skip vectors:
                # table_columns[field] = "vector"
                pass
    table_schema = ',\n    '.join(f'`{k}` {v}' for k, v in table_columns.items())
#     sql_statement = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
#     {table_schema}
# ) STORED AS TEXTFILE
# LOCATION '{s3_bucket}'
# TBLPROPERTIES ('skip.header.line.count'='1')
# """
    sql_statement = f"""CREATE EXTERNAL TABLE IF NOT EXISTS {table_name} (
    {table_schema}
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '{s3_bucket}'
TBLPROPERTIES ('classification' = 'csv');
"""

    logger.info(f"SQL_CREATE_TABLE:\n{sql_statement}")

    athena_client = boto3.client('athena')
    try:
        response = athena_client.start_query_execution(
            QueryString=sql_statement,
            # for the query result:
            ResultConfiguration={'OutputLocation': s3_bucket},
        )
        logger.info(f"Creating table: {response['QueryExecutionId']}")
        query_execution_id = response['QueryExecutionId']
    except ClientError as e:
        logger.error(f"Error creating table: {e}")
        raise

    # TODO use TaskFuture instead
    for _ in range(1, max_attempts):
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status != 'SUCCEEDED':
                logger.error(f"Query {query_execution_id} failed with status: {status}")
                logger.error(f"Query error: {response['QueryExecution']['Status']['StateChangeReason']}")
                return False
            break
        time.sleep(polling_interval)

def create_table_schema_from_s3(s3_bucket: str, table_schema: str, polling_interval=10, max_attempts=60):
    """Create external table in Athena from S3 data.
    
    Args:
        :param table_schema: SQL CREATE EXTRNAL TABLE statement
        :param s3_bucket: S3 bucket path containing data
        :param max_attempts:
        :param polling_interval:
    """
    # TODO check if s3_bucket is empty
    # TODO validate table_schema
    # TODO validate if table exists already
    # TODO check is skip drop table is False and table exists

    client = boto3.client('athena')
    try:
        response = client.start_query_execution(
            QueryString=table_schema,
            # for the query result:
            ResultConfiguration={'OutputLocation': s3_bucket},
        )
        logger.info(f"Created table: {response['QueryExecutionId']}")
        query_execution_id = response['QueryExecutionId']
    except ClientError as e:
        logger.error(f"Error creating table: {e}")
        raise

    # TODO use TaskFuture instead
    for _ in range(1, max_attempts):
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status != 'SUCCEEDED':
                logger.error(f"Query {query_execution_id} failed with status: {status}")
                logger.error(f"Query error: {response['QueryExecution']['Status']['StateChangeReason']}")
                return False
            break
        time.sleep(polling_interval)
    logger.info(f"Successfully completed execution of query")

    return True

def empty_s3_bucket(s3_bucket: str):
    # TODO Empty bucket and delete folder?
    pass

def validate_permissions():
    user_arn = boto3.client("sts").get_caller_identity()["Arn"]
    iam_client = IamClient(role_arn=user_arn, client=boto3.client(SERVICE_IAM))

    s3_location_import = os.getenv('NETWORKX_S3_IMPORT_BUCKET_PATH')
    if s3_location_import is not None:
        kms_key_import = _get_bucket_encryption_key_arn(s3_location_import)
    else:
        kms_key_import = None

    s3_location_export = os.getenv('NETWORKX_S3_EXPORT_BUCKET_PATH')
    if s3_location_export is not None:
        kms_key_export = _get_bucket_encryption_key_arn(s3_location_export)
    else:
        kms_key_export = None
    return iam_client.validate_permissions(s3_location_import, kms_key_import,
                                           s3_location_export, kms_key_export)