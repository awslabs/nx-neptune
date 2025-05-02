import asyncio
import logging
from concurrent.futures import Future
from datetime import datetime
from enum import Enum

import boto3
import jmespath
from botocore.exceptions import ClientError

from nx_neptune.na_graph import NeptuneGraph

__all__ = ["import_csv_from_s3", "export_csv_to_s3", "TaskFuture", "TaskType"]

logger = logging.getLogger(__name__)


class TaskType(Enum):
    IMPORT = 1
    EXPORT = 2


class TaskFuture(Future):
    """A Future subclass that tracks Neptune Analytics task information.

    This class extends the standard Future to include task-specific identifiers
    for Neptune Analytics import and export operations.

    Args:
        task_id (str): The Neptune Analytics task identifier
        task_type (TaskType): The type of task ('import' or 'export')
        polling_interval(int): Time interval in seconds to perform job status query
    """

    def __init__(self, task_id, task_type, polling_interval):
        super().__init__()
        self.task_id = task_id
        self.task_type = task_type
        self.polling_interval = polling_interval


async def _wait_until_task_complete(client: boto3.client, future: TaskFuture):
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
    status_list = [
        "RUNNING",
        "PENDING",
        "INITIALIZING",
        "IMPORTING",
        "EXPORTING",
        "INI",
    ]
    status = "INI"

    while status in status_list:
        try:
            if task_type == TaskType.IMPORT:
                response = client.get_import_task(taskIdentifier=task_id)
            elif task_type == TaskType.EXPORT:
                response = client.get_export_task(taskIdentifier=task_id)

            status = response.get("status")
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[{current_time}] Current status: {status}")

            if status in status_list:
                await asyncio.sleep(task_polling_interval)
            elif status == "SUCCEEDED":
                logger.info(f"Task [{task_id}] completed at [{current_time}]")
                future.set_result(status)
                return

        except ClientError as e:
            raise e


def import_csv_from_s3(
    na_graph: NeptuneGraph,
    s3_arn,
    reset_graph_ahead=True,
    skip_snapshot=True,
    polling_interval=30,
):
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
    graph_id = na_graph.client.graph_id
    na_client = na_graph.client.client
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


def export_csv_to_s3(na_graph: NeptuneGraph, s3_arn, polling_interval=30):
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
    graph_id = na_graph.client.graph_id
    na_client = na_graph.client.client
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


def _start_import_task(
    client: boto3.client,
    graph_id: str,
    s3_location: str,
    role_arn: str,
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
        response = client.start_import_task(
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
    client: boto3.client,
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
        response = client.start_export_task(
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


def _reset_graph(
    client: boto3.client, graph_id: str, skip_snapshot: bool = True
) -> bool:
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
        client.reset_graph(graphIdentifier=graph_id, skipSnapshot=skip_snapshot)
        waiter = client.get_waiter("graph_available")
        waiter.wait(
            graphIdentifier=graph_id, WaiterConfig={"Delay": 10, "MaxAttempts": 60}
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
