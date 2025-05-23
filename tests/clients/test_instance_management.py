import json
import asyncio
from unittest.mock import patch, MagicMock

import pytest
from botocore.exceptions import ClientError

from nx_neptune.instance_management import (
    _clean_s3_path,
    _get_bucket_encryption_key_arn,
    TaskFuture,
    TaskType,
    _get_status_code,
    _get_graph_id,
    create_na_instance,
    _wait_until_task_complete,
    import_csv_from_s3,
    export_csv_to_s3,
    delete_na_instance,
)

NX_CREATE_SUCCESS_FIXTURE = """{
          "ResponseMetadata": {
            "HTTPHeaders": {
              "connection": "keep-alive",
              "content-length": "402",
              "content-type": "application/json",
              "date": "Wed, 07 May 2025 22:57:49 GMT",
              "x-amz-apigw-id": "test_api_id",
              "x-amzn-requestid": "test_api_id",
              "x-amzn-trace-id": "test_trace_id"
            },
            "HTTPStatusCode": 201,
            "RequestId": "test_request_id",
            "RetryAttempts": 0
          },
          "arn": "test_arn",
          "createTime": "test_date",
          "deletionProtection": false,
          "endpoint": "test_endpoint",
          "id": "test_graph_id",
          "kmsKeyIdentifier": "AWS_OWNED_KEY",
          "name": "test_graph_name",
          "provisionedMemory": 16,
          "publicConnectivity": true,
          "replicaCount": 0,
          "status": "CREATING"
        }"""

NX_IMPORT_FAIL_FIXTURE = """{
          "ResponseMetadata": {
            "HTTPHeaders": {
              "connection": "keep-alive",
              "content-length": "402",
              "content-type": "application/json",
              "date": "Wed, 07 May 2025 22:57:49 GMT",
              "x-amz-apigw-id": "test_api_id",
              "x-amzn-requestid": "test_api_id",
              "x-amzn-trace-id": "test_trace_id"
            },
            "HTTPStatusCode": 503,
            "RequestId": "test_request_id",
            "RetryAttempts": 0
          },
         "arn": "test_arn",
          "createTime": "test_date",
          "deletionProtection": false,
          "endpoint": "test_endpoint",
          "id": "test_graph_id",
          "kmsKeyIdentifier": "AWS_OWNED_KEY",
          "name": "test_graph_name",
          "provisionedMemory": 16,
          "publicConnectivity": true,
          "replicaCount": 0,
          "status": "CREATING"
        }"""

NX_STATUS_CHECK_SUCCESS_FIXTURE = """{
              "ResponseMetadata": {
                "HTTPHeaders": {
                  "connection": "keep-alive",
                  "content-length": "402",
                  "content-type": "application/json",
                  "date": "Wed, 07 May 2025 22:57:49 GMT",
                  "x-amz-apigw-id": "test_api_id",
                  "x-amzn-requestid": "test_api_id",
                  "x-amzn-trace-id": "test_trace_id"
                },
                "HTTPStatusCode": 201,
                "RequestId": "test_request_id",
                "RetryAttempts": 0
              },
              "arn": "test_arn",
              "createTime": "test_date",
              "deletionProtection": false,
              "endpoint": "test_endpoint",
              "id": "test_graph_id",
              "kmsKeyIdentifier": "AWS_OWNED_KEY",
              "name": "test_graph_name",
              "provisionedMemory": 16,
              "publicConnectivity": true,
              "replicaCount": 0,
              "status": "AVAILABLE"
            }
            """

NX_STATUS_CHECK_IMPORT_EXPORT_SUCCESS_FIXTURE = """{
              "ResponseMetadata": {
                "HTTPHeaders": {
                  "connection": "keep-alive",
                  "content-length": "402",
                  "content-type": "application/json",
                  "date": "Wed, 07 May 2025 22:57:49 GMT",
                  "x-amz-apigw-id": "test_api_id",
                  "x-amzn-requestid": "test_api_id",
                  "x-amzn-trace-id": "test_trace_id"
                },
                "HTTPStatusCode": 201,
                "RequestId": "test_request_id",
                "RetryAttempts": 0
              },
              "arn": "test_arn",
              "createTime": "test_date",
              "deletionProtection": false,
              "endpoint": "test_endpoint",
              "id": "test_graph_id",
              "kmsKeyIdentifier": "AWS_OWNED_KEY",
              "name": "test_graph_name",
              "provisionedMemory": 16,
              "publicConnectivity": true,
              "replicaCount": 0,
              "status": "SUCCEEDED"
            }
            """

NX_IMPORT_TASK_SUCCESS_FIXTURE = """{
              "ResponseMetadata": {
                "HTTPHeaders": {
                  "connection": "keep-alive",
                  "content-length": "402",
                  "content-type": "application/json",
                  "date": "Wed, 07 May 2025 22:57:49 GMT",
                  "x-amz-apigw-id": "test_api_id",
                  "x-amzn-requestid": "test_api_id",
                  "x-amzn-trace-id": "test_trace_id"
                },
                "HTTPStatusCode": 201,
                "RequestId": "test_request_id",
                "RetryAttempts": 0
              },
              "taskId": "test-import-task-id",
              "status": "IMPORTING"
            }
            """

VALID_KMS_ARN = (
    "arn:aws:kms:us-west-2:123456789012:key/abcd1234-a123-456a-a12b-a123b4cd56ef"
)

VALID_ASSUME_ROLE_RESPONSE = {
    "Role": {
        "AssumeRolePolicyDocument": {
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "sts:AssumeRole",
                    "Principal": {"Service": "neptune-graph.amazonaws.com"},
                }
            ]
        }
    }
}


NX_DELETE_SUCCESS_FIXTURE = """{
          "ResponseMetadata": {
            "HTTPHeaders": {
              "connection": "keep-alive",
              "content-length": "402",
              "content-type": "application/json",
              "date": "Wed, 07 May 2025 22:57:49 GMT",
              "x-amz-apigw-id": "test_api_id",
              "x-amzn-requestid": "test_api_id",
              "x-amzn-trace-id": "test_trace_id"
            },
            "HTTPStatusCode": 200,
            "RequestId": "test_request_id",
            "RetryAttempts": 0
          },
          "arn": "test_arn",
          "createTime": "test_date",
          "deletionProtection": false,
          "endpoint": "test_endpoint",
          "id": "test_graph_id",
          "kmsKeyIdentifier": "AWS_OWNED_KEY",
          "name": "test_graph_name",
          "provisionedMemory": 16,
          "publicConnectivity": true,
          "replicaCount": 0,
          "status": "DELETING"
        }"""

NX_DELETE_FAILURE_FIXTURE = """{
          "ResponseMetadata": {
            "HTTPHeaders": {
              "connection": "keep-alive",
              "content-length": "402",
              "content-type": "application/json",
              "date": "Wed, 07 May 2025 22:57:49 GMT",
              "x-amz-apigw-id": "test_api_id",
              "x-amzn-requestid": "test_api_id",
              "x-amzn-trace-id": "test_trace_id"
            },
            "HTTPStatusCode": 503,
            "RequestId": "test_request_id",
            "RetryAttempts": 0
          },
          "arn": "test_arn",
          "createTime": "test_date",
          "deletionProtection": false,
          "endpoint": "test_endpoint",
          "id": "test_graph_id",
          "kmsKeyIdentifier": "AWS_OWNED_KEY",
          "name": "test_graph_name",
          "provisionedMemory": 16,
          "publicConnectivity": true,
          "replicaCount": 0,
          "status": "DELETING"
        }"""

NX_DELETE_STATUS_DELETED = {
    "Error": {
        "Code": "ResourceNotFoundException",
        "Message": "The specified resource was not found",
        "Type": "Sender",
    }
}


@pytest.mark.parametrize(
    "s3_path,expected_result",
    [
        # Test with s3:// prefix and folder path
        ("s3://my-bucket/folder/subfolder/", "my-bucket/folder"),
        # Test without s3:// prefix but with folder path
        ("my-bucket/folder/subfolder/", "my-bucket/folder"),
        # Test with only bucket name and s3:// prefix
        ("s3://my-bucket", "my-bucket"),
        # Test with only bucket name and no s3:// prefix
        ("my-bucket", "my-bucket"),
        # Test with trailing slash
        ("s3://my-bucket/", "my-bucket"),
        # Test with multiple nested folders
        (
            "s3://my-bucket/folder1/folder2/folder3/file.csv",
            "my-bucket/folder1/folder2/folder3",
        ),
        # Test with special characters in bucket name
        ("s3://my-bucket-name-with-hyphens/folder/", "my-bucket-name-with-hyphens"),
        # Test with empty string
        ("", ""),
    ],
)
def test_clean_s3_path(s3_path, expected_result):
    """Test the _clean_s3_path function with various input scenarios."""
    result = _clean_s3_path(s3_path)
    assert result == expected_result


@pytest.mark.parametrize(
    "mock_response,expected_result",
    [
        # Test with KMS encryption
        (
            {
                "ServerSideEncryptionConfiguration": {
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "aws:kms",
                                "KMSMasterKeyID": "arn:aws:kms:us-west-2:123456789012:key/abcd1234-a123-456a-a12b-a123b4cd56ef",
                            }
                        }
                    ]
                }
            },
            "arn:aws:kms:us-west-2:123456789012:key/abcd1234-a123-456a-a12b-a123b4cd56ef",
        ),
        # Test with non-KMS encryption (AES256)
        (
            {
                "ServerSideEncryptionConfiguration": {
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {
                                "SSEAlgorithm": "AES256"
                            }
                        }
                    ]
                }
            },
            None,
        ),
    ],
)
@patch("boto3.client")
def test_get_bucket_encryption_key_arn(
    mock_boto3_client, mock_response, expected_result
):
    """Test the _get_bucket_encryption_key_arn function with various scenarios."""
    # Mock the S3 client
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client

    # Configure the mock response
    mock_s3_client.get_bucket_encryption.return_value = mock_response

    # Call the function with a test S3 path
    result = _get_bucket_encryption_key_arn("s3://my-test-bucket/folder/")

    # Verify the result
    assert result == expected_result

    # Verify the S3 client was called correctly
    mock_boto3_client.assert_called_once_with("s3")
    mock_s3_client.get_bucket_encryption.assert_called_once_with(
        Bucket="my-test-bucket"
    )


@patch("boto3.client")
def test_get_bucket_encryption_key_arn_with_exception(mock_boto3_client):
    """Test handling of exceptions when retrieving bucket encryption."""
    # Mock the S3 client to raise an exception
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    mock_s3_client.get_bucket_encryption.side_effect = Exception(
        "Bucket encryption not configured"
    )

    # Call the function with a test S3 path
    result = _get_bucket_encryption_key_arn("s3://my-test-bucket/folder/")

    # Verify the result is None when an exception occurs
    assert result is None

    # Verify the S3 client was called correctly
    mock_boto3_client.assert_called_once_with("s3")
    mock_s3_client.get_bucket_encryption.assert_called_once_with(
        Bucket="my-test-bucket"
    )


@pytest.mark.parametrize(
    "json_str,expected_status_code",
    [
        ("{ }", None),
        (
            """{
              "ResponseMetadata": {
                "HTTPHeaders": {
                  "connection": "keep-alive",
                  "content-length": "402",
                  "content-type": "application/json",
                  "date": "Wed, 07 May 2025 22:57:49 GMT",
                  "x-amz-apigw-id": "test_api_id",
                  "x-amzn-requestid": "test_api_id",
                  "x-amzn-trace-id": "test_trace_id"
                },
                "HTTPStatusCode": 201,
                "RequestId": "test_request_id",
                "RetryAttempts": 0
              }
            }""",
            201,
        ),
    ],
)
def test_get_status_code(json_str, expected_status_code):
    test_response = json.loads(json_str)
    assert expected_status_code == _get_status_code(test_response)


@pytest.mark.parametrize(
    "json_str,expected_status_code",
    [
        ("{ }", None),
        (
            """{
              "ResponseMetadata": {
                "HTTPHeaders": {
                  "connection": "keep-alive",
                  "content-length": "402",
                  "content-type": "application/json",
                  "date": "Wed, 07 May 2025 22:57:49 GMT",
                  "x-amz-apigw-id": "test_api_id",
                  "x-amzn-requestid": "test_api_id",
                  "x-amzn-trace-id": "test_trace_id"
                },
                "HTTPStatusCode": 201,
                "RequestId": "test_request_id",
                "RetryAttempts": 0
              },
              "arn": "test_arn",
              "createTime": "test_date",
              "deletionProtection": false,
              "endpoint": "test_endpoint",
              "id": "test_graph_id",
              "kmsKeyIdentifier": "AWS_OWNED_KEY",
              "name": "test_graph_name",
              "provisionedMemory": 16,
              "publicConnectivity": true,
              "replicaCount": 0,
              "status": "CREATING"
            }""",
            "test_graph_id",
        ),
    ],
)
def test_get_graph_id(json_str, expected_status_code):
    test_response = json.loads(json_str)
    assert expected_status_code == _get_graph_id(test_response)


@pytest.mark.asyncio
@patch("boto3.client")
async def test_create_na_instance_graph_absent_create_fail(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    # Mock creation
    test_response = json.loads(NX_IMPORT_FAIL_FIXTURE)
    mock_nx_client.create_graph.return_value = test_response

    with pytest.raises(Exception, match="Neptune instance creation failure"):
        # Make sure graph_id is absent.
        result = create_na_instance()
        await result


@pytest.mark.asyncio
@patch("boto3.client")
async def test_create_na_instance_graph_absent_status_check_success(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    # Mock creation
    test_response = json.loads(NX_CREATE_SUCCESS_FIXTURE)
    mock_nx_client.create_graph.return_value = test_response

    # Mock status check
    test_status_response = json.loads(NX_STATUS_CHECK_SUCCESS_FIXTURE)
    mock_nx_client.get_graph.return_value = test_status_response

    # Make sure graph_id is absent.
    result = create_na_instance()
    await result
    assert result.result() == "test_graph_id"
    assert result.done()


@pytest.mark.asyncio
@patch("boto3.client")
async def test_create_na_instance_graph_absent_status_check_failure(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    # Mock creation response
    test_response = json.loads(NX_CREATE_SUCCESS_FIXTURE)
    mock_nx_client.create_graph.return_value = test_response

    # Mock status check
    mock_nx_client.create_graph.side_effect = ClientError(
        {"Error": {"Code": "InvalidGraphId", "Message": "Graph ID not found"}},
        "Graph status",
    )

    with pytest.raises(ClientError, match="InvalidGraphId"):
        result = create_na_instance()
        await result


@pytest.mark.asyncio
@patch("boto3.client")
async def test_create_na_instance_insufficient_permissions(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    # Mock setup
    mock_nx_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalActionName": "neptune-graph:CreateGraph", "EvalDecision": "allowed"},
            {"EvalActionName": "neptune-graph:TagResource", "EvalDecision": "deny"},
        ]
    }

    with pytest.raises(
        Exception, match="Insufficient permission, neptune-graph:TagResource"
    ):
        result = create_na_instance()
        await result


@pytest.mark.asyncio
@patch("boto3.client")
async def test_status_check_create(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    future = TaskFuture("test-create-id", TaskType.CREATE, 10)

    # Mock status check
    test_status_response = json.loads(NX_STATUS_CHECK_SUCCESS_FIXTURE)
    mock_nx_client.get_graph.return_value = test_status_response

    await _wait_until_task_complete(mock_nx_client, future)
    assert future.done()
    assert future.result() == "test-create-id"


@pytest.mark.asyncio
@patch("boto3.client")
async def test_status_check_import(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    future = TaskFuture("test-import-job-id", TaskType.IMPORT, 10)

    # Mock status check
    test_status_response = json.loads(NX_STATUS_CHECK_IMPORT_EXPORT_SUCCESS_FIXTURE)
    mock_nx_client.get_import_task.return_value = test_status_response

    await _wait_until_task_complete(mock_nx_client, future)
    assert future.done()
    assert future.result() == "test-import-job-id"


@pytest.mark.asyncio
@patch("boto3.client")
async def test_status_check_export(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    future = TaskFuture("test-export-job-id", TaskType.EXPORT, 10)

    # Mock status check
    test_status_response = json.loads(NX_STATUS_CHECK_IMPORT_EXPORT_SUCCESS_FIXTURE)
    mock_nx_client.get_export_task.return_value = test_status_response

    await _wait_until_task_complete(mock_nx_client, future)
    assert future.done()
    assert future.result() == "test-export-job-id"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_import_task")
@patch("nx_neptune.instance_management._reset_graph")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("asyncio.create_task")
async def test_import_csv_from_s3_success(
    mock_create_task,
    mock_get_bucket_encryption_key_arn,
    mock_reset_graph,
    mock_start_import_task,
):
    # Setup mocks
    mock_get_bucket_encryption_key_arn.return_value = None
    mock_reset_graph.return_value = True
    mock_start_import_task.return_value = "test-import-task-id"

    # Create mock NeptuneGraph
    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "test-graph-id"
    mock_na_graph.na_client.client = MagicMock()
    mock_na_graph.iam_client = MagicMock()
    mock_na_graph.iam_client.role_arn = "test-role-arn"
    mock_na_graph.current_jobs = set()

    # Call the function
    future = import_csv_from_s3(
        mock_na_graph,
        "s3://test-bucket/test-folder/",
        reset_graph_ahead=True,
        skip_snapshot=True,
        polling_interval=10,
    )

    # Verify the function behavior
    mock_get_bucket_encryption_key_arn.assert_called_once_with(
        "s3://test-bucket/test-folder/"
    )
    mock_na_graph.iam_client.has_import_from_s3_permissions.assert_called_once_with(
        "s3://test-bucket/test-folder/", None
    )
    mock_reset_graph.assert_called_once_with(
        mock_na_graph.na_client.client, "test-graph-id", True
    )
    mock_start_import_task.assert_called_once_with(
        mock_na_graph.na_client.client,
        "test-graph-id",
        "s3://test-bucket/test-folder/",
        "test-role-arn",
    )
    mock_create_task.assert_called_once()

    # Verify the future is properly set up
    assert isinstance(future, asyncio.Future)
    assert not future.done()


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_import_task")
@patch("nx_neptune.instance_management._reset_graph")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("asyncio.create_task")
async def test_import_csv_from_s3_without_reset(
    mock_create_task,
    mock_get_bucket_encryption_key_arn,
    mock_reset_graph,
    mock_start_import_task,
):
    # Setup mocks
    mock_get_bucket_encryption_key_arn.return_value = "test-kms-key-arn"
    mock_start_import_task.return_value = "test-import-task-id"

    # Create mock NeptuneGraph
    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "test-graph-id"
    mock_na_graph.na_client.client = MagicMock()
    mock_na_graph.iam_client = MagicMock()
    mock_na_graph.iam_client.role_arn = "test-role-arn"
    mock_na_graph.current_jobs = set()

    # Call the function with reset_graph_ahead=False
    future = import_csv_from_s3(
        mock_na_graph,
        "s3://test-bucket/test-folder/",
        reset_graph_ahead=False,
        skip_snapshot=True,
        polling_interval=10,
    )

    # Verify the function behavior
    mock_get_bucket_encryption_key_arn.assert_called_once_with(
        "s3://test-bucket/test-folder/"
    )
    mock_na_graph.iam_client.has_import_from_s3_permissions.assert_called_once_with(
        "s3://test-bucket/test-folder/", "test-kms-key-arn"
    )
    mock_reset_graph.assert_not_called()
    mock_start_import_task.assert_called_once_with(
        mock_na_graph.na_client.client,
        "test-graph-id",
        "s3://test-bucket/test-folder/",
        "test-role-arn",
    )
    mock_create_task.assert_called_once()

    # Verify the future is properly set up
    assert isinstance(future, asyncio.Future)
    assert not future.done()


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_import_csv_from_s3_permission_error(mock_get_bucket_encryption_key_arn):
    # Setup mocks
    mock_get_bucket_encryption_key_arn.return_value = None

    # Create mock NeptuneGraph
    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "test-graph-id"
    mock_na_graph.iam_client = MagicMock()
    mock_na_graph.iam_client.role_arn = "test-role-arn"

    # Configure permission check to fail
    mock_na_graph.iam_client.has_import_from_s3_permissions.side_effect = ValueError(
        "Insufficient permissions"
    )

    # Call the function and expect ValueError
    with pytest.raises(ValueError, match="Insufficient permissions"):
        import_csv_from_s3(
            mock_na_graph, "s3://test-bucket/test-folder/", reset_graph_ahead=True
        )


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_export_task")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("asyncio.create_task")
async def test_export_csv_to_s3_success(
    mock_create_task,
    mock_get_bucket_encryption_key_arn,
    mock_start_export_task,
):
    # Setup mocks
    mock_get_bucket_encryption_key_arn.return_value = None
    mock_start_export_task.return_value = "test-export-task-id"

    # Create mock NeptuneGraph
    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "test-graph-id"
    mock_na_graph.na_client.client = MagicMock()
    mock_na_graph.iam_client = MagicMock()
    mock_na_graph.iam_client.role_arn = "test-role-arn"
    mock_na_graph.current_jobs = set()

    # Call the function
    future = export_csv_to_s3(
        mock_na_graph,
        "s3://test-bucket/test-folder/",
        polling_interval=10,
    )

    # Verify the function behavior
    mock_get_bucket_encryption_key_arn.assert_called_once_with(
        "s3://test-bucket/test-folder/"
    )
    mock_na_graph.iam_client.has_export_to_s3_permissions.assert_called_once_with(
        "s3://test-bucket/test-folder/", None
    )
    mock_start_export_task.assert_called_once_with(
        mock_na_graph.na_client.client,
        "test-graph-id",
        "s3://test-bucket/test-folder/",
        "test-role-arn",
        None,
    )
    mock_create_task.assert_called_once()

    # Verify the future is properly set up
    assert isinstance(future, asyncio.Future)
    assert not future.done()


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_export_task")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("asyncio.create_task")
async def test_export_csv_to_s3_with_kms_key(
    mock_create_task,
    mock_get_bucket_encryption_key_arn,
    mock_start_export_task,
):
    # Setup mocks
    mock_get_bucket_encryption_key_arn.return_value = "test-kms-key-arn"
    mock_start_export_task.return_value = "test-export-task-id"

    # Create mock NeptuneGraph
    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "test-graph-id"
    mock_na_graph.na_client.client = MagicMock()
    mock_na_graph.iam_client = MagicMock()
    mock_na_graph.iam_client.role_arn = "test-role-arn"
    mock_na_graph.current_jobs = set()

    # Call the function
    future = export_csv_to_s3(
        mock_na_graph,
        "s3://test-bucket/test-folder/",
        polling_interval=10,
    )

    # Verify the function behavior
    mock_get_bucket_encryption_key_arn.assert_called_once_with(
        "s3://test-bucket/test-folder/"
    )
    mock_na_graph.iam_client.has_export_to_s3_permissions.assert_called_once_with(
        "s3://test-bucket/test-folder/", "test-kms-key-arn"
    )
    mock_start_export_task.assert_called_once_with(
        mock_na_graph.na_client.client,
        "test-graph-id",
        "s3://test-bucket/test-folder/",
        "test-role-arn",
        "test-kms-key-arn",
    )
    mock_create_task.assert_called_once()

    # Verify the future is properly set up
    assert isinstance(future, asyncio.Future)
    assert not future.done()


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_export_csv_to_s3_permission_error(mock_get_bucket_encryption_key_arn):
    # Setup mocks
    mock_get_bucket_encryption_key_arn.return_value = None

    # Create mock NeptuneGraph
    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "test-graph-id"
    mock_na_graph.iam_client = MagicMock()
    mock_na_graph.iam_client.role_arn = "test-role-arn"

    # Configure permission check to fail
    mock_na_graph.iam_client.has_export_to_s3_permissions.side_effect = ValueError(
        "Insufficient permissions"
    )

    # Call the function and expect ValueError
    with pytest.raises(ValueError, match="Insufficient permissions"):
        export_csv_to_s3(mock_na_graph, "s3://test-bucket/test-folder/")


@pytest.mark.asyncio
@patch("boto3.client")
async def test_delete_na_instance_success(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    test_status_response = json.loads(NX_DELETE_SUCCESS_FIXTURE)
    mock_nx_client.delete_graph.return_value = test_status_response

    # Configure the get_graph method to raise ResourceNotFoundException
    mock_nx_client.get_graph.side_effect = ClientError(
        error_response=NX_DELETE_STATUS_DELETED, operation_name="GetGraph"
    )

    result = await delete_na_instance("test-123")
    assert result == "test-123"


@pytest.mark.asyncio
@patch("boto3.client")
async def test_delete_na_instance_insufficient_permissions(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    #
    mock_nx_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalActionName": "neptune-graph:DeleteGraph", "EvalDecision": "deny"}
        ]
    }

    with pytest.raises(
        Exception, match="Insufficient permission, neptune-graph:DeleteGraph"
    ):
        result = delete_na_instance("")
        await result


@pytest.mark.asyncio
@patch("boto3.client")
async def test_delete_na_instance_failure(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    test_status_response = json.loads(NX_DELETE_FAILURE_FIXTURE)
    mock_nx_client.delete_graph.return_value = test_status_response

    with pytest.raises(Exception, match="Invalid response status code"):
        await delete_na_instance("test-123")
