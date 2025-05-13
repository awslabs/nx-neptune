import json
from concurrent.futures import Future
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
    "task_id,task_type,polling_interval",
    [
        ("task-123", TaskType.IMPORT, 60),
        ("task-456", TaskType.EXPORT, 60),
    ],
)
def test_task_future_initialization(task_id, task_type, polling_interval):
    """Test that TaskFuture correctly initializes with task_id and task_type."""
    future = TaskFuture(task_id, task_type, polling_interval)

    # Verify the attributes are set correctly
    assert future.task_id == task_id
    assert future.task_type == task_type

    # Verify it's a proper Future subclass
    assert isinstance(future, Future)
    assert not future.done()  # Should start in not-done state


def test_task_future_completion():
    """Test that TaskFuture can be completed and results retrieved."""
    future = TaskFuture("task-123", TaskType.IMPORT, 60)

    # Set a result and verify it can be retrieved
    test_result = "Operation completed successfully"
    future.set_result(test_result)

    assert future.done()
    assert future.result() == test_result


def test_task_future_exception():
    """Test that TaskFuture can handle exceptions."""
    future = TaskFuture("task-123", TaskType.IMPORT, 60)

    # Set an exception and verify it's properly handled
    test_exception = ValueError("Operation failed")
    future.set_exception(test_exception)

    assert future.done()
    with pytest.raises(ValueError) as excinfo:
        future.result()

    assert str(excinfo.value) == "Operation failed"


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
async def test_create_na_instance_graph_exist():
    result = create_na_instance("test_graph_id", False)
    await result
    assert result.result() == "test_graph_id"
    assert result.done()


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
        result = create_na_instance(None, True)
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
    result = create_na_instance("", True)
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
        result = create_na_instance(None, True)
        await result


@pytest.mark.asyncio
@patch("boto3.client")
async def test_create_na_instance_default_option_create_instance(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    with pytest.raises(Exception, match="Instance provisioning was requested"):
        result = create_na_instance("", False)
        await result


@pytest.mark.asyncio
@patch("boto3.client")
async def test_create_na_instance_insufficient_permissions(mock_boto3_client):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    #
    mock_nx_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalActionName": "neptune-graph:CreateGraph", "EvalDecision": "allowed"},
            {"EvalActionName": "neptune-graph:TagResource", "EvalDecision": "deny"},
        ]
    }

    with pytest.raises(
        Exception, match="Insufficient permission, neptune-graph:TagResource"
    ):
        result = create_na_instance("", True)
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
