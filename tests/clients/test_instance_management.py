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
import json
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
import os
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
    import_csv_from_s3,
    export_csv_to_s3,
    delete_na_instance,
    _get_create_instance_config,
    validate_athena_query,
    ProjectionType,
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
    "query,projection_type,expected_result",
    [
        ("some_invalid_SQL_query", ProjectionType.NODE, False),
        # Python library couldn't infer the runtime DB schema, will print a warning and pass instead.
        ("select * from test_table", ProjectionType.NODE, True),
        # Simple query which satisfied all conditions for Node
        ("select '~id' from test_table", ProjectionType.NODE, True),
        # Simple query which satisfied all conditions for Edge
        ("select '~id', '~from', '~to' from test_table", ProjectionType.EDGE, True),
        # Projection with alias (Node)
        ("select col_a as '~id' from test_table", ProjectionType.NODE, True),
        # Projection with alias (Edge)
        (
            "select col_a as '~id', col_b as '~from', col_c as '~to' from test_table",
            ProjectionType.EDGE,
            True,
        ),
        # Alias with sub-queries (Node)
        (
            """ 
            SELECT DISTINCT "~id", airport_name, 'airline' AS "~label" FROM (
                SELECT source_airport_id as "~id", source_airport as "airport_name"
                FROM air_routes_db.air_routes_table
                WHERE source_airport_id IS NOT NULL
                UNION ALL
                SELECT dest_airport_id as "~id", dest_airport as "airport_name"
                FROM air_routes_db.air_routes_table
                WHERE dest_airport_id IS NOT NULL );
        """,
            ProjectionType.NODE,
            True,
        ),
    ],
)
def test_validate_athena_query(query, projection_type, expected_result):
    """Test the validate_athena_query function with various SQL query scenarios.

    Args:
        query (str): The SQL query to validate
        projection_type (ProjectionType): The type of projection (NODE or EDGE)
        expected_result (bool): The expected validation result

    Tests validation of:
    - Invalid SQL queries
    - Simple SELECT queries
    - Queries with required node/edge columns
    - Queries with column aliases
    - Complex queries with subqueries
    """
    result = validate_athena_query(query, projection_type)
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
        await create_na_instance()


@pytest.mark.asyncio
@patch("nx_neptune.utils.task_future.asyncio.sleep", new_callable=AsyncMock)
@patch("boto3.client")
async def test_create_na_instance_graph_absent_status_check_success(
    mock_boto3_client, mock_sleep
):

    # Mock boto client
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client

    # Mock creation
    test_response = json.loads(NX_CREATE_SUCCESS_FIXTURE)
    mock_nx_client.create_graph.return_value = test_response

    # Mock status check - return CREATING then AVAILABLE
    test_status_creating = json.loads(NX_STATUS_CHECK_SUCCESS_FIXTURE)
    test_status_creating["status"] = "CREATING"
    test_status_available = json.loads(NX_STATUS_CHECK_SUCCESS_FIXTURE)
    mock_nx_client.get_graph.side_effect = [test_status_creating, test_status_available]

    # Make sure graph_id is absent.
    graph_id = await create_na_instance()
    assert graph_id == "test_graph_id"


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
        await create_na_instance()


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
        await create_na_instance()


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

    await future.wait_until_complete(mock_nx_client)
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

    await future.wait_until_complete(mock_nx_client)
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

    await future.wait_until_complete(mock_nx_client)
    assert future.done()
    assert future.result() == "test-export-job-id"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_import_task")
@patch("nx_neptune.instance_management.reset_graph")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_import_csv_from_s3_success(
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
    test_status_response = json.loads(NX_STATUS_CHECK_IMPORT_EXPORT_SUCCESS_FIXTURE)
    mock_na_graph.na_client.client.get_import_task.return_value = test_status_response

    # Call the function
    task_id = await import_csv_from_s3(
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
        "test-graph-id", mock_na_graph.na_client.client, True
    )
    mock_start_import_task.assert_called_once_with(
        mock_na_graph.na_client.client,
        "test-graph-id",
        "s3://test-bucket/test-folder/",
        "test-role-arn",
    )

    assert task_id == "test-import-task-id"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_import_task")
@patch("nx_neptune.instance_management.reset_graph")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_import_csv_from_s3_without_reset(
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
    test_status_response = json.loads(NX_STATUS_CHECK_IMPORT_EXPORT_SUCCESS_FIXTURE)
    mock_na_graph.na_client.client.get_import_task.return_value = test_status_response

    # Call the function with reset_graph_ahead=False
    task_id = await import_csv_from_s3(
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

    # verify return
    assert task_id == "test-import-task-id"


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
        await import_csv_from_s3(
            mock_na_graph, "s3://test-bucket/test-folder/", reset_graph_ahead=True
        )


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_export_task")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_export_csv_to_s3_success(
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
    task_id = await export_csv_to_s3(
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
        export_filter=None,
    )

    assert task_id == "test-export-task-id"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._start_export_task")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_export_csv_to_s3_with_kms_key(
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
    task_id = await export_csv_to_s3(
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
        export_filter=None,
    )

    assert task_id == "test-export-task-id"


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
        await export_csv_to_s3(mock_na_graph, "s3://test-bucket/test-folder/")


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
        await delete_na_instance("")


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


@pytest.mark.asyncio
async def test_create_graph_config_base():
    result = _get_create_instance_config("test")
    expected = {
        "graphName": "test",
        "publicConnectivity": True,
        "replicaCount": 0,
        "deletionProtection": False,
        "provisionedMemory": 16,
        "tags": {"agent": "nx-neptune"},
    }
    assert expected == result


@pytest.mark.asyncio
async def test_create_graph_config_custom_parameters():
    # Unrelated parameters will be discarded.
    config = {
        "custom_parameter": 123,
        "kmsKeyIdentifier": "test_kms",
        "vectorSearchConfiguration": 1024,
    }
    result = _get_create_instance_config("test", config)
    expected = {
        "graphName": "test",
        "publicConnectivity": True,
        "replicaCount": 0,
        "deletionProtection": False,
        "provisionedMemory": 16,
        "tags": {"agent": "nx-neptune"},
        "custom_parameter": 123,
        "kmsKeyIdentifier": "test_kms",
        "vectorSearchConfiguration": 1024,
    }
    assert expected == result


@pytest.mark.asyncio
async def test_create_graph_config_override_default_options():
    # Only permitted parameters will be considered and default will always present regardless.
    config = {
        "publicConnectivity": False,
        "replicaCount": 3,
        "deletionProtection": True,
        "provisionedMemory": 32,
        "tags": {"additional_tag": "test_value"},
    }
    result = _get_create_instance_config("test", config)
    expected = {
        "graphName": "test",
        "publicConnectivity": False,
        "replicaCount": 3,
        "deletionProtection": True,
        "provisionedMemory": 32,
        "tags": {"agent": "nx-neptune", "additional_tag": "test_value"},
    }
    assert expected == result


@pytest.mark.asyncio
async def test_create_random_graph_name_default():
    """Test _create_random_graph_name with default prefix."""
    from nx_neptune.instance_management import _create_random_graph_name

    result = _create_random_graph_name()
    assert result.startswith("nx-neptune-")
    assert len(result) > len("nx-neptune-")


@pytest.mark.asyncio
async def test_create_random_graph_name_custom_prefix():
    """Test _create_random_graph_name with custom prefix."""
    from nx_neptune.instance_management import _create_random_graph_name

    result = _create_random_graph_name("custom-prefix")
    assert result.startswith("custom-prefix-")
    assert len(result) > len("custom-prefix-")


@pytest.mark.asyncio
@patch("nx_neptune.utils.task_future.asyncio.sleep", new_callable=AsyncMock)
@patch("boto3.client")
async def test_start_na_instance_success(mock_boto3_client, mock_sleep):
    """Test successful start of NA instance."""
    from nx_neptune.instance_management import start_na_instance

    mock_na_client = MagicMock()
    mock_boto3_client.return_value = mock_na_client

    # Mock status progression: STOPPED -> STARTING -> AVAILABLE
    mock_na_client.get_graph.side_effect = [
        {"status": "STOPPED"},  # Initial check
        {"status": "STARTING"},  # First poll
        {"status": "AVAILABLE"},  # Complete
    ]
    mock_na_client.start_graph.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }
    mock_na_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalActionName": "neptune-graph:StartGraph", "EvalDecision": "allowed"}
        ]
    }

    result = await start_na_instance("test-graph-id")
    assert result == "test-graph-id"


@pytest.mark.asyncio
@patch("boto3.client")
async def test_start_na_instance_wrong_status(mock_boto3_client):
    """Test start NA instance when graph is not in STOPPED state."""
    from nx_neptune.instance_management import start_na_instance

    mock_na_client = MagicMock()
    mock_boto3_client.return_value = mock_na_client

    mock_na_client.get_graph.return_value = {"status": "AVAILABLE"}
    mock_na_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalActionName": "neptune-graph:StartGraph", "EvalDecision": "allowed"}
        ]
    }

    with pytest.raises(Exception, match="Invalid graph .* instance state"):
        await start_na_instance("test-graph-id")


@pytest.mark.asyncio
@patch("nx_neptune.utils.task_future.asyncio.sleep", new_callable=AsyncMock)
@patch("boto3.client")
async def test_stop_na_instance_success(mock_boto3_client, mock_sleep):
    """Test successful stop of NA instance."""
    from nx_neptune.instance_management import stop_na_instance

    mock_na_client = MagicMock()
    mock_boto3_client.return_value = mock_na_client

    # Mock status progression: AVAILABLE -> STOPPING -> STOPPED
    mock_na_client.get_graph.side_effect = [
        {"status": "AVAILABLE"},  # Initial check
        {"status": "STOPPING"},  # First poll
        {"status": "STOPPED"},  # Complete
    ]
    mock_na_client.stop_graph.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }
    mock_na_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalActionName": "neptune-graph:StopGraph", "EvalDecision": "allowed"}
        ]
    }

    result = await stop_na_instance("test-graph-id")
    assert result == "test-graph-id"


@pytest.mark.asyncio
@patch("nx_neptune.utils.task_future.asyncio.sleep", new_callable=AsyncMock)
@patch("boto3.client")
async def test_create_graph_snapshot_success(mock_boto3_client, mock_sleep):
    """Test successful creation of graph snapshot."""
    from nx_neptune.instance_management import create_graph_snapshot

    mock_na_client = MagicMock()
    mock_boto3_client.return_value = mock_na_client

    mock_na_client.create_graph_snapshot.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 201},
        "id": "test-snapshot-id",
    }
    mock_na_client.get_graph.side_effect = [
        {"status": "SNAPSHOTTING"},
        {"status": "AVAILABLE"},
    ]
    mock_na_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {
                "EvalActionName": "neptune-graph:CreateGraphSnapshot",
                "EvalDecision": "allowed",
            }
        ]
    }

    result = await create_graph_snapshot("test-graph-id", "test-snapshot")
    assert result == "test-snapshot-id"


@pytest.mark.asyncio
@patch("nx_neptune.utils.task_future.asyncio.sleep", new_callable=AsyncMock)
@patch("boto3.client")
async def test_delete_graph_snapshot_success(mock_boto3_client, mock_sleep):
    """Test successful deletion of graph snapshot."""
    from nx_neptune.instance_management import delete_graph_snapshot

    mock_na_client = MagicMock()
    mock_boto3_client.return_value = mock_na_client

    mock_na_client.delete_graph_snapshot.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }
    mock_na_client.get_graph_snapshot.side_effect = [
        {"status": "DELETING"},
        ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "GetGraphSnapshot"
        ),
    ]
    mock_na_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {
                "EvalActionName": "neptune-graph:DeleteGraphSnapshot",
                "EvalDecision": "allowed",
            }
        ]
    }

    result = await delete_graph_snapshot("test-snapshot-id")
    assert result == "test-snapshot-id"


@pytest.mark.asyncio
@patch("nx_neptune.utils.task_future.asyncio.sleep", new_callable=AsyncMock)
@patch("boto3.client")
async def test_create_na_instance_from_snapshot_success(mock_boto3_client, mock_sleep):
    """Test successful creation of NA instance from snapshot."""
    from nx_neptune.instance_management import create_na_instance_from_snapshot

    mock_na_client = MagicMock()
    mock_boto3_client.return_value = mock_na_client

    mock_na_client.restore_graph_from_snapshot.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 201},
        "id": "test-graph-id",
    }
    mock_na_client.get_graph.side_effect = [
        {"status": "CREATING"},
        {"status": "AVAILABLE"},
    ]
    mock_na_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {
                "EvalActionName": "neptune-graph:RestoreGraphFromSnapshot",
                "EvalDecision": "allowed",
            }
        ]
    }

    result = await create_na_instance_from_snapshot("test-snapshot-id")
    assert result == "test-graph-id"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_status_check_future")
@patch("nx_neptune.instance_management._get_or_create_clients")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_create_na_instance_with_s3_import_success(
    mock_get_bucket_encryption_key_arn,
    mock_get_or_create_clients,
    mock_get_status_check_future,
):
    """Test successful creation of NA instance with S3 import."""
    from nx_neptune.instance_management import create_na_instance_with_s3_import

    mock_na_client = MagicMock()
    mock_iam_client = MagicMock()
    mock_iam_client.role_arn = "arn:aws:iam::123456789012:role/test-role"

    mock_get_or_create_clients.return_value = (mock_iam_client, mock_na_client)
    mock_get_bucket_encryption_key_arn.return_value = None

    # Create an async function that returns None
    async def mock_future():
        return None

    # Return a new coroutine each time the function is called
    mock_get_status_check_future.side_effect = lambda *args, **kwargs: mock_future()

    mock_na_client.create_graph_using_import_task.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 201},
        "graphId": "test-graph-id",
        "taskId": "test-task-id",
    }

    graph_id, task_id = await create_na_instance_with_s3_import(
        "s3://test-bucket/test-data/"
    )
    assert graph_id == "test-graph-id"
    assert task_id == "test-task-id"


def test_get_create_instance_with_import_config():
    """Test _get_create_instance_with_import_config function."""
    from nx_neptune.instance_management import _get_create_instance_with_import_config

    result = _get_create_instance_with_import_config(
        "test-graph",
        "s3://test-bucket/data",
        "arn:aws:iam::123456789012:role/test-role",
    )

    assert result["graphName"] == "test-graph"
    assert result["source"] == "s3://test-bucket/data"
    assert result["roleArn"] == "arn:aws:iam::123456789012:role/test-role"
    assert result["format"] == "CSV"
    assert result["publicConnectivity"] is True
    assert result["tags"]["agent"] == "nx-neptune"


def test_get_create_instance_with_import_config_custom():
    """Test _get_create_instance_with_import_config with custom config."""
    from nx_neptune.instance_management import _get_create_instance_with_import_config

    config = {
        "minProvisionedMemory": 32,
        "maxProvisionedMemory": 64,
        "format": "PARQUET",
    }

    result = _get_create_instance_with_import_config(
        "test-graph",
        "s3://test-bucket/data",
        "arn:aws:iam::123456789012:role/test-role",
        config,
    )

    assert result["minProvisionedMemory"] == 32
    assert result["maxProvisionedMemory"] == 64
    assert result["format"] == "PARQUET"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._execute_athena_query")
@patch("boto3.client")
async def test_export_athena_table_to_s3_success(
    mock_boto3_client, mock_execute_athena_query
):
    """Test successful export of Athena table to S3."""
    from nx_neptune.instance_management import export_athena_table_to_s3

    mock_athena_client = MagicMock()
    mock_s3_client = MagicMock()

    def client_factory(service_name):
        if service_name == "athena":
            return mock_athena_client
        elif service_name == "s3":
            return mock_s3_client
        return MagicMock()

    mock_boto3_client.side_effect = client_factory
    mock_execute_athena_query.side_effect = ["query-exec-id-1", "query-exec-id-2"]

    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED", "StateChangeReason": ""}}
    }

    result = await export_athena_table_to_s3(
        ["SELECT * FROM table1", "SELECT * FROM table2"],
        "s3://test-bucket/results/",
        polling_interval=1,
    )

    assert result == ["query-exec-id-1", "query-exec-id-2"]
    assert mock_execute_athena_query.call_count == 2


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_status_check_future")
@patch("boto3.client")
async def test_update_instance_size_success(mock_boto3_client, mock_get_future):
    """Test successful to upsize a NA instance."""
    from nx_neptune.instance_management import update_na_instance_size

    mock_na_client = MagicMock()
    mock_boto3_client.return_value = mock_na_client

    mock_future = MagicMock()
    mock_get_future.return_value = mock_future

    mock_na_client.get_graph.return_value = {"status": "AVAILABLE"}
    mock_na_client.update_graph.return_value = {
        "ResponseMetadata": {"HTTPStatusCode": 200}
    }
    mock_na_client.simulate_principal_policy.return_value = {
        "EvaluationResults": [
            {"EvalActionName": "neptune-graph:UpdateGraph", "EvalDecision": "allowed"}
        ]
    }

    graph_id = await update_na_instance_size("test-graph-id", 32)
    assert graph_id == "test-graph-id"
