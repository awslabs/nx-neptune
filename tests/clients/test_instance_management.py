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
import logging
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
    _get_graph_type,
    _import_via_neptune_load,
    _load_client,
    _LOAD_TIMEOUT_SECONDS,
    GraphType,
    export_csv_to_s3,
    delete_na_instance,
    _get_create_instance_config,
    validate_athena_query,
    ProjectionType,
    empty_s3_bucket,
    drop_athena_table,
    get_athena_query_results,
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
        # Valid embedding header (Node)
        (
            "select col_a as '~id', col_b as 'embedding:vector' from test_table",
            ProjectionType.NODE,
            True,
        ),
        # Invalid embedding type (Node)
        (
            "select col_a as 'embedding:xxxx' from test_table",
            ProjectionType.NODE,
            False,
        ),
        # Invalid variable naming for embedding column (Node)
        ("select col_a as 'xxx:vector' from test_table", ProjectionType.NODE, False),
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
    "s3_path,expected_bucket",
    [
        # Single-level prefix.
        ("s3://my-test-bucket/folder/", "my-test-bucket"),
        # Multi-level prefix: regression guard. _clean_s3_path would return
        # "bucket/a/b" here (rejected by get_bucket_encryption); the bare bucket
        # must be extracted instead.
        ("s3://bucket/a/b/c/", "bucket"),
    ],
)
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
@patch("nx_neptune.instance_management.ClientFactory")
def test_get_bucket_encryption_key_arn(
    mock_factory_cls, mock_response, expected_result, s3_path, expected_bucket
):
    """Test the _get_bucket_encryption_key_arn function with various scenarios."""
    # Mock the S3 client
    mock_s3_client = MagicMock()
    mock_factory_cls.return_value.s3.return_value = mock_s3_client

    # Configure the mock response
    mock_s3_client.get_bucket_encryption.return_value = mock_response

    # Call the function with a test S3 path
    result = _get_bucket_encryption_key_arn(s3_path)

    # Verify the result
    assert result == expected_result

    # Verify the S3 client was called correctly
    mock_factory_cls.return_value.s3.assert_called_once()
    mock_s3_client.get_bucket_encryption.assert_called_once_with(Bucket=expected_bucket)


@patch("nx_neptune.instance_management.ClientFactory")
def test_get_bucket_encryption_key_arn_with_exception(mock_factory_cls):
    """Test handling of exceptions when retrieving bucket encryption."""
    # Mock the S3 client to raise an exception
    mock_s3_client = MagicMock()
    mock_factory_cls.return_value.s3.return_value = mock_s3_client
    mock_s3_client.get_bucket_encryption.side_effect = Exception(
        "Bucket encryption not configured"
    )

    # Call the function with a test S3 path
    result = _get_bucket_encryption_key_arn("s3://my-test-bucket/folder/")

    # Verify the result is None when an exception occurs
    assert result is None

    # Verify the S3 client was called correctly
    mock_factory_cls.return_value.s3.assert_called_once()
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
@patch("nx_neptune.utils.task_future.asyncio.sleep", new_callable=AsyncMock)
@patch("boto3.client")
async def test_create_na_instance_on_created_before_wait(mock_boto3_client, mock_sleep):
    """on_created gets the graph id even when the wait for AVAILABLE fails."""
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client
    mock_nx_client.create_graph.return_value = json.loads(NX_CREATE_SUCCESS_FIXTURE)
    test_status_creating = json.loads(NX_STATUS_CHECK_SUCCESS_FIXTURE)
    test_status_creating["status"] = "CREATING"
    mock_nx_client.get_graph.return_value = test_status_creating
    on_created = MagicMock()

    with pytest.raises(ClientError, match="MaxAttemptsReached"):
        await create_na_instance(max_attempts=1, on_created=on_created)

    on_created.assert_called_once_with("test_graph_id")


@pytest.mark.asyncio
@patch("boto3.client")
async def test_create_na_instance_on_created_not_called_on_rejection(
    mock_boto3_client,
):
    """on_created is not called when creation is not accepted."""
    mock_nx_client = MagicMock()
    mock_boto3_client.return_value = mock_nx_client
    mock_nx_client.create_graph.return_value = json.loads(NX_IMPORT_FAIL_FIXTURE)
    on_created = MagicMock()

    with pytest.raises(Exception, match="Neptune instance creation failure"):
        await create_na_instance(on_created=on_created)

    on_created.assert_not_called()


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


def test_delete_clears_deletion_protection_before_delete():
    """The teardown path must clear deletion protection (update_graph) before
    delete_graph, otherwise Neptune rejects deleting a protected graph."""
    from nx_neptune.instance_management import _delete_na_instance_task

    client = MagicMock()
    manager = MagicMock()
    manager.attach_mock(client.update_graph, "update_graph")
    manager.attach_mock(client.delete_graph, "delete_graph")

    _delete_na_instance_task(client, "g-123")

    client.update_graph.assert_called_once_with(
        graphIdentifier="g-123", deletionProtection=False
    )
    client.delete_graph.assert_called_once_with(
        graphIdentifier="g-123", skipSnapshot=True
    )
    # update_graph must come before delete_graph
    order = [c[0] for c in manager.mock_calls]
    assert order.index("update_graph") < order.index("delete_graph")


def test_delete_proceeds_when_clearing_protection_fails():
    """If clearing deletion protection errors (e.g. already unprotected), the
    delete the caller asked for still proceeds."""
    from nx_neptune.instance_management import _delete_na_instance_task

    client = MagicMock()
    client.update_graph.side_effect = ClientError(
        error_response={"Error": {"Code": "ValidationException"}},
        operation_name="UpdateGraph",
    )

    _delete_na_instance_task(client, "g-123")

    client.delete_graph.assert_called_once_with(
        graphIdentifier="g-123", skipSnapshot=True
    )


def test_private_graph_logs_connectivity_hint(caplog, monkeypatch):
    """A private (default) graph logs a hint pointing at the opt-in env var."""
    from nx_neptune.instance_management import _get_create_instance_config

    monkeypatch.delenv("NETWORKX_PUBLIC_CONNECTIVITY", raising=False)
    with caplog.at_level("INFO", logger="nx_neptune.instance_management"):
        _get_create_instance_config("test")
    assert "NETWORKX_PUBLIC_CONNECTIVITY=true" in caplog.text


def test_public_graph_does_not_log_connectivity_hint(caplog, monkeypatch):
    """A public graph (opt-in) does not emit the private-connectivity hint."""
    from nx_neptune.instance_management import _get_create_instance_config

    monkeypatch.setenv("NETWORKX_PUBLIC_CONNECTIVITY", "true")
    with caplog.at_level("INFO", logger="nx_neptune.instance_management"):
        _get_create_instance_config("test")
    assert "publicConnectivity=false" not in caplog.text


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
async def test_create_graph_config_base(monkeypatch):
    # Assert the secure defaults independent of the developer's environment.
    monkeypatch.delenv("NETWORKX_PUBLIC_CONNECTIVITY", raising=False)
    monkeypatch.delenv("NETWORKX_DELETION_PROTECTION", raising=False)
    result = _get_create_instance_config("test")
    expected = {
        "graphName": "test",
        "publicConnectivity": False,
        "replicaCount": 0,
        "deletionProtection": True,
        "provisionedMemory": 16,
        "tags": {"agent": "nx-neptune"},
    }
    assert expected == result


@pytest.mark.asyncio
async def test_create_graph_config_custom_parameters(monkeypatch):
    # Assert the secure defaults independent of the developer's environment.
    monkeypatch.delenv("NETWORKX_PUBLIC_CONNECTIVITY", raising=False)
    monkeypatch.delenv("NETWORKX_DELETION_PROTECTION", raising=False)
    # Unrelated parameters will be discarded.
    config = {
        "custom_parameter": 123,
        "kmsKeyIdentifier": "test_kms",
        "vectorSearchConfiguration": 1024,
    }
    result = _get_create_instance_config("test", config)
    expected = {
        "graphName": "test",
        "publicConnectivity": False,
        "replicaCount": 0,
        "deletionProtection": True,
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
@pytest.mark.parametrize("memory", [4, 8])
async def test_create_graph_config_tiny_graph(monkeypatch, memory):
    # provisionedMemory 4/8 is a graph session, which uses a different request
    # shape: the value moves to computeConfiguration. provisionedMemory must not
    # be sent.
    monkeypatch.delenv("NETWORKX_PUBLIC_CONNECTIVITY", raising=False)
    monkeypatch.delenv("NETWORKX_DELETION_PROTECTION", raising=False)
    result = _get_create_instance_config("test", {"provisionedMemory": memory})
    expected = {
        "graphName": "test",
        "publicConnectivity": False,
        "replicaCount": 0,
        "deletionProtection": True,
        "computeConfiguration": {"graphSession": {"minMemory": memory}},
        "tags": {"agent": "nx-neptune"},
    }
    assert expected == result
    assert "provisionedMemory" not in result


@pytest.mark.parametrize("memory", [1, 2, 3, 5, 6, 7, 9, 15])
def test_create_graph_config_rejects_unsupported_small_memory(memory):
    # Only 4 or 8 are valid below 16; any other small size must raise rather
    # than silently falling through to the provisioned path (server min is 16).
    with pytest.raises(ValueError, match=rf"Unsupported memory size {memory} NCU"):
        _get_create_instance_config("test", {"provisionedMemory": memory})


@pytest.mark.parametrize("memory", [4, 8])
def test_create_graph_config_conflicting_memory_and_compute_raises(memory):
    # Passing both a graph-session provisionedMemory and an explicit
    # computeConfiguration is ambiguous and must raise, not silently drop the
    # requested size.
    with pytest.raises(ValueError, match="Cannot set both provisionedMemory"):
        _get_create_instance_config(
            "test",
            {
                "provisionedMemory": memory,
                "computeConfiguration": {"graphSession": {"minMemory": 8}},
            },
        )


def test_create_graph_config_conflict_raises_for_nonsession_memory():
    # The conflict check is unconditional: it must also fire when the supplied
    # provisionedMemory is a non-session (>=16) size, not only for 4/8.
    with pytest.raises(ValueError, match="Cannot set both provisionedMemory"):
        _get_create_instance_config(
            "test",
            {
                "provisionedMemory": 16,
                "computeConfiguration": {"graphSession": {"minMemory": 8}},
            },
        )


def test_create_graph_config_nested_compute_only():
    # Passing the nested computeConfiguration form directly (no
    # provisionedMemory) must be accepted as-is: no provisionedMemory injected.
    result = _get_create_instance_config(
        "test",
        {"computeConfiguration": {"graphSession": {"minMemory": 8}}},
    )
    assert result["computeConfiguration"] == {"graphSession": {"minMemory": 8}}
    assert "provisionedMemory" not in result


@pytest.mark.asyncio
async def test_create_graph_config_default_is_ec2():
    # No memory specified -> standard EC2-backed graph, no graph-session keys.
    result = _get_create_instance_config("test")
    assert result["provisionedMemory"] == 16
    assert "computeConfiguration" not in result


@pytest.mark.asyncio
async def test_create_graph_config_env_override(monkeypatch):
    # Env vars flip the secure defaults when no explicit config is given.
    monkeypatch.setenv("NETWORKX_PUBLIC_CONNECTIVITY", "true")
    monkeypatch.setenv("NETWORKX_DELETION_PROTECTION", "false")
    result = _get_create_instance_config("test")
    assert result["publicConnectivity"] is True
    assert result["deletionProtection"] is False


@pytest.mark.asyncio
async def test_create_graph_config_explicit_beats_env(monkeypatch):
    # Explicit config always wins over env vars.
    monkeypatch.setenv("NETWORKX_PUBLIC_CONNECTIVITY", "true")
    monkeypatch.setenv("NETWORKX_DELETION_PROTECTION", "false")
    result = _get_create_instance_config(
        "test", {"publicConnectivity": False, "deletionProtection": True}
    )
    assert result["publicConnectivity"] is False
    assert result["deletionProtection"] is True


@pytest.mark.asyncio
async def test_create_graph_config_unrecognized_env_keeps_secure(monkeypatch):
    # A typo / unrecognized value must land on the secure side for both:
    # public stays private, deletion stays protected.
    monkeypatch.setenv("NETWORKX_PUBLIC_CONNECTIVITY", "yes")
    monkeypatch.setenv("NETWORKX_DELETION_PROTECTION", "ture")
    result = _get_create_instance_config("test")
    assert result["publicConnectivity"] is False
    assert result["deletionProtection"] is True


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


def test_generated_graph_name_matches_its_session():
    """A generated name is recognised by _is_session_graph for its own prefix."""
    from nx_neptune.instance_management import (
        _create_random_graph_name,
        _is_session_graph,
    )

    assert _is_session_graph(_create_random_graph_name("demo"), "demo")


@pytest.mark.parametrize(
    "graph_name, expected",
    [
        ("demo-1b4e28ba-2fa1-41d2-883f-0016d3cca427", True),
        ("demo2-1b4e28ba-2fa1-41d2-883f-0016d3cca427", False),
        ("demo-prod-1b4e28ba-2fa1-41d2-883f-0016d3cca427", False),
        ("demo-backup", False),
        ("demo-", False),
        ("demo", True),
        ("demo-1B4E28BA-2FA1-41D2-883F-0016D3CCA427", False),
        ("demo-1b4e28ba2fa141d2883f0016d3cca427", False),
        ("demo-1b4e28ba-2fa1-41d2-883f-0016d3cca427-x", False),
        ("x-demo-1b4e28ba-2fa1-41d2-883f-0016d3cca427", False),
    ],
)
def test_is_session_graph(graph_name, expected):
    """Only the session name itself or '{session}-{uuid}' in canonical form matches."""
    from nx_neptune.instance_management import _is_session_graph

    assert _is_session_graph(graph_name, "demo") is expected


def test_is_session_graph_escapes_session_name():
    """Regex characters in the session name are matched literally."""
    from nx_neptune.instance_management import _is_session_graph

    uid = "1b4e28ba-2fa1-41d2-883f-0016d3cca427"
    assert _is_session_graph(f"a.b-{uid}", "a.b")
    assert not _is_session_graph(f"axb-{uid}", "a.b")


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
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
async def test_create_na_instance_with_s3_import_success(
    mock_get_bucket_encryption_key_arn,
    mock_resolve_iam,
    mock_get_status_check_future,
):
    """Test successful creation of NA instance with S3 import."""
    from nx_neptune.instance_management import create_na_instance_with_s3_import

    mock_na_client = MagicMock()
    mock_iam_client = MagicMock()
    mock_iam_client.role_arn = "arn:aws:iam::123456789012:role/test-role"

    mock_resolve_iam.return_value = mock_iam_client
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
        "s3://test-bucket/test-data/",
        na_client=mock_na_client,
    )
    assert graph_id == "test-graph-id"
    assert task_id == "test-task-id"


def test_get_create_instance_with_import_config(monkeypatch):
    """Test _get_create_instance_with_import_config function."""
    from nx_neptune.instance_management import _get_create_instance_with_import_config

    # Assert the secure defaults independent of the developer's environment.
    monkeypatch.delenv("NETWORKX_PUBLIC_CONNECTIVITY", raising=False)
    monkeypatch.delenv("NETWORKX_DELETION_PROTECTION", raising=False)
    result = _get_create_instance_with_import_config(
        "test-graph",
        "s3://test-bucket/data",
        "arn:aws:iam::123456789012:role/test-role",
    )

    assert result["graphName"] == "test-graph"
    assert result["source"] == "s3://test-bucket/data"
    assert result["roleArn"] == "arn:aws:iam::123456789012:role/test-role"
    assert result["format"] == "CSV"
    assert result["publicConnectivity"] is False
    assert result["deletionProtection"] is True
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


@pytest.mark.parametrize("key", ["minProvisionedMemory", "provisionedMemory"])
@pytest.mark.parametrize("memory", [4, 8])
def test_get_create_instance_with_import_config_rejects_session_memory(key, memory):
    """Create-with-import must reject 4/8 s-NCU requests (16 m-NCU or above only)."""
    from nx_neptune.instance_management import _get_create_instance_with_import_config

    with pytest.raises(
        ValueError,
        match=r"does not support 4 or 8 s-NCU memory; use 16 m-NCU or above",
    ):
        _get_create_instance_with_import_config(
            "test-graph",
            "s3://test-bucket/data",
            "arn:aws:iam::123456789012:role/test-role",
            {key: memory},
        )


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._execute_athena_query")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("boto3.client")
async def test_export_athena_table_to_s3_success(
    mock_boto3_client,
    mock_resolve_iam,
    mock_get_bucket_encryption,
    mock_execute_athena_query,
):
    """Test successful export of Athena table to S3."""
    from nx_neptune.instance_management import export_athena_table_to_s3

    mock_athena_client = MagicMock()
    mock_s3_client = MagicMock()
    mock_iam_client = MagicMock()
    mock_iam_client.has_athena_permissions.return_value = True

    def client_factory(service_name):
        if service_name == "athena":
            return mock_athena_client
        elif service_name == "s3":
            return mock_s3_client
        return MagicMock()

    mock_boto3_client.side_effect = client_factory
    mock_resolve_iam.return_value = mock_iam_client
    mock_get_bucket_encryption.return_value = None
    mock_execute_athena_query.side_effect = ["query-exec-id-1", "query-exec-id-2"]

    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED", "StateChangeReason": ""}}
    }

    result = await export_athena_table_to_s3(
        ["SELECT * FROM table1 WHERE id = ?", "SELECT * FROM table2 WHERE status = ?"],
        [["123"], ["active"]],  # sql_parameters - one list per query
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


@patch("nx_neptune.instance_management.boto3.client")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_empty_s3_bucket_folder_success(mock_resolve_iam, mock_boto3_client):
    """Test empty_s3_bucket with folder path (ends with /)."""
    # Setup mocks
    mock_s3_client = MagicMock()
    mock_iam_client = MagicMock()
    mock_iam_client.has_delete_s3_permissions.return_value = True

    mock_boto3_client.return_value = mock_s3_client
    mock_resolve_iam.return_value = mock_iam_client

    # Mock paginator for folder deletion
    mock_paginator = MagicMock()
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "folder/file1.txt"}, {"Key": "folder/file2.txt"}]}
    ]
    mock_s3_client.delete_objects.return_value = {"Deleted": []}

    # Test folder deletion
    empty_s3_bucket("s3://test-bucket/folder/")

    # Verify calls
    mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test-bucket", Prefix="folder/"
    )
    mock_s3_client.delete_objects.assert_called_once()


@patch("nx_neptune.instance_management.boto3.client")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_empty_s3_bucket_specific_key_success(mock_resolve_iam, mock_boto3_client):
    """Test empty_s3_bucket with specific key path."""
    # Setup mocks
    mock_s3_client = MagicMock()
    mock_iam_client = MagicMock()
    mock_iam_client.has_delete_s3_permissions.return_value = True

    mock_boto3_client.return_value = mock_s3_client
    mock_resolve_iam.return_value = mock_iam_client

    mock_s3_client.delete_objects.return_value = {"Deleted": []}

    # Test specific key deletion
    empty_s3_bucket("s3://test-bucket/file.txt")

    # Verify calls
    mock_s3_client.delete_objects.assert_called_once_with(
        Bucket="test-bucket", Delete={"Objects": [{"Key": "file.txt"}], "Quiet": False}
    )


@patch("nx_neptune.instance_management.boto3.client")
def test_empty_s3_bucket_invalid_arn(mock_boto3_client):
    """Test empty_s3_bucket with invalid S3 ARN."""
    with pytest.raises(ValueError, match="s3_arn must be a non-empty string"):
        empty_s3_bucket("")

    with pytest.raises(ValueError, match="s3_arn must be a non-empty string"):
        empty_s3_bucket(None)


@patch("nx_neptune.instance_management.boto3.client")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_empty_s3_bucket_permission_error(mock_resolve_iam, mock_boto3_client):
    """Test empty_s3_bucket with permission error."""
    # Setup mocks
    mock_iam_client = MagicMock()
    mock_s3_client = MagicMock()

    mock_boto3_client.return_value = mock_s3_client
    mock_resolve_iam.return_value = mock_iam_client
    mock_iam_client.has_delete_s3_permissions.side_effect = Exception(
        "Permission denied"
    )

    # Test permission error
    with pytest.raises(Exception, match="Permission denied"):
        empty_s3_bucket("s3://test-bucket/folder/")


@patch("nx_neptune.instance_management.boto3.client")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_empty_s3_bucket_client_error(mock_resolve_iam, mock_boto3_client):
    """Test empty_s3_bucket with S3 client error."""
    # Setup mocks
    mock_s3_client = MagicMock()
    mock_iam_client = MagicMock()

    mock_boto3_client.return_value = mock_s3_client
    mock_resolve_iam.return_value = mock_iam_client
    mock_iam_client.has_delete_s3_permissions.return_value = True

    # Mock S3 client error
    mock_s3_client.delete_objects.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}},
        "delete_objects",
    )

    # Test S3 client error
    with pytest.raises(Exception, match="Failed to empty S3 bucket"):
        empty_s3_bucket("s3://test-bucket/file.txt")


@pytest.mark.asyncio
@patch("nx_neptune.instance_management.boto3.client")
@patch("nx_neptune.instance_management._execute_athena_query")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management.TaskFuture")
async def test_drop_athena_table_success(
    mock_task_future,
    mock_resolve_iam,
    mock_get_bucket_encryption,
    mock_execute_athena_query,
    mock_boto3_client,
):
    """Test drop_athena_table with successful execution."""
    # Setup mocks
    mock_athena_client = MagicMock()
    mock_boto3_client.return_value = mock_athena_client
    mock_execute_athena_query.return_value = "test-query-execution-id"

    mock_iam_client = MagicMock()
    mock_iam_client.has_athena_permissions.return_value = True
    mock_resolve_iam.return_value = mock_iam_client
    mock_get_bucket_encryption.return_value = None

    mock_future = AsyncMock()
    mock_future.current_status = "SUCCEEDED"
    mock_task_future.return_value = mock_future

    # Test the function
    result = await drop_athena_table("test_table", "s3://test-bucket/results/")

    # Verify calls
    mock_execute_athena_query.assert_called_once_with(
        mock_athena_client,
        "DROP TABLE test_table",
        "s3://test-bucket/results/",
        catalog=None,
        database=None,
    )
    mock_task_future.assert_called_once()
    mock_future.wait_until_complete.assert_called_once_with(mock_athena_client)

    assert result == "test-query-execution-id"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management.boto3.client")
@patch("nx_neptune.instance_management._execute_athena_query")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management.TaskFuture")
async def test_drop_athena_table_with_catalog_database(
    mock_task_future,
    mock_resolve_iam,
    mock_get_bucket_encryption,
    mock_execute_athena_query,
    mock_boto3_client,
):
    """Test drop_athena_table with catalog and database parameters."""
    # Setup mocks
    mock_athena_client = MagicMock()
    mock_boto3_client.return_value = mock_athena_client
    mock_execute_athena_query.return_value = "test-query-execution-id"

    mock_iam_client = MagicMock()
    mock_iam_client.has_athena_permissions.return_value = True
    mock_resolve_iam.return_value = mock_iam_client
    mock_get_bucket_encryption.return_value = None

    mock_future = AsyncMock()
    mock_future.current_status = "SUCCEEDED"
    mock_task_future.return_value = mock_future

    # Test the function with catalog and database
    result = await drop_athena_table(
        "test_table",
        "s3://test-bucket/results/",
        catalog="test_catalog",
        database="test_database",
    )

    # Verify calls
    mock_execute_athena_query.assert_called_once_with(
        mock_athena_client,
        "DROP TABLE test_table",
        "s3://test-bucket/results/",
        catalog="test_catalog",
        database="test_database",
    )

    assert result == "test-query-execution-id"


@pytest.mark.asyncio
@patch("nx_neptune.instance_management.boto3.client")
@patch("nx_neptune.instance_management._execute_athena_query")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management.TaskFuture")
async def test_drop_athena_table_with_polling_params(
    mock_task_future,
    mock_resolve_iam,
    mock_get_bucket_encryption,
    mock_execute_athena_query,
    mock_boto3_client,
):
    """Test drop_athena_table with polling parameters."""
    # Setup mocks
    mock_athena_client = MagicMock()
    mock_boto3_client.return_value = mock_athena_client
    mock_execute_athena_query.return_value = "test-query-execution-id"

    mock_iam_client = MagicMock()
    mock_iam_client.has_athena_permissions.return_value = True
    mock_resolve_iam.return_value = mock_iam_client
    mock_get_bucket_encryption.return_value = None

    mock_future = AsyncMock()
    mock_future.current_status = "SUCCEEDED"
    mock_task_future.return_value = mock_future

    # Test the function with polling parameters
    result = await drop_athena_table(
        "test_table", "s3://test-bucket/results/", polling_interval=30, max_attempts=10
    )

    # Verify TaskFuture was called with correct parameters
    mock_task_future.assert_called_once_with(
        "test-query-execution-id", TaskType.EXPORT_ATHENA_TABLE, 30, 10
    )

    assert result == "test-query-execution-id"


@patch("nx_neptune.instance_management.boto3.client")
def test_get_athena_query_results_single_page(mock_boto3_client):
    """Test fetching Athena query results with a single page."""
    mock_athena_client = MagicMock()
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_paginator = MagicMock()
    mock_athena_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},
                    {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "Alice"}]},
                    {"Data": [{"VarCharValue": "2"}, {"VarCharValue": "Bob"}]},
                ]
            }
        }
    ]

    rows = get_athena_query_results("test-query-id", client=mock_athena_client)

    assert len(rows) == 3
    assert rows[0] == ["id", "name"]
    assert rows[1] == ["1", "Alice"]
    assert rows[2] == ["2", "Bob"]
    mock_athena_client.get_paginator.assert_called_once_with("get_query_results")
    mock_paginator.paginate.assert_called_once_with(QueryExecutionId="test-query-id")


@patch("nx_neptune.instance_management.boto3.client")
def test_get_athena_query_results_multiple_pages(mock_boto3_client):
    """Test fetching Athena query results across multiple pages."""
    mock_athena_client = MagicMock()
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_paginator = MagicMock()
    mock_athena_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "1"}]},
                ]
            }
        },
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "2"}]},
                ]
            }
        },
    ]

    rows = get_athena_query_results("test-query-id", client=mock_athena_client)

    assert len(rows) == 3
    assert rows[0] == ["id"]
    assert rows[1] == ["1"]
    assert rows[2] == ["2"]


@patch("nx_neptune.instance_management.boto3.client")
def test_get_athena_query_results_empty(mock_boto3_client):
    """Test fetching Athena query results with no rows."""
    mock_athena_client = MagicMock()
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_paginator = MagicMock()
    mock_athena_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{"ResultSet": {"Rows": []}}]

    rows = get_athena_query_results("test-query-id", client=mock_athena_client)

    assert rows == []


@patch("nx_neptune.instance_management.ClientFactory")
def test_get_athena_query_results_creates_default_client(mock_factory_cls):
    """Test that a default Athena client is created when none is provided."""
    mock_athena_client = MagicMock()
    mock_factory_cls.return_value.athena.return_value = mock_athena_client
    mock_athena_client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_paginator = MagicMock()
    mock_athena_client.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{"ResultSet": {"Rows": []}}]

    get_athena_query_results("test-query-id")

    mock_factory_cls.return_value.athena.assert_called_once()


# ---------------------------------------------------------------------------
# Graph-type detection + synchronous neptune.load import (graph session path)
# ---------------------------------------------------------------------------


def _make_na_client(execute_return=None, region="us-west-2", graph_id="g-1"):
    """Build a mock NeptuneAnalyticsClient-like object for the load helper."""
    na_client = MagicMock()
    na_client.graph_id = graph_id
    na_client.client.meta.region_name = region
    if execute_return is not None:
        na_client.execute_query.return_value = execute_return
    return na_client


def test_get_graph_type_session():
    client = MagicMock()
    client.get_graph.return_value = {
        "computeConfiguration": {"graphSession": {"minMemory": 8}}
    }
    assert _get_graph_type("g-1", client) == GraphType.SESSION
    client.get_graph.assert_called_once_with(graphIdentifier="g-1")


def test_get_graph_type_provisioned():
    client = MagicMock()
    client.get_graph.return_value = {
        "computeConfiguration": {"provisionedGraph": {"memory": 16}}
    }
    assert _get_graph_type("g-1", client) == GraphType.PROVISIONED


def test_get_graph_type_missing_compute_is_unknown():
    client = MagicMock()
    client.get_graph.return_value = {}
    assert _get_graph_type("g-1", client) == GraphType.UNKNOWN


@pytest.mark.parametrize(
    "details, expected",
    [
        ({"computeConfiguration": {"graphSession": {"minMemory": 4}}}, "session"),
        ({"computeConfiguration": {"provisionedGraph": {"memory": 16}}}, "provisioned"),
        ({"computeConfiguration": None}, "unknown"),
        ({"computeConfiguration": {}}, "unknown"),
        ({"provisionedMemory": 16}, "provisioned"),
        ({}, "unknown"),
    ],
)
def test_graph_type_from_details(details, expected):
    # Works on a GetGraph response or a ListGraphs item; makes no AWS call.
    from nx_neptune.instance_management import graph_type_from_details

    assert graph_type_from_details(details).value == expected


def test_import_via_neptune_load_success_returns_load_id():
    na_client = _make_na_client(
        execute_return=[
            {"totalRecords": 108070, "insertErrors": 0, "loadId": "abc-123"}
        ]
    )
    load_id = _import_via_neptune_load(na_client, "s3://bucket/prefix/")
    assert load_id == "abc-123"
    # The query carries source, region and format.
    query = na_client.execute_query.call_args[0][0]
    assert "neptune.load" in query
    assert "s3://bucket/prefix/" in query
    assert "us-west-2" in query
    assert "csv" in query


def test_import_via_neptune_load_insert_errors_raises():
    na_client = _make_na_client(execute_return=[{"insertErrors": 5, "loadId": "bad-1"}])
    with pytest.raises(Exception, match="insert error"):
        _import_via_neptune_load(na_client, "s3://bucket/prefix/")


@pytest.mark.parametrize(
    "stats",
    [
        {"totalRecords": 1, "insertErrors": 0},  # loadId key absent
        {"totalRecords": 1, "insertErrors": 0, "loadId": None},
        {"totalRecords": 1, "insertErrors": 0, "loadId": ""},
    ],
)
def test_import_via_neptune_load_missing_load_id_raises(stats):
    # The contract promises a loadId string; a missing/empty id is a broken
    # response and must raise rather than silently return None.
    na_client = _make_na_client(execute_return=[stats])
    with pytest.raises(Exception, match="no loadId"):
        _import_via_neptune_load(na_client, "s3://bucket/prefix/")


@pytest.mark.parametrize(
    "source",
    [
        's3://b/x"}) MATCH (n) DETACH DELETE n //',
        's3://bucket/pre"fix/',
        "s3://bucket/pre\\fix/",
        "s3://bucket/trailing\\",
        "s3://bucket/a\nb/",
        "s3://bucket/x'}) //",
        "http://bucket/prefix/",
        "bucket/prefix/",
        "s3://B/prefix/",
        "",
        None,
    ],
)
def test_import_via_neptune_load_rejects_unsafe_source(source):
    na_client = _make_na_client(execute_return=[{"loadId": "x", "insertErrors": 0}])
    with pytest.raises(ValueError, match="Invalid S3 location"):
        _import_via_neptune_load(na_client, source)
    na_client.execute_query.assert_not_called()


@pytest.mark.parametrize("region", ['us-west-2", format: "x', "US-WEST-2", "", None])
def test_import_via_neptune_load_rejects_unsafe_region(region):
    na_client = _make_na_client(
        execute_return=[{"loadId": "x", "insertErrors": 0}], region=region
    )
    with pytest.raises(ValueError, match="Invalid region"):
        _import_via_neptune_load(na_client, "s3://bucket/prefix/")
    na_client.execute_query.assert_not_called()


@pytest.mark.parametrize("data_format", ['csv"}) //', "parquet", "", None])
def test_import_via_neptune_load_rejects_unsupported_format(data_format):
    na_client = _make_na_client(execute_return=[{"loadId": "x", "insertErrors": 0}])
    with pytest.raises(ValueError, match="Unsupported format"):
        _import_via_neptune_load(na_client, "s3://bucket/prefix/", data_format)
    na_client.execute_query.assert_not_called()


@pytest.mark.parametrize(
    "source",
    ["s3://bucket", "s3://bucket/", "s3://my.bucket-1/a/b_c=1/d.e/", "s3://abc/x*"],
)
def test_import_via_neptune_load_accepts_normal_sources(source):
    na_client = _make_na_client(execute_return=[{"loadId": "ok", "insertErrors": 0}])
    assert _import_via_neptune_load(na_client, source) == "ok"


@pytest.mark.parametrize(
    "bad_response",
    [[], None, {}, "not-a-list"],
)
def test_import_via_neptune_load_bad_shape_raises(bad_response):
    na_client = _make_na_client(execute_return=bad_response)
    with pytest.raises(Exception, match="Unexpected neptune.load response"):
        _import_via_neptune_load(na_client, "s3://bucket/prefix/")


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management.reset_graph")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management._load_client")
@patch("nx_neptune.instance_management._import_via_neptune_load")
@patch("nx_neptune.instance_management._get_graph_type")
@patch("nx_neptune.instance_management._start_import_task")
async def test_import_csv_from_s3_routes_session_to_neptune_load(
    mock_start_import_task,
    mock_get_graph_type,
    mock_load,
    mock_load_client,
    mock_iam_wrapper,
    mock_reset_graph,
    mock_key_arn,
):
    # A session graph must use neptune.load and skip the import task entirely.
    mock_get_graph_type.return_value = GraphType.SESSION
    mock_load.return_value = "load-id-1"
    mock_key_arn.return_value = None

    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "g-session"
    mock_na_graph.na_client.client = MagicMock()

    result = await import_csv_from_s3(
        mock_na_graph, "s3://bucket/prefix/", reset_graph_ahead=True
    )

    assert result == "load-id-1"
    # Caller-side read permission is validated (not the service-role import check).
    mock_iam_wrapper.return_value.has_read_s3_permissions.assert_called_once_with(
        "s3://bucket/prefix/", None
    )
    # The caller's identity is checked, not na_graph.iam_client (which may be a
    # role for Neptune to assume).
    mock_na_graph.iam_client.has_read_s3_permissions.assert_not_called()
    mock_na_graph.iam_client.has_import_from_s3_permissions.assert_not_called()
    # Reset honored, then load; import task never used.
    mock_reset_graph.assert_called_once()
    mock_load_client.assert_called_once_with(mock_na_graph.na_client)
    mock_load.assert_called_once_with(
        mock_load_client.return_value, "s3://bucket/prefix/"
    )
    mock_start_import_task.assert_not_called()


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management.reset_graph")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management._load_client")
@patch("nx_neptune.instance_management._import_via_neptune_load")
@patch("nx_neptune.instance_management._get_graph_type")
async def test_import_csv_from_s3_session_forwards_kms_key_arn(
    mock_get_graph_type,
    mock_load,
    mock_load_client,
    mock_iam_wrapper,
    mock_reset_graph,
    mock_key_arn,
):
    # When the source bucket is KMS-encrypted, the resolved key ARN must be
    # forwarded to the caller-side read check so kms:Decrypt is validated.
    key_arn = "arn:aws:kms:us-west-2:123456789012:key/abc"
    mock_get_graph_type.return_value = GraphType.SESSION
    mock_load.return_value = "load-id-3"
    mock_key_arn.return_value = key_arn

    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "g-session"
    mock_na_graph.na_client.client = MagicMock()

    result = await import_csv_from_s3(
        mock_na_graph, "s3://bucket/prefix/", reset_graph_ahead=False
    )

    assert result == "load-id-3"
    mock_iam_wrapper.return_value.has_read_s3_permissions.assert_called_once_with(
        "s3://bucket/prefix/", key_arn
    )
    mock_reset_graph.assert_not_called()


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management.reset_graph")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management._load_client")
@patch("nx_neptune.instance_management._import_via_neptune_load")
@patch("nx_neptune.instance_management._get_graph_type")
async def test_import_csv_from_s3_session_skips_reset_when_false(
    mock_get_graph_type,
    mock_load,
    mock_load_client,
    mock_iam_wrapper,
    mock_reset_graph,
    mock_key_arn,
):
    mock_get_graph_type.return_value = GraphType.SESSION
    mock_load.return_value = "load-id-2"
    mock_key_arn.return_value = None

    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "g-session"
    mock_na_graph.na_client.client = MagicMock()

    result = await import_csv_from_s3(
        mock_na_graph, "s3://bucket/prefix/", reset_graph_ahead=False
    )

    assert result == "load-id-2"
    mock_iam_wrapper.return_value.has_read_s3_permissions.assert_called_once()
    mock_reset_graph.assert_not_called()


@patch("nx_neptune.instance_management.ClientFactory")
def test_load_client_has_long_timeout_and_single_attempt(mock_factory):
    # neptune.load must not be re-sent, and must not hit the 60s default read timeout.
    na_client = _make_na_client(region="us-west-2", graph_id="g-9")
    load_client = _load_client(na_client)

    mock_factory.assert_called_once_with(
        region="us-west-2", timeout_seconds=_LOAD_TIMEOUT_SECONDS, max_attempts=1
    )
    assert load_client.graph_id == "g-9"
    assert load_client.client is mock_factory.return_value.neptune.return_value
    # Also sent as queryTimeoutMilliseconds on the query itself.
    assert load_client.timeout_seconds == _LOAD_TIMEOUT_SECONDS


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management._load_client")
@patch("nx_neptune.instance_management._get_graph_type")
async def test_import_csv_from_s3_session_load_runs_off_the_event_loop(
    mock_get_graph_type, mock_load_client, mock_iam_wrapper, mock_key_arn
):
    import threading
    import time

    mock_get_graph_type.return_value = GraphType.SESSION
    mock_key_arn.return_value = None
    threads = []

    def slow_load(client, s3_arn):
        threads.append(threading.current_thread())
        time.sleep(0.3)
        return "load-id"

    ticks = []

    async def ticker():
        for _ in range(5):
            await asyncio.sleep(0.05)
            ticks.append(1)

    mock_na_graph = MagicMock()
    with patch("nx_neptune.instance_management._import_via_neptune_load", slow_load):
        result, _ = await asyncio.gather(
            import_csv_from_s3(
                mock_na_graph, "s3://bucket/prefix/", reset_graph_ahead=False
            ),
            ticker(),
        )

    assert result == "load-id"
    assert threads[0] is not threading.main_thread()
    # The event loop kept running while the load blocked in its worker thread.
    assert len(ticks) == 5


@pytest.mark.parametrize(
    "details, expected",
    [
        ({"provisionedMemory": 16}, 16),
        ({"computeConfiguration": {"provisionedGraph": {"memory": 32}}}, 32),
        (
            {
                "provisionedMemory": 16,
                "computeConfiguration": {"provisionedGraph": {"memory": 32}},
            },
            16,
        ),
        ({"computeConfiguration": {"graphSession": {"minMemory": 8}}}, None),
        ({"computeConfiguration": None}, None),
        ({}, None),
    ],
)
def test_provisioned_memory_from_details(details, expected):
    from nx_neptune.instance_management import provisioned_memory_from_details

    assert provisioned_memory_from_details(details) == expected


def _reset_not_supported_error():
    from botocore.exceptions import ClientError

    return ClientError(
        {
            "Error": {
                "Code": "ValidationException",
                "Message": "ResetGraph is not supported for graph sessions.",
            }
        },
        "ResetGraph",
    )


@pytest.mark.asyncio
async def test_reset_graph_propagates_the_service_error_unchanged():
    # reset_graph does not classify or swallow a ResetGraph failure: the caller
    # gets the service's own exception, and nothing waits on a reset that never began.
    from nx_neptune.instance_management import reset_graph

    error = _reset_not_supported_error()
    na_client = MagicMock()
    na_client.reset_graph.side_effect = error

    with patch("nx_neptune.instance_management.TaskFuture") as mock_future:
        with pytest.raises(type(error)) as raised:
            await reset_graph("g-1", na_client)

    assert raised.value is error
    mock_future.assert_not_called()


@pytest.mark.asyncio
async def test_reset_graph_failure_logs_a_warning_with_the_details(caplog):
    from botocore.exceptions import ClientError
    from nx_neptune.instance_management import reset_graph

    # A service message that carries an ARN and an account ID.
    raw = "Cannot reset arn:aws:neptune-graph:us-west-2:123456789012:graph/g-42"
    error = ClientError(
        {"Error": {"Code": "ValidationException", "Message": raw}}, "ResetGraph"
    )
    na_client = MagicMock()
    na_client.reset_graph.side_effect = error

    with caplog.at_level(logging.WARNING, logger="nx_neptune.instance_management"):
        with pytest.raises(ClientError) as raised:
            await reset_graph("g-42", na_client, skip_snapshot=False)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "g-42" in message
    assert "skip_snapshot=False" in message
    assert "ValidationException" in message
    assert "not reset" in message
    # The service's message is not logged, in the text or as an attached traceback.
    assert raw not in message
    assert "123456789012" not in message
    assert warnings[0].exc_info is None
    # It is still on the exception the caller receives.
    assert raised.value is error
    assert raw in str(raised.value)


@pytest.mark.asyncio
@patch("nx_neptune.instance_management._create_iam_wrapper")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._import_via_neptune_load")
@patch("nx_neptune.instance_management._get_graph_type")
async def test_import_csv_from_s3_aborts_before_loading_when_reset_fails(
    mock_get_graph_type, mock_load, mock_key_arn, mock_iam_wrapper
):
    # The reset runs before the load, so a failed reset leaves the graph unchanged.
    mock_get_graph_type.return_value = GraphType.SESSION
    mock_key_arn.return_value = None
    error = _reset_not_supported_error()
    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "g-session"
    mock_na_graph.na_client.client.reset_graph.side_effect = error

    with pytest.raises(type(error)) as raised:
        await import_csv_from_s3(
            mock_na_graph, "s3://bucket/prefix/", reset_graph_ahead=True
        )

    assert raised.value is error
    mock_load.assert_not_called()
