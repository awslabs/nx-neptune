import pytest
from concurrent.futures import Future
from unittest.mock import patch, MagicMock
from nx_neptune.instance_management import (
    _clean_s3_path,
    _get_bucket_encryption_key_arn,
    TaskFuture,
    TaskType,
)


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
