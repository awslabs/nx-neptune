import pytest

from nx_neptune.clients.iam_client import IamClient, _get_s3_in_arn


@pytest.mark.parametrize(
    "s3_path,expected",
    [
        # Test with s3:// prefix
        ("s3://my-bucket/my-folder", "arn:aws:s3:::my-bucket/my-folder"),
        # Test without s3:// prefix (should not modify the path)
        ("my-bucket/my-folder", "my-bucket/my-folder"),
        # Test with trailing slash
        ("s3://my-bucket/my-folder/", "arn:aws:s3:::my-bucket/my-folder"),
        # Test with bucket name only
        ("s3://my-bucket", "arn:aws:s3:::my-bucket"),
        # Test with bucket name and trailing slash
        ("s3://my-bucket/", "arn:aws:s3:::my-bucket"),
        # Test with multiple folders
        (
            "s3://my-bucket/folder1/folder2/folder3",
            "arn:aws:s3:::my-bucket/folder1/folder2/folder3",
        ),
        # Test with empty string
        ("", ""),
        # Test with already ARN format
        (
            "arn:aws:s3:::my-bucket/my-folder",
            "arn:aws:s3:::my-bucket/my-folder",
        ),
    ],
)
def test_get_s3_in_arn(s3_path, expected):
    """Test _get_s3_in_arn with various input formats."""
    result = _get_s3_in_arn(s3_path)
    assert result == expected


@pytest.mark.parametrize(
    "arn",
    ["test/", "arn:aws:s3:::andy-networkx-test/"],
)
def test_validate_arns(arn):
    with pytest.raises(ValueError, match="Invalid ARN"):
        IamClient._validate_arns(arn)
