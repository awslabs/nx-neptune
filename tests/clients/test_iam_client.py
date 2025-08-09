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
import pytest

from nx_neptune.clients.iam_client import (
    IamClient,
    _get_s3_in_arn,
    convert_sts_to_iam_arn,
)


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


@pytest.mark.parametrize(
    "arn, expected",
    [
        (
            "arn:aws:sts::ACCOUNT_ID:assumed-role/ROLE_NAME/SESSION_NAME",
            "arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME",
        )
    ],
)
def test_sts_to_iam_arn(arn, expected):
    result = convert_sts_to_iam_arn(arn)
    assert result == expected


def test_sts_to_iam_arn_with_invalid_str():
    with pytest.raises(ValueError, match="Input is not a valid STS assumed-role ARN"):
        convert_sts_to_iam_arn(
            "INVALID_PREFIX::ACCOUNT_ID:assumed-role/ROLE_NAME/SESSION_NAME"
        )
