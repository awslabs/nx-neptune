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
from unittest.mock import MagicMock
from resources_management.clients import IamClient


@pytest.mark.parametrize(
    "mock_response,expected_result",
    [
        # Test case 1: Role name with single service that matches
        (
            {
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
            },
            True,
        ),
        # Test case 2: Mis-match service name
        (
            {
                "Role": {
                    "AssumeRolePolicyDocument": {
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": "sts:AssumeRole",
                                "Principal": {"Service": "s3.amazonaws.com"},
                            }
                        ]
                    }
                }
            },
            False,
        ),
        # Test case 3: Empty statement
        ({"Role": {"AssumeRolePolicyDocument": {"Statement": []}}}, False),
    ],
)
def test_check_assume_role_content(mock_response, expected_result):

    # Create a mock IAM client
    mock_iam_client = MagicMock()
    mock_iam_client.get_role.return_value = mock_response
    logger = MagicMock()

    iam_client = IamClient(
        "arn:aws:iam::123456789012:role/test-role", mock_iam_client, logger
    )
    result = iam_client.check_assume_role("neptune-graph")
    # Verify the result matches expected
    assert result == expected_result


@pytest.mark.parametrize(
    "mock_response,error_message",
    [
        ({"Role": {"RoleName": "test-role"}}, "Unexpected response structure:"),
        ({"Role": {}}, "Unexpected response structure:"),
        ({"SomeOtherKey": {}}, "Unexpected response structure:"),
        ({}, "Unexpected response structure:"),
        (
            {"Role": {"AssumeRolePolicyDocument": {"Version": "2012-10-17"}}},
            "Unexpected response structure:",
        ),
        (
            {"Role": {"AssumeRolePolicyDocument": {"Statement": None}}},
            "Unexpected response structure:",
        ),
    ],
)
def test_check_assume_role_invalid_json(mock_response, error_message):
    """Test that check_assume_role throws ValueError when JSON structure is invalid"""
    # Create a mock IAM client
    mock_iam_client = MagicMock()

    # Set the mock response
    mock_iam_client.get_role.return_value = mock_response

    logger = MagicMock()

    # Test with invalid JSON structure
    with pytest.raises(ValueError, match=error_message):
        iam_client = IamClient(
            "arn:aws:iam::123456789012:role/test-role", mock_iam_client, logger
        )
        iam_client.check_assume_role("neptune-graph")


def test_check_assume_role_invalid_arn():
    """Test the happy path for check_assume_role method with parameterized inputs"""

    # Create a mock IAM client
    mock_iam_client = MagicMock()

    # Test with the parametrized ARNs
    with pytest.raises(ValueError, match="Invalid ARN format"):
        iam_client = IamClient("invalid_ar", None, mock_iam_client)
        iam_client.check_assume_role("neptune-graph")
