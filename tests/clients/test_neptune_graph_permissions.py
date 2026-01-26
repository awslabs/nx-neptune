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
    "role_arn,resource_arn",
    [
        # Invalid role ARN, valid resource ARN
        ("not-an-arn", "arn:aws:neptune-graph:us-east-1:123456789012:graph/my-graph"),
        # Valid role ARN, invalid resource ARN
        ("arn:aws:iam::123456789012:role/test-role", "not-an-arn"),
        # Both invalid
        ("not-an-arn", "also-not-an-arn"),
    ],
)
def test_aws_permission_check_invalid_arn(role_arn, resource_arn):
    """Test that aws_permission_check throws a ValueError when invalid ARNs are provided"""
    # Create a mock IAM client
    mock_iam_client = MagicMock()
    logger = MagicMock()
    iam_client = IamClient(role_arn, mock_iam_client, logger)

    # Test with the parametrized ARNs
    with pytest.raises(ValueError, match="Invalid ARN format"):
        iam_client = IamClient(role_arn, mock_iam_client, logger)
        iam_client.check_aws_permission(
            "Test operation", ["neptune-graph:ReadDataViaQuery"], resource_arn
        )


@pytest.mark.parametrize(
    "mock_response",
    [
        # Test case 1: Missing EvaluationResults field
        {"SomeOtherField": "value"},
        # Test case 2.1: EvaluationResults exists but with malformed entries
        {"EvaluationResults": [{"EvalDecision": "allowed"}]},
        # Test case 2.2 Missing EvalDecision
        {"EvaluationResults": [{"EvalActionName": "neptune-graph:ReadDataViaQuery"}]},
        # Test case 2.3 Both fields missing
        {"EvaluationResults": [{}]},
        # Test case 3: Empty EvaluationResults
        {"EvaluationResults": []},
        # Test case 4: None EvaluationResults
        {"EvaluationResults": None},
    ],
)
def test_aws_permission_check_malformed_response(mock_response):
    """Test that aws_permission_check throws a ValueError when the API returns unexpected JSON structure"""
    # Ensure ARN validation passes
    # mock_validate_arns.return_value = True

    # Create a mock IAM client that returns the parameterized malformed response
    mock_iam_client = MagicMock()
    mock_iam_client.simulate_principal_policy.return_value = mock_response
    logger = MagicMock()

    with pytest.raises(ValueError, match="Unexpected result structure"):
        iam_client = IamClient(
            "arn:aws:iam::123456789012:role/test-role", mock_iam_client, logger
        )
        iam_client.check_aws_permission(
            ["neptune-graph:ReadDataViaQuery"],
            "arn:aws:neptune-graph:us-east-1:123456789012:graph/my-graph",
        )
