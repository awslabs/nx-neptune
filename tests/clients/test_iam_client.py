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


class TestIamClient:
    """Tests for IamClient class methods."""

    @pytest.fixture
    def mock_iam_client(self):
        """Create a mock IamClient for testing."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        iam_client = IamClient(
            role_arn="arn:aws:iam::123456789012:role/test-role", client=mock_client
        )
        return iam_client, mock_client

    def test_iam_client_init(self):
        """Test IamClient initialization."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        iam_client = IamClient(
            role_arn="arn:aws:iam::123456789012:role/test-role", client=mock_client
        )
        assert iam_client.role_arn == "arn:aws:iam::123456789012:role/test-role"
        assert iam_client.client == mock_client

    def test_check_assume_role_success(self, mock_iam_client):
        """Test check_assume_role with valid service."""
        iam_client, mock_client = mock_iam_client

        mock_client.get_role.return_value = {
            "Role": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "neptune-graph.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ]
                }
            }
        }

        result = iam_client.check_assume_role("neptune-graph")
        assert result is True

    def test_check_assume_role_failure(self, mock_iam_client):
        """Test check_assume_role with invalid service."""
        iam_client, mock_client = mock_iam_client

        mock_client.get_role.return_value = {
            "Role": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "other-service.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ]
                }
            }
        }

        result = iam_client.check_assume_role("neptune-graph.amazonaws.com")
        assert result is False

    def test_has_create_na_permissions_success(self, mock_iam_client):
        """Test has_create_na_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:CreateGraph",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:TagResource",
                    "EvalDecision": "allowed",
                },
            ]
        }

        # Should not raise exception
        iam_client.has_create_na_permissions()

    def test_has_create_na_permissions_failure(self, mock_iam_client):
        """Test has_create_na_permissions with missing permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:CreateGraph",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:TagResource",
                    "EvalDecision": "denied",
                },
            ]
        }

        with pytest.raises(Exception, match="Insufficient permission"):
            iam_client.has_create_na_permissions()

    def test_has_delete_na_permissions_success(self, mock_iam_client):
        """Test has_delete_na_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:DeleteGraph",
                    "EvalDecision": "allowed",
                }
            ]
        }

        # Should not raise exception
        iam_client.has_delete_na_permissions()

    def test_has_start_na_permissions_success(self, mock_iam_client):
        """Test has_start_na_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:StartGraph",
                    "EvalDecision": "allowed",
                }
            ]
        }

        # Should not raise exception
        iam_client.has_start_na_permissions()

    def test_has_stop_na_permissions_success(self, mock_iam_client):
        """Test has_stop_na_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {"EvalActionName": "neptune-graph:StopGraph", "EvalDecision": "allowed"}
            ]
        }

        # Should not raise exception
        iam_client.has_stop_na_permissions()

    def test_has_update_na_permissions_success(self, mock_iam_client):
        """Test has_update_na_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:UpdateGraph",
                    "EvalDecision": "allowed",
                }
            ]
        }

        # Should not raise exception
        iam_client.has_update_na_permissions()

    def test_has_create_na_snapshot_permissions_success(self, mock_iam_client):
        """Test has_create_na_snapshot_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:CreateGraphSnapshot",
                    "EvalDecision": "allowed",
                }
            ]
        }

        # Should not raise exception
        iam_client.has_create_na_snapshot_permissions()

    def test_has_delete_snapshot_permissions_success(self, mock_iam_client):
        """Test has_delete_snapshot_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:DeleteGraphSnapshot",
                    "EvalDecision": "allowed",
                }
            ]
        }

        # Should not raise exception
        iam_client.has_delete_snapshot_permissions()

    def test_has_import_from_s3_permissions_success(self, mock_iam_client):
        """Test has_import_from_s3_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.get_role.return_value = {
            "Role": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "neptune-graph.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ]
                }
            }
        }

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {"EvalActionName": "s3:GetObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:ListBucket", "EvalDecision": "allowed"},
            ]
        }

        # Should not raise exception
        iam_client.has_import_from_s3_permissions("arn:aws:s3:::test-bucket")

    def test_has_export_to_s3_permissions_success(self, mock_iam_client):
        """Test has_export_to_s3_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.get_role.return_value = {
            "Role": {
                "AssumeRolePolicyDocument": {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "neptune-graph.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ]
                }
            }
        }

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {"EvalActionName": "s3:PutObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:ListBucket", "EvalDecision": "allowed"},
            ]
        }

        # Should not raise exception
        iam_client.has_export_to_s3_permissions("arn:aws:s3:::test-bucket")

    def test_check_aws_permission_success(self, mock_iam_client):
        """Test check_aws_permission with allowed permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {"EvalActionName": "s3:GetObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:PutObject", "EvalDecision": "allowed"},
            ]
        }

        result = iam_client.check_aws_permission(
            "test-operation",
            ["s3:GetObject", "s3:PutObject"],
            "arn:aws:s3:::test-bucket",
        )

        assert result == {"s3:GetObject": True, "s3:PutObject": True}

    def test_check_aws_permission_denied(self, mock_iam_client):
        """Test check_aws_permission with denied permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {"EvalActionName": "s3:GetObject", "EvalDecision": "denied"}
            ]
        }

        with pytest.raises(ValueError, match="Insufficient permission"):
            iam_client.check_aws_permission(
                "test-operation", ["s3:GetObject"], "arn:aws:s3:::test-bucket"
            )

    def test_check_aws_permission_wildcard_resource(self, mock_iam_client):
        """Test check_aws_permission with wildcard resource."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:CreateGraph",
                    "EvalDecision": "allowed",
                }
            ]
        }

        result = iam_client.check_aws_permission(
            "test-operation", ["neptune-graph:CreateGraph"], "*"
        )

        assert result == {"neptune-graph:CreateGraph": True}

    def test_check_aws_permission_access_denied(self, mock_iam_client):
        """Test check_aws_permission when IAM permission is missing."""
        iam_client, mock_client = mock_iam_client
        from botocore.exceptions import ClientError

        mock_client.simulate_principal_policy.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
            "SimulatePrincipalPolicy",
        )

        result = iam_client.check_aws_permission(
            "test-operation", ["s3:GetObject"], "arn:aws:s3:::test-bucket"
        )

        assert result == {}

    def test_check_aws_permission_empty_results(self, mock_iam_client):
        """Test check_aws_permission with empty evaluation results."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {"EvaluationResults": []}

        with pytest.raises(ValueError, match="No evaluation results found"):
            iam_client.check_aws_permission(
                "test-operation", ["s3:GetObject"], "arn:aws:s3:::test-bucket"
            )

    def test_check_aws_permission_missing_fields(self, mock_iam_client):
        """Test check_aws_permission with missing result fields."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [{"EvalActionName": "s3:GetObject"}]
        }

        with pytest.raises(ValueError, match="Unexpected result structure"):
            iam_client.check_aws_permission(
                "test-operation", ["s3:GetObject"], "arn:aws:s3:::test-bucket"
            )

    def test_validate_permissions_success(self, mock_iam_client):
        """Test validate_permissions with all permissions allowed."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:CreateGraph",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:TagResource",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:DeleteGraph",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:StartGraph",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:StopGraph",
                    "EvalDecision": "allowed",
                },
                {"EvalActionName": "s3:GetObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:PutObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:ListBucket", "EvalDecision": "allowed"},
                {"EvalActionName": "kms:Decrypt", "EvalDecision": "allowed"},
                {"EvalActionName": "kms:GenerateDataKey", "EvalDecision": "allowed"},
                {"EvalActionName": "kms:DescribeKey", "EvalDecision": "allowed"},
                {
                    "EvalActionName": "athena:StartQueryExecution",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "athena:GetQueryExecution",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:CreateGraphSnapshot",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:RestoreGraphFromSnapshot",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "neptune-graph:DeleteGraphSnapshot",
                    "EvalDecision": "allowed",
                },
            ]
        }

        result = iam_client.validate_permissions(
            "s3://test-import-bucket",
            "arn:aws:kms:us-east-1:123456789012:key/test-key",
            "s3://test-export-bucket",
            "arn:aws:kms:us-east-1:123456789012:key/test-key",
        )

        assert result["create_na_instance"] is True
        assert result["delete_na_instance"] is True
        assert result["start_graph"] is True
        assert result["stop_graph"] is True
        assert result["import_from_s3"] is True
        assert result["export_csv_to_s3"] is True
        assert result["export_athena_table_to_s3"] is True
        assert result["create_csv_table_from_s3"] is True
        assert result["create_graph_snapshot"] is True
        assert result["create_na_instance_from_snapshot"] is True
        assert result["delete_graph_snapshot"] is True

    def test_validate_permissions_partial_failure(self, mock_iam_client):
        """Test validate_permissions with some permissions denied."""
        iam_client, mock_client = mock_iam_client

        def mock_simulate(*args, **kwargs):
            action_names = kwargs.get("ActionNames", [])
            if "neptune-graph:DeleteGraph" in action_names:
                return {
                    "EvaluationResults": [
                        {
                            "EvalActionName": "neptune-graph:DeleteGraph",
                            "EvalDecision": "denied",
                        }
                    ]
                }
            return {
                "EvaluationResults": [
                    {"EvalActionName": action, "EvalDecision": "allowed"}
                    for action in action_names
                ]
            }

        mock_client.simulate_principal_policy.side_effect = mock_simulate

        result = iam_client.validate_permissions(
            "s3://test-import-bucket",
            None,
            "s3://test-export-bucket",
            None,
        )

        assert result["create_na_instance"] is True
        assert result["delete_na_instance"] is False
        assert result["start_graph"] is True
        assert result["stop_graph"] is True

    def test_has_athena_permissions_success(self, mock_iam_client):
        """Test has_athena_permissions with valid permissions."""
        iam_client, mock_client = mock_iam_client

        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "athena:StartQueryExecution",
                    "EvalDecision": "allowed",
                },
                {
                    "EvalActionName": "athena:GetQueryExecution",
                    "EvalDecision": "allowed",
                },
                {"EvalActionName": "s3:GetObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:PutObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:ListBucket", "EvalDecision": "allowed"},
                {"EvalActionName": "kms:Decrypt", "EvalDecision": "allowed"},
                {"EvalActionName": "kms:GenerateDataKey", "EvalDecision": "allowed"},
                {"EvalActionName": "kms:DescribeKey", "EvalDecision": "allowed"},
            ]
        }

        # Should not raise exception
        result = iam_client.has_athena_permissions(
            "s3://test-bucket/", "arn:aws:kms:us-east-1:123456789012:key/test-key"
        )
        assert result is True
