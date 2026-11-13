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
import logging
import os
from fnmatch import fnmatchcase
from unittest.mock import MagicMock

import boto3
import pytest

from nx_neptune.clients.iam_client import (
    IamClientWrapper,
    _get_s3_in_arn,
    _convert_sts_to_iam_arn,
    split_s3_arn_to_bucket_and_path,
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
    """Test _get_s3_in_arn with various input formats (default partition: aws)."""
    result = _get_s3_in_arn(s3_path)
    assert result == expected


@pytest.mark.parametrize(
    "s3_path,partition,expected",
    [
        (
            "s3://my-bucket/my-folder/",
            "aws-us-gov",
            "arn:aws-us-gov:s3:::my-bucket/my-folder",
        ),
        ("s3://my-bucket", "aws-cn", "arn:aws-cn:s3:::my-bucket"),
        ("s3://my-bucket/my-folder", "aws", "arn:aws:s3:::my-bucket/my-folder"),
    ],
)
def test_get_s3_in_arn_uses_given_partition(s3_path, partition, expected):
    """_get_s3_in_arn takes an optional ``partition`` keyword (default ``aws``)."""
    assert _get_s3_in_arn(s3_path, partition=partition) == expected


@pytest.mark.parametrize(
    "arn",
    ["test/", "arn:aws:s3:::andy-networkx-test/"],
)
def test_validate_arns(arn):
    with pytest.raises(ValueError, match="Invalid ARN"):
        IamClientWrapper._validate_arns(arn)


@pytest.mark.parametrize(
    "arn, expected",
    [
        (
            "arn:aws:sts::ACCOUNT_ID:assumed-role/ROLE_NAME/SESSION_NAME",
            "arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME",
        ),
        # Test role with path (fixes issue #83)
        (
            "arn:aws:sts::123456789012:assumed-role/sub-role/MyRole-RoleType/session-name",
            "arn:aws:iam::123456789012:role/sub-role/MyRole-RoleType",
        ),
        # Test role with deeper path
        (
            "arn:aws:sts::123456789012:assumed-role/path/to/MyRole/session-name",
            "arn:aws:iam::123456789012:role/path/to/MyRole",
        ),
        # Non-commercial partitions keep their own partition
        (
            "arn:aws-cn:sts::123456789012:assumed-role/MyRole/session",
            "arn:aws-cn:iam::123456789012:role/MyRole",
        ),
        (
            "arn:aws-us-gov:sts::123456789012:assumed-role/path/MyRole/session",
            "arn:aws-us-gov:iam::123456789012:role/path/MyRole",
        ),
    ],
)
def test_sts_to_iam_arn(arn, expected):
    result = _convert_sts_to_iam_arn(arn)
    assert result == expected


def test_sts_to_iam_arn_with_invalid_str():
    with pytest.raises(ValueError, match="Input is not a valid STS assumed-role ARN"):
        _convert_sts_to_iam_arn(
            "INVALID_PREFIX::ACCOUNT_ID:assumed-role/ROLE_NAME/SESSION_NAME"
        )


@pytest.mark.parametrize(
    "s3_arn,expected_bucket,expected_path",
    [
        ("s3://my-bucket/my-folder", "my-bucket", "my-folder"),
        ("s3://my-bucket", "my-bucket", ""),
        ("s3://my-bucket/", "my-bucket", ""),
        (
            "s3://my-bucket/folder1/folder2/folder3",
            "my-bucket",
            "folder1/folder2/folder3",
        ),
        (
            "s3://test-bucket-name/path/to/file.txt",
            "test-bucket-name",
            "path/to/file.txt",
        ),
        (
            "s3://bucket-with-hyphens/deep/nested/path/",
            "bucket-with-hyphens",
            "deep/nested/path/",
        ),
    ],
)
def test_split_s3_arn_to_bucket_and_path(s3_arn, expected_bucket, expected_path):
    """Test split_s3_arn_to_bucket_and_path with various S3 ARN formats."""
    bucket, path = split_s3_arn_to_bucket_and_path(s3_arn)
    assert bucket == expected_bucket
    assert path == expected_path


class TestIamClientWrapper:
    """Tests for IamClientWrapper class methods."""

    @pytest.fixture
    def mock_iam_client(self):
        """Create a mock IamClientWrapper for testing."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        # A real IAM client reports the pseudo-region, never a usable one.
        mock_client.meta.region_name = "aws-global"
        iam_client = IamClientWrapper(
            role_arn="arn:aws:iam::123456789012:role/test-role", client=mock_client
        )
        return iam_client, mock_client

    def test_iam_client_init(self):
        """Test IamClientWrapper initialization."""
        from unittest.mock import MagicMock

        mock_client = MagicMock()
        iam_client = IamClientWrapper(
            role_arn="arn:aws:iam::123456789012:role/test-role", client=mock_client
        )
        assert iam_client.role_arn == "arn:aws:iam::123456789012:role/test-role"
        assert iam_client.client == mock_client

    def test_default_resolves_caller_identity(self):
        """default() builds a wrapper from the factory's STS identity + IAM client."""
        from unittest.mock import patch, MagicMock

        mock_iam = MagicMock()
        with patch("nx_neptune.clients.iam_client.ClientFactory") as mock_factory_cls:
            factory = mock_factory_cls.return_value
            factory.sts.return_value.get_caller_identity.return_value = {
                "Arn": "arn:aws:iam::123456789012:role/caller-role"
            }
            factory.iam.return_value = mock_iam

            wrapper = IamClientWrapper.default()

        assert isinstance(wrapper, IamClientWrapper)
        assert wrapper.role_arn == "arn:aws:iam::123456789012:role/caller-role"
        assert wrapper.client is mock_iam

    def test_default_uses_injected_clients(self):
        """default() uses provided STS/IAM clients and logger instead of the factory."""
        from unittest.mock import patch, MagicMock

        sts = MagicMock()
        sts.get_caller_identity.return_value = {
            "Arn": "arn:aws:iam::123456789012:role/injected-role"
        }
        iam = MagicMock()
        logger = logging.getLogger("injected")
        with patch("nx_neptune.clients.iam_client.ClientFactory") as mock_factory_cls:
            wrapper = IamClientWrapper.default(
                sts_client=sts, iam_client=iam, logger=logger
            )

        assert wrapper.role_arn == "arn:aws:iam::123456789012:role/injected-role"
        assert wrapper.client is iam
        assert wrapper.logger is logger
        mock_factory_cls.return_value.sts.assert_not_called()
        mock_factory_cls.return_value.iam.assert_not_called()

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
        """Test has_export_to_s3_permissions with valid permissions and versioning enabled."""
        from unittest.mock import patch, MagicMock

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

        mock_s3 = MagicMock()
        mock_s3.get_bucket_versioning.return_value = {"Status": "Enabled"}
        with patch.object(iam_client, "check_s3_versioning_enabled"):
            iam_client.has_export_to_s3_permissions("arn:aws:s3:::test-bucket")

    def test_has_export_to_s3_permissions_versioning_disabled(self, mock_iam_client):
        """Test has_export_to_s3_permissions raises when versioning is not enabled."""
        from unittest.mock import patch, MagicMock

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

        mock_s3 = MagicMock()
        mock_s3.get_bucket_versioning.return_value = {"Status": "Suspended"}
        with patch.object(
            iam_client,
            "check_s3_versioning_enabled",
            side_effect=ValueError("does not have versioning enabled"),
        ):
            with pytest.raises(ValueError, match="does not have versioning enabled"):
                iam_client.has_export_to_s3_permissions("arn:aws:s3:::test-bucket")

    def test_check_s3_versioning_enabled_with_mock_client(self, mock_iam_client):
        """Test check_s3_versioning_enabled with injected mock S3 client."""
        from unittest.mock import MagicMock

        iam_client, _ = mock_iam_client
        mock_s3 = MagicMock()

        mock_s3.get_bucket_versioning.return_value = {"Status": "Enabled"}
        iam_client.check_s3_versioning_enabled("s3://test-bucket/", s3_client=mock_s3)

        mock_s3.get_bucket_versioning.return_value = {"Status": "Suspended"}
        with pytest.raises(ValueError, match="does not have versioning enabled"):
            iam_client.check_s3_versioning_enabled(
                "s3://test-bucket/", s3_client=mock_s3
            )

        mock_s3.get_bucket_versioning.return_value = {}
        with pytest.raises(ValueError, match="does not have versioning enabled"):
            iam_client.check_s3_versioning_enabled(
                "s3://test-bucket/", s3_client=mock_s3
            )

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
        from unittest.mock import patch, MagicMock

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

        mock_s3 = MagicMock()
        mock_s3.get_bucket_versioning.return_value = {"Status": "Enabled"}
        with patch.object(iam_client, "check_s3_versioning_enabled"):
            result = iam_client.has_athena_permissions(
                "s3://test-bucket/", "arn:aws:kms:us-east-1:123456789012:key/test-key"
            )
            assert result is True


# ---------------------------------------------------------------------------
# Least-privilege preflight simulation
#
# The tests below drive the wrapper against a small in-memory policy simulator
# instead of canned "allowed" responses, so they fail when the *request* the
# wrapper builds (resource ARN, context entries) would not match a realistic,
# least-privilege policy -- not merely when the response parsing changes.
# ---------------------------------------------------------------------------

ACCOUNT = "123456789012"
REGION = "us-east-1"
IAM_PSEUDO_REGION = "aws-global"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/test-role"
AGENT_VALUE = "nx-neptune"
TRUST_NA = {
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


def allow(actions, resources, condition=None):
    stmt = {"Effect": "Allow", "Action": actions, "Resource": resources}
    if condition:
        stmt["Condition"] = condition
    return stmt


def deny(actions, resources):
    return {"Effect": "Deny", "Action": actions, "Resource": resources}


ALLOW_ALL = [allow(["*"], ["*"])]


class FakePolicySimulator:
    """Minimal ``iam.simulate_principal_policy`` over Allow/Deny statements.

    Supports ``*`` globs in Action/Resource and the ``StringEquals``,
    ``StringNotEquals`` and ``StringLike`` condition operators, evaluated against
    the request's ``ContextEntries``. Missing-key semantics follow the real
    service, per operator (verified against live IAM):

    * ``StringEquals`` / ``StringLike`` with the key missing: the condition does
      not match, so the statement does not apply (an Allow yields ``implicitDeny``).
    * ``StringNotEquals`` with the key missing: the condition DOES match, so the
      statement applies (a Deny yields ``explicitDeny``).

    Records every call in ``calls``.
    """

    def __init__(self, statements):
        self.statements = statements
        self.calls = []
        self.decisions = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        context = {
            e["ContextKeyName"]: e["ContextKeyValues"]
            for e in kwargs.get("ContextEntries", [])
        }
        results = []
        for action in kwargs["ActionNames"]:
            for resource in kwargs["ResourceArns"]:
                decision = self._decide(action, resource, context)
                self.decisions.append((action, decision))
                results.append(
                    {
                        "EvalActionName": action,
                        "EvalResourceName": resource,
                        "EvalDecision": decision,
                    }
                )
        return {"EvaluationResults": results}

    def _decide(self, action, resource, context):
        decision = "implicitDeny"
        for stmt in self.statements:
            if not any(fnmatchcase(action, a) for a in stmt["Action"]):
                continue
            if not any(fnmatchcase(resource, r) for r in stmt["Resource"]):
                continue
            if not self._conditions_hold(stmt.get("Condition", {}), context):
                continue
            if stmt["Effect"] == "Deny":
                return "explicitDeny"
            decision = "allowed"
        return decision

    @staticmethod
    def _conditions_hold(conditions, context):
        for operator, pairs in conditions.items():
            for key, expected in pairs.items():
                expected = [expected] if isinstance(expected, str) else list(expected)
                actual = context.get(key)  # None means the key is missing
                if operator == "StringEquals":
                    holds = actual is not None and any(a in expected for a in actual)
                elif operator == "StringNotEquals":
                    # A missing key makes a negated operator match.
                    holds = actual is None or not any(a in expected for a in actual)
                elif operator == "StringLike":
                    holds = actual is not None and any(
                        fnmatchcase(a, e) for a in actual for e in expected
                    )
                else:
                    raise NotImplementedError(operator)
                if not holds:
                    return False
        return True

    def resources_by_action(self):
        out = {}
        for call in self.calls:
            for action in call["ActionNames"]:
                out.setdefault(action, set()).update(call["ResourceArns"])
        return out

    def calls_for(self, prefix):
        return [
            c for c in self.calls if any(a.startswith(prefix) for a in c["ActionNames"])
        ]


def make_wrapper(
    statements=None,
    role_arn=ROLE_ARN,
    region=REGION,
    simulator=None,
    **kwargs,
):
    client = MagicMock()
    # Real IAM clients report a pseudo-region; the wrapper must not rely on it.
    client.meta.region_name = IAM_PSEUDO_REGION
    client.get_role.return_value = TRUST_NA
    sim = simulator or FakePolicySimulator(
        ALLOW_ALL if statements is None else statements
    )
    client.simulate_principal_policy.side_effect = sim
    logger = MagicMock()
    wrapper = IamClientWrapper(role_arn, client, logger=logger, region=region, **kwargs)
    return wrapper, sim, logger


def assert_clean(sim, logger):
    """Every simulated decision was ``allowed`` and nothing was warned about.

    "Does not raise" is not enough: an implicit deny only warns, so it must be
    ruled out explicitly or a wrong simulation target passes unnoticed.
    """
    assert sim.decisions
    assert all(d == "allowed" for _, d in sim.decisions), sim.decisions
    assert not logger.warning.called, logger.warning.call_args_list


def no_versioning(wrapper):
    wrapper.check_s3_versioning_enabled = MagicMock()
    return wrapper


GRAPH_ARN = f"arn:aws:neptune-graph:{REGION}:{ACCOUNT}:graph/*"
SNAPSHOT_RESOURCE_ARN = f"arn:aws:neptune-graph:{REGION}:{ACCOUNT}:graph-snapshot/*"
ARN_FOR_TYPE = {"graph": GRAPH_ARN, "graph-snapshot": SNAPSHOT_RESOURCE_ARN}

# Valid resource types per action, from the AWS Service Authorization Reference
# (servicereference.us-east-1.amazonaws.com/v1/neptune-graph/neptune-graph.json).
# Kept as a literal here on purpose: the tests must not read the production table.
NA_RESOURCE_TYPES = {
    "neptune-graph:CreateGraph": ("graph",),
    "neptune-graph:StartGraph": ("graph",),
    "neptune-graph:StopGraph": ("graph",),
    "neptune-graph:DeleteGraph": ("graph",),
    "neptune-graph:UpdateGraph": ("graph",),
    "neptune-graph:DeleteGraphSnapshot": ("graph-snapshot",),
    "neptune-graph:CreateGraphSnapshot": ("graph", "graph-snapshot"),
    "neptune-graph:RestoreGraphFromSnapshot": ("graph", "graph-snapshot"),
    "neptune-graph:TagResource": ("graph", "graph-snapshot"),
}
DUAL_TYPE_ACTIONS = [a for a, t in NA_RESOURCE_TYPES.items() if len(t) == 2]
SINGLE_TYPE_ACTIONS = [a for a, t in NA_RESOURCE_TYPES.items() if len(t) == 1]


def valid_neptune_arns(action):
    """Every resource ARN the action may legitimately be simulated against."""
    return {ARN_FOR_TYPE[t] for t in NA_RESOURCE_TYPES[action]}


def assert_each_action_on_its_resource_type(calls):
    """Every simulated Neptune action targeted only ARNs valid for it, and all of them.

    Per request, each ARN must be valid for every action in that request. Across
    requests, each action must be simulated against its full set of valid ARNs,
    so a policy granting it on either resource type can be confirmed.
    """
    assert calls
    seen = {}
    for call in calls:
        for action in call["ActionNames"]:
            assert set(call["ResourceArns"]) <= valid_neptune_arns(action), (
                action,
                call["ResourceArns"],
            )
            seen.setdefault(action, set()).update(call["ResourceArns"])
    for action, arns in seen.items():
        assert arns == valid_neptune_arns(action), (action, arns)


def neptune_outcome(statements, action):
    """Run one Neptune action through the preflight; return (result, logger)."""
    wrapper, _, logger = make_wrapper(statements)
    return wrapper._check_neptune_permission("op", [action]), logger


def assert_confirmed(statements, action):
    result, logger = neptune_outcome(statements, action)
    assert result == {action: True}, result
    assert not logger.warning.called, logger.warning.call_args_list


def assert_unconfirmed(statements, action):
    result, logger = neptune_outcome(statements, action)
    assert result == {action: False}, result
    assert any(
        action in " ".join(str(a) for a in c.args)
        for c in logger.warning.call_args_list
    ), logger.warning.call_args_list


NEPTUNE_METHODS = [
    "has_create_na_permissions",
    "has_start_na_permissions",
    "has_stop_na_permissions",
    "has_delete_na_permissions",
    "has_update_na_permissions",
    "has_delete_snapshot_permissions",
    "has_create_na_from_snapshot_permissions",
    "has_create_na_snapshot_permissions",
]

# Reported reproduction: grant scoped to the account's Neptune Analytics
# resources, plus CreateGraph on "*".
ACCOUNT_SCOPED_NEPTUNE = [
    allow(["neptune-graph:*"], [f"arn:aws:neptune-graph:{REGION}:{ACCOUNT}:*"]),
    allow(["neptune-graph:CreateGraph"], ["*"]),
]

# Reported reproduction: TagResource only when the request carries agent=nx-neptune.
TAG_CONDITIONED_NEPTUNE = [
    allow(["neptune-graph:CreateGraph"], ["*"]),
    allow(
        ["neptune-graph:TagResource"],
        [GRAPH_ARN],
        {"StringEquals": {"aws:RequestTag/agent": AGENT_VALUE}},
    ),
]
# TagResource applies to graphs and graph snapshots; grant both so every simulated
# decision is "allowed" regardless of which resource type is evaluated.
TAG_CONDITIONED_NEPTUNE[1]["Resource"].append(
    f"arn:aws:neptune-graph:{REGION}:{ACCOUNT}:graph-snapshot/*"
)


class TestNeptuneSimulationTarget:
    """Requirement: Neptune actions are simulated against a graph ARN with tag context."""

    @pytest.mark.parametrize("method", NEPTUNE_METHODS)
    def test_account_scoped_grant_is_not_rejected(self, method):
        wrapper, sim, logger = make_wrapper(ACCOUNT_SCOPED_NEPTUNE)
        getattr(wrapper, method)()
        assert_clean(sim, logger)

    def test_create_simulates_against_account_graph_arn(self):
        wrapper, sim, _ = make_wrapper(ACCOUNT_SCOPED_NEPTUNE)
        wrapper.has_create_na_permissions()
        assert sim.resources_by_action()["neptune-graph:CreateGraph"] == {GRAPH_ARN}
        assert GRAPH_ARN in sim.resources_by_action()["neptune-graph:TagResource"]
        assert_each_action_on_its_resource_type(sim.calls)

    @pytest.mark.parametrize("method", NEPTUNE_METHODS)
    def test_every_neptune_check_uses_graph_arn(self, method):
        wrapper, sim, _ = make_wrapper()
        getattr(wrapper, method)()
        assert_each_action_on_its_resource_type(sim.calls)

    @pytest.mark.parametrize("method", NEPTUNE_METHODS)
    def test_every_neptune_check_sends_tag_and_region_context(self, method):
        wrapper, sim, _ = make_wrapper()
        getattr(wrapper, method)()
        for call in sim.calls:
            entries = {e["ContextKeyName"]: e for e in call["ContextEntries"]}
            assert set(entries) >= {
                "aws:RequestTag/agent",
                "aws:ResourceTag/agent",
                "aws:RequestedRegion",
            }
            assert entries["aws:RequestTag/agent"]["ContextKeyValues"] == [AGENT_VALUE]
            assert entries["aws:ResourceTag/agent"]["ContextKeyValues"] == [AGENT_VALUE]
            assert entries["aws:RequestedRegion"]["ContextKeyValues"] == [REGION]
            assert all(e["ContextKeyType"] == "string" for e in entries.values())

    def test_tag_conditioned_tag_resource_is_not_rejected(self):
        wrapper, sim, logger = make_wrapper(TAG_CONDITIONED_NEPTUNE)
        wrapper.has_create_na_permissions()
        assert_clean(sim, logger)

    def test_resource_tag_conditioned_grant_is_not_rejected(self):
        wrapper, sim, logger = make_wrapper(
            [
                allow(
                    ["neptune-graph:StartGraph"],
                    [GRAPH_ARN],
                    {"StringEquals": {"aws:ResourceTag/agent": AGENT_VALUE}},
                )
            ]
        )
        wrapper.has_start_na_permissions()
        assert_clean(sim, logger)

    def test_requested_region_conditioned_grant_is_not_rejected(self):
        wrapper, sim, logger = make_wrapper(
            [
                allow(
                    ["neptune-graph:*"],
                    [GRAPH_ARN, "*"],
                    {"StringEquals": {"aws:RequestedRegion": REGION}},
                )
            ]
        )
        wrapper.has_create_na_permissions()
        assert_clean(sim, logger)

    def test_validate_permissions_neptune_entries_use_graph_arn_and_context(self):
        wrapper, sim, _ = make_wrapper()
        wrapper.validate_permissions(None, None, None, None)
        neptune_calls = sim.calls_for("neptune-graph:")
        assert neptune_calls
        assert_each_action_on_its_resource_type(neptune_calls)
        for call in neptune_calls:
            names = {e["ContextKeyName"] for e in call["ContextEntries"]}
            assert "aws:RequestTag/agent" in names

    def test_kms_simulation_keeps_its_own_resource_and_sends_no_context(self):
        # Guard (passes against the pre-fix code except for the new region= keyword; pins behaviour the fix must not break):
        # protects non-Neptune callers.
        key = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/abc"
        wrapper, sim, _ = make_wrapper()
        wrapper.has_import_from_s3_permissions("s3://my-bucket/data/", key)
        kms_calls = sim.calls_for("kms:")
        assert len(kms_calls) == 1
        assert kms_calls[0]["ResourceArns"] == [key]
        assert not kms_calls[0].get("ContextEntries")
        assert not any(
            c["ResourceArns"] == [GRAPH_ARN] for c in sim.calls
        ), "no Neptune graph ARN may be sent for non-Neptune simulations"

    def test_s3_simulations_send_no_tag_context(self):
        # Guard (passes against the pre-fix code except for the new region= keyword; pins behaviour the fix must not break).
        wrapper, sim, _ = make_wrapper()
        wrapper.has_import_from_s3_permissions("s3://my-bucket/data/")
        assert not any(c.get("ContextEntries") for c in sim.calls_for("s3:"))

    def test_check_aws_permission_forwards_context_entries(self):
        """Pinned API: ``context_entries`` keyword on ``check_aws_permission``."""
        wrapper, sim, _ = make_wrapper()
        entries = [
            {
                "ContextKeyName": "aws:RequestTag/agent",
                "ContextKeyValues": ["x"],
                "ContextKeyType": "string",
            }
        ]
        wrapper.check_aws_permission(
            "op", ["neptune-graph:TagResource"], "*", context_entries=entries
        )
        assert sim.calls[0]["ContextEntries"] == entries

    def test_check_aws_permission_without_context_keeps_default_resource(self):
        # Guard (passes against the pre-fix code except for the new region= keyword; pins behaviour the fix must not break):
        # the public "*" default must stay.
        wrapper, sim, _ = make_wrapper()
        wrapper.check_aws_permission("op", ["athena:StartQueryExecution"])
        assert sim.calls[0]["ResourceArns"] == ["*"]
        assert not sim.calls[0].get("ContextEntries")


class TestRegionResolution:
    """Requirement: the region comes from the caller, never from the IAM client.

    IAM is a global service: a real IAM client reports ``aws-global``
    (``aws-us-gov-global``, ``aws-cn-global``) as ``meta.region_name`` whatever
    region is configured, so that value can never match a policy.
    """

    @pytest.fixture
    def aws_env(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        monkeypatch.delenv("AWS_PROFILE", raising=False)

    @staticmethod
    def real_iam_client(region):
        client = boto3.Session(region_name=region).client("iam")
        sim = FakePolicySimulator(ALLOW_ALL)
        client.simulate_principal_policy = MagicMock(side_effect=sim)
        return client, sim

    @staticmethod
    def sent_regions(sim):
        """Every region string the wrapper put into an ARN or a context entry."""
        regions = set()
        for call in sim.calls:
            regions.update(a.split(":")[3] for a in call["ResourceArns"])
            for e in call["ContextEntries"]:
                if e["ContextKeyName"] == "aws:RequestedRegion":
                    regions.update(e["ContextKeyValues"])
        return regions

    @pytest.mark.parametrize(
        "partition,region,pseudo",
        [
            ("aws", "eu-west-1", "aws-global"),
            ("aws", "us-east-1", "aws-global"),
            ("aws-us-gov", "us-gov-west-1", "aws-us-gov-global"),
            ("aws-cn", "cn-north-1", "aws-cn-global"),
        ],
    )
    def test_region_is_not_taken_from_iam_client_meta(
        self, aws_env, partition, region, pseudo
    ):
        client, sim = self.real_iam_client(region)
        # Premise, with a real client: IAM never reports the configured region.
        assert client.meta.region_name == pseudo
        role = f"arn:{partition}:iam::{ACCOUNT}:role/test-role"
        wrapper = IamClientWrapper(role, client, logger=MagicMock())
        wrapper.has_create_na_permissions()
        sent = self.sent_regions(sim)
        assert sent, "expected a Neptune simulation"
        assert pseudo not in sent, sent
        assert not any("global" in r for r in sent), sent

    def test_region_keyword_feeds_graph_arn_with_real_iam_client(self, aws_env):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break); the region keyword already existed.
        client, sim = self.real_iam_client("us-east-1")
        wrapper = IamClientWrapper(
            ROLE_ARN, client, logger=MagicMock(), region="eu-west-1"
        )
        wrapper.has_create_na_permissions()
        assert self.sent_regions(sim) == {"eu-west-1"}

    def test_real_iam_client_satisfies_region_conditioned_grant(self, aws_env):
        # End to end with a real IAM client: a policy keyed on the real region
        # is only satisfied when the real region is what gets simulated.
        client, _ = self.real_iam_client("eu-west-1")
        grant = FakePolicySimulator(
            [
                allow(
                    ["neptune-graph:*"],
                    [
                        f"arn:aws:neptune-graph:eu-west-1:{ACCOUNT}:graph/*",
                        f"arn:aws:neptune-graph:eu-west-1:{ACCOUNT}:graph-snapshot/*",
                    ],
                    {"StringEquals": {"aws:RequestedRegion": "eu-west-1"}},
                )
            ]
        )
        client.simulate_principal_policy = MagicMock(side_effect=grant)
        logger = MagicMock()
        wrapper = IamClientWrapper(ROLE_ARN, client, logger=logger, region="eu-west-1")
        wrapper.has_create_na_permissions()
        assert_clean(grant, logger)

    def test_default_accepts_region_for_real_iam_client(self, aws_env):
        """Pinned API: ``IamClientWrapper.default(..., region=...)``."""
        client, sim = self.real_iam_client("eu-west-1")
        sts = MagicMock()
        sts.get_caller_identity.return_value = {"Arn": ROLE_ARN}
        wrapper = IamClientWrapper.default(
            sts_client=sts, iam_client=client, region="eu-west-1"
        )
        wrapper.has_create_na_permissions()
        assert self.sent_regions(sim) == {"eu-west-1"}

    def test_region_keyword_wins_over_iam_client_region(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break).
        wrapper, sim, _ = make_wrapper(region="ap-southeast-2")
        wrapper.client.meta.region_name = "us-east-1"
        wrapper.has_create_na_permissions()
        assert all(
            c["ResourceArns"][0].split(":")[3] == "ap-southeast-2" for c in sim.calls
        )

    def test_region_keyword_feeds_graph_arn(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break); the region keyword is new.
        wrapper, _, _ = make_wrapper(region="eu-west-1")
        assert wrapper._neptune_graph_arn() == (
            f"arn:aws:neptune-graph:eu-west-1:{ACCOUNT}:graph/*"
        )

    def test_explicit_region_keyword_feeds_requested_region_context(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break); the region keyword is new.
        client = MagicMock()
        client.meta.region_name = IAM_PSEUDO_REGION
        sim = FakePolicySimulator(ALLOW_ALL)
        client.simulate_principal_policy.side_effect = sim
        wrapper = IamClientWrapper(ROLE_ARN, client, region="ap-southeast-2")
        wrapper.has_start_na_permissions()
        entries = {e["ContextKeyName"]: e for e in sim.calls[0]["ContextEntries"]}
        assert entries["aws:RequestedRegion"]["ContextKeyValues"] == ["ap-southeast-2"]


class TestNeptuneRegionFallback:
    """Requirement: with no ``region=``, the wrapper uses the ambient region.

    Every wrapper here is built WITHOUT ``region=``; that is the path real
    callers hit. The environment is isolated from ``~/.aws/config`` and the
    process-wide default session, and each fixture asserts its own premise.
    """

    @staticmethod
    def isolate(monkeypatch):
        monkeypatch.setenv("AWS_CONFIG_FILE", os.devnull)
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", os.devnull)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        for name in ("AWS_PROFILE", "AWS_DEFAULT_REGION", "AWS_REGION"):
            monkeypatch.delenv(name, raising=False)
        monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)

    @pytest.fixture
    def no_region(self, monkeypatch):
        self.isolate(monkeypatch)
        # Premise: nothing anywhere configures a region.
        assert boto3.Session().region_name is None

    @pytest.fixture
    def ambient_region(self, monkeypatch, request):
        self.isolate(monkeypatch)
        monkeypatch.setenv("AWS_DEFAULT_REGION", request.param)
        # Premise: the environment alone yields exactly this region.
        assert boto3.Session().region_name == request.param
        return request.param

    @staticmethod
    def unconfigured_wrapper():
        client = MagicMock()
        client.meta.region_name = IAM_PSEUDO_REGION
        client.get_role.return_value = TRUST_NA
        sim = FakePolicySimulator(ALLOW_ALL)
        client.simulate_principal_policy.side_effect = sim
        logger = MagicMock()
        # Deliberately no region= : this is the fallback path.
        return IamClientWrapper(ROLE_ARN, client, logger=logger), sim, logger

    @staticmethod
    def requested_region(call):
        return [
            e["ContextKeyValues"]
            for e in call["ContextEntries"]
            if e["ContextKeyName"] == "aws:RequestedRegion"
        ]

    REGIONS = ["eu-west-1", "ap-southeast-2", "sa-east-1"]

    @pytest.mark.parametrize("ambient_region", REGIONS, indirect=True)
    def test_ambient_region_reaches_graph_arn(self, ambient_region):
        wrapper, _, _ = self.unconfigured_wrapper()
        assert wrapper._neptune_graph_arn() == (
            f"arn:aws:neptune-graph:{ambient_region}:{ACCOUNT}:graph/*"
        )

    @pytest.mark.parametrize("ambient_region", REGIONS, indirect=True)
    def test_ambient_region_reaches_simulation_and_requested_region(
        self, ambient_region
    ):
        wrapper, sim, logger = self.unconfigured_wrapper()
        wrapper.has_create_na_permissions()
        assert sim.calls
        for call in sim.calls:
            assert [a.split(":")[3] for a in call["ResourceArns"]] == [ambient_region]
            assert self.requested_region(call) == [[ambient_region]]
        assert_clean(sim, logger)

    @pytest.mark.parametrize("ambient_region", REGIONS, indirect=True)
    def test_ambient_region_satisfies_region_conditioned_grant(self, ambient_region):
        grant = FakePolicySimulator(
            [
                allow(
                    ["neptune-graph:*"],
                    [
                        f"arn:aws:neptune-graph:{ambient_region}:{ACCOUNT}:graph/*",
                        f"arn:aws:neptune-graph:{ambient_region}:{ACCOUNT}:graph-snapshot/*",
                    ],
                    {"StringEquals": {"aws:RequestedRegion": ambient_region}},
                )
            ]
        )
        wrapper, _, logger = self.unconfigured_wrapper()
        wrapper.client.simulate_principal_policy.side_effect = grant
        wrapper.has_create_na_permissions()
        assert_clean(grant, logger)

    @pytest.mark.parametrize("ambient_region", ["eu-west-1"], indirect=True)
    def test_explicit_region_wins_over_ambient_region(self, ambient_region):
        wrapper, _, _ = make_wrapper(region="ap-southeast-2")
        assert wrapper._neptune_graph_arn().split(":")[3] == "ap-southeast-2"

    def test_no_region_uses_wildcard_in_graph_arn(self, no_region):
        wrapper, _, _ = self.unconfigured_wrapper()
        assert wrapper._neptune_graph_arn() == (
            f"arn:aws:neptune-graph:*:{ACCOUNT}:graph/*"
        )

    def test_no_region_omits_requested_region_and_never_sends_none(self, no_region):
        wrapper, sim, _ = self.unconfigured_wrapper()
        wrapper.has_create_na_permissions()
        assert sim.calls
        for call in sim.calls:
            assert self.requested_region(call) == []
            assert all("None" not in a for a in call["ResourceArns"])
            assert [a.split(":")[3] for a in call["ResourceArns"]] == ["*"]

    def test_no_region_logs_a_warning(self, no_region):
        wrapper, _, logger = self.unconfigured_wrapper()
        wrapper.has_create_na_permissions()
        assert logger.warning.called
        assert "region" in logger.warning.call_args_list[0].args[0].lower()

    @pytest.mark.parametrize("ambient_region", ["eu-west-1"], indirect=True)
    def test_configured_region_does_not_log_the_no_region_warning(self, ambient_region):
        wrapper, _, logger = self.unconfigured_wrapper()
        wrapper.has_create_na_permissions()
        assert not logger.warning.called

    @pytest.fixture
    def default_session_region(self, monkeypatch, request):
        """Region set only via ``boto3.setup_default_session``; env has none.

        ``isolate`` registers the prior ``boto3.DEFAULT_SESSION`` with
        monkeypatch, so it is restored on teardown and cannot leak.
        """
        self.isolate(monkeypatch)
        boto3.setup_default_session(region_name=request.param)
        # Premises: the default session has the region, a fresh Session does not.
        assert boto3.DEFAULT_SESSION.region_name == request.param
        assert boto3.Session().region_name is None
        return request.param

    @pytest.mark.parametrize(
        "default_session_region", ["eu-west-1", "ap-southeast-2"], indirect=True
    )
    def test_default_session_region_reaches_graph_arn(self, default_session_region):
        wrapper, _, _ = self.unconfigured_wrapper()
        assert wrapper._neptune_graph_arn() == (
            f"arn:aws:neptune-graph:{default_session_region}:{ACCOUNT}:graph/*"
        )

    @pytest.mark.parametrize(
        "default_session_region", ["eu-west-1", "ap-southeast-2"], indirect=True
    )
    def test_default_session_region_reaches_requested_region_and_logs_no_warning(
        self, default_session_region
    ):
        wrapper, sim, logger = self.unconfigured_wrapper()
        wrapper.has_create_na_permissions()
        assert sim.calls
        for call in sim.calls:
            assert [a.split(":")[3] for a in call["ResourceArns"]] == [
                default_session_region
            ]
            assert self.requested_region(call) == [[default_session_region]]
        assert_clean(sim, logger)

    @pytest.mark.parametrize("default_session_region", ["eu-west-1"], indirect=True)
    def test_explicit_region_wins_over_default_session(self, default_session_region):
        wrapper, _, _ = make_wrapper(region="ap-southeast-2")
        assert wrapper._neptune_graph_arn().split(":")[3] == "ap-southeast-2"

    @pytest.mark.parametrize("default_session_region", ["eu-west-1"], indirect=True)
    def test_default_session_region_wins_over_environment_region(
        self, default_session_region, monkeypatch
    ):
        monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")
        assert boto3.Session().region_name == "sa-east-1"  # env really differs
        wrapper, sim, _ = self.unconfigured_wrapper()
        assert wrapper._neptune_graph_arn().split(":")[3] == default_session_region
        wrapper.has_create_na_permissions()
        for call in sim.calls:
            assert self.requested_region(call) == [[default_session_region]]


S3_SPLIT_GRANTS = [
    allow(["athena:*"], ["*"]),
    allow(["s3:ListBucket"], ["arn:aws:s3:::my-bucket"]),
    allow(
        ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        ["arn:aws:s3:::my-bucket/*"],
    ),
]
S3_NARROW_GRANTS = [
    allow(["athena:*"], ["*"]),
    allow(["s3:ListBucket"], ["arn:aws:s3:::my-bucket"]),
    allow(
        ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
        ["arn:aws:s3:::my-bucket/staging/*"],
    ),
]


class TestS3BucketVersusObjectSimulation:
    """Requirement: Bucket-level and object-level S3 actions are simulated separately."""

    def test_athena_prefixed_staging_path_with_split_grants_passes(self):
        wrapper, _, _ = make_wrapper(S3_SPLIT_GRANTS)
        no_versioning(wrapper)
        assert wrapper.has_athena_permissions("s3://my-bucket/staging/") is True

    def test_athena_object_grant_scoped_to_prefix_passes(self):
        wrapper, _, _ = make_wrapper(S3_NARROW_GRANTS)
        no_versioning(wrapper)
        assert wrapper.has_athena_permissions("s3://my-bucket/staging/") is True

    def test_athena_simulates_bucket_and_object_actions_against_own_arns(self):
        wrapper, sim, _ = make_wrapper()
        no_versioning(wrapper)
        wrapper.has_athena_permissions("s3://my-bucket/staging/")
        by_action = sim.resources_by_action()
        assert by_action["s3:ListBucket"] == {"arn:aws:s3:::my-bucket"}
        assert by_action["s3:GetObject"] == {"arn:aws:s3:::my-bucket/staging/*"}
        assert by_action["s3:PutObject"] == {"arn:aws:s3:::my-bucket/staging/*"}

    def test_athena_bucket_and_object_actions_are_separate_requests(self):
        wrapper, sim, _ = make_wrapper()
        no_versioning(wrapper)
        wrapper.has_athena_permissions("s3://my-bucket/staging/")
        for call in sim.calls_for("s3:"):
            kinds = {a == "s3:ListBucket" for a in call["ActionNames"]}
            assert len(kinds) == 1, f"mixed bucket/object actions: {call}"

    def test_prefixless_staging_path_simulates_objects_against_bucket_wildcard(self):
        wrapper, sim, _ = make_wrapper(S3_SPLIT_GRANTS)
        no_versioning(wrapper)
        wrapper.has_athena_permissions("s3://my-bucket/")  # must not raise
        by_action = sim.resources_by_action()
        assert by_action["s3:GetObject"] == {"arn:aws:s3:::my-bucket/*"}
        assert by_action["s3:PutObject"] == {"arn:aws:s3:::my-bucket/*"}
        assert by_action["s3:ListBucket"] == {"arn:aws:s3:::my-bucket"}

    @pytest.mark.parametrize(
        "invoke,expected",
        [
            (
                lambda w: w.has_import_from_s3_permissions("s3://my-bucket/data/"),
                {"s3:GetObject": "arn:aws:s3:::my-bucket/data/*"},
            ),
            (
                lambda w: w.has_export_to_s3_permissions("s3://my-bucket/out/"),
                {
                    "s3:PutObject": "arn:aws:s3:::my-bucket/out/*",
                    "s3:DeleteObject": "arn:aws:s3:::my-bucket/out/*",
                    "s3:ListBucket": "arn:aws:s3:::my-bucket",
                },
            ),
            (
                lambda w: w.has_delete_s3_permissions("s3://my-bucket/out/"),
                {
                    "s3:DeleteObject": "arn:aws:s3:::my-bucket/out/*",
                    "s3:ListBucket": "arn:aws:s3:::my-bucket",
                },
            ),
            (
                lambda w: w.has_import_from_s3_permissions("s3://my-bucket/"),
                {"s3:GetObject": "arn:aws:s3:::my-bucket/*"},
            ),
        ],
    )
    def test_s3_checks_simulate_each_action_against_its_own_arn(self, invoke, expected):
        wrapper, sim, _ = make_wrapper(S3_SPLIT_GRANTS)
        no_versioning(wrapper)
        invoke(wrapper)  # split grants: must not raise
        assert sim.resources_by_action() == {a: {r} for a, r in expected.items()}

    def test_validate_permissions_with_least_privilege_policy_reports_all_ok(self):
        key = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/abc"
        policy = [
            *ACCOUNT_SCOPED_NEPTUNE,
            allow(["kms:*"], [key]),
            allow(["athena:*"], ["*"]),
            allow(["s3:ListBucket"], ["arn:aws:s3:::imp", "arn:aws:s3:::exp"]),
            allow(
                ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                ["arn:aws:s3:::imp/*", "arn:aws:s3:::exp/*"],
            ),
        ]
        wrapper, sim, _ = make_wrapper(policy)
        result = wrapper.validate_permissions("s3://imp/", key, "s3://exp/", key)
        assert result and all(result.values()), result
        for call in sim.calls_for("s3:"):
            assert len({a == "s3:ListBucket" for a in call["ActionNames"]}) == 1


class TestPartitionHandling:
    """Requirement: ARNs use the caller's partition."""

    @pytest.mark.parametrize(
        "partition,region",
        [("aws-us-gov", "us-gov-west-1"), ("aws-cn", "cn-north-1")],
    )
    def test_neptune_and_s3_arns_use_callers_partition(self, partition, region):
        role = f"arn:{partition}:iam::{ACCOUNT}:role/test-role"
        wrapper, sim, _ = make_wrapper(role_arn=role, region=region)
        no_versioning(wrapper)
        wrapper.has_create_na_permissions()
        wrapper.has_athena_permissions("s3://my-bucket/staging/")
        by_action = sim.resources_by_action()
        assert by_action["neptune-graph:CreateGraph"] == {
            f"arn:{partition}:neptune-graph:{region}:{ACCOUNT}:graph/*"
        }
        assert by_action["s3:ListBucket"] == {f"arn:{partition}:s3:::my-bucket"}
        assert by_action["s3:PutObject"] == {
            f"arn:{partition}:s3:::my-bucket/staging/*"
        }

    @pytest.mark.parametrize(
        "partition,region",
        [
            ("aws", "us-east-1"),
            ("aws-us-gov", "us-gov-west-1"),
            ("aws-cn", "cn-north-1"),
        ],
    )
    def test_neptune_graph_arn_helper(self, partition, region):
        role = f"arn:{partition}:iam::{ACCOUNT}:role/test-role"
        wrapper, _, _ = make_wrapper(role_arn=role, region=region)
        assert wrapper._neptune_graph_arn() == (
            f"arn:{partition}:neptune-graph:{region}:{ACCOUNT}:graph/*"
        )

    def test_aws_cn_sts_caller_is_converted_in_its_own_partition(self):
        wrapper, _, _ = make_wrapper(
            role_arn=f"arn:aws-cn:sts::{ACCOUNT}:assumed-role/MyRole/session",
            region="cn-north-1",
        )
        assert wrapper.role_arn == f"arn:aws-cn:iam::{ACCOUNT}:role/MyRole"

    def test_commercial_partition_arns_are_unchanged(self):
        # Guard (passes against the pre-fix code except for the new region= keyword; pins behaviour the fix must not break).
        wrapper, sim, _ = make_wrapper()
        no_versioning(wrapper)
        wrapper.has_athena_permissions("s3://my-bucket/staging/")
        assert all(
            r.startswith("arn:aws:") or r == "*"
            for c in sim.calls
            for r in c["ResourceArns"]
        )

    @pytest.mark.parametrize(
        "s3_path,partition,bucket,obj",
        [
            ("s3://b/staging/", "aws", "arn:aws:s3:::b", "arn:aws:s3:::b/staging/*"),
            ("s3://b/staging", "aws", "arn:aws:s3:::b", "arn:aws:s3:::b/staging/*"),
            ("s3://b/a/c/", "aws", "arn:aws:s3:::b", "arn:aws:s3:::b/a/c/*"),
            ("s3://b/", "aws", "arn:aws:s3:::b", "arn:aws:s3:::b/*"),
            ("s3://b", "aws", "arn:aws:s3:::b", "arn:aws:s3:::b/*"),
            (
                "s3://b/p/",
                "aws-us-gov",
                "arn:aws-us-gov:s3:::b",
                "arn:aws-us-gov:s3:::b/p/*",
            ),
            ("s3://b/", "aws-cn", "arn:aws-cn:s3:::b", "arn:aws-cn:s3:::b/*"),
        ],
    )
    def test_bucket_and_object_arn_pair(self, s3_path, partition, bucket, obj):
        """Pinned API: ``_get_s3_bucket_and_object_arns(s3_path, partition="aws")``."""
        # Imported here so a missing helper reds this test only, not the module.
        from nx_neptune.clients.iam_client import _get_s3_bucket_and_object_arns

        assert _get_s3_bucket_and_object_arns(s3_path, partition=partition) == (
            bucket,
            obj,
        )


class TestNewArnsPassValidation:
    """Risk: ``_validate_arns`` rejects a trailing ``/``; the new ARNs end in ``/*``."""

    # Guard (passes against the pre-fix code; pins behaviour the fix must not break):
    # failing later would break every simulation.
    @pytest.mark.parametrize(
        "arn",
        [
            "arn:aws:neptune-graph:us-east-1:123456789012:graph/*",
            "arn:aws-us-gov:neptune-graph:us-gov-west-1:123456789012:graph/*",
            "arn:aws-cn:neptune-graph:cn-north-1:123456789012:graph/*",
            "arn:aws:s3:::my-bucket/*",
            "arn:aws:s3:::my-bucket/staging/*",
            "arn:aws-us-gov:s3:::my-bucket/a/b/*",
            "arn:aws:s3:::my-bucket",
        ],
    )
    def test_botocore_accepts_wildcard_suffixed_arns(self, arn):
        assert IamClientWrapper._validate_arns(arn) is True

    @pytest.mark.parametrize(
        "resource",
        [
            "arn:aws:neptune-graph:us-east-1:123456789012:graph/*",
            "arn:aws:s3:::my-bucket/staging/*",
        ],
    )
    def test_check_aws_permission_accepts_wildcard_suffixed_resource(self, resource):
        # Guard (passes against the pre-fix code except for the new region= keyword; pins behaviour the fix must not break).
        wrapper, _, _ = make_wrapper()
        assert wrapper.check_aws_permission("op", ["s3:GetObject"], resource) == {
            "s3:GetObject": True
        }


class TestInconclusiveSimulation:
    """Requirement: An inconclusive simulation warns instead of aborting."""

    @staticmethod
    def _canned(decisions):
        client = MagicMock()
        client.meta.region_name = IAM_PSEUDO_REGION
        client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {"EvalActionName": a, "EvalDecision": d} for a, d in decisions.items()
            ]
        }
        logger = MagicMock()
        return IamClientWrapper(ROLE_ARN, client, logger=logger), logger

    def test_implicit_deny_warns_and_records_false(self):
        wrapper, logger = self._canned(
            {
                "neptune-graph:CreateGraph": "allowed",
                "neptune-graph:TagResource": "implicitDeny",
            }
        )
        result = wrapper.check_aws_permission(
            "Create Neptune Instance",
            ["neptune-graph:CreateGraph", "neptune-graph:TagResource"],
        )
        assert result == {
            "neptune-graph:CreateGraph": True,
            "neptune-graph:TagResource": False,
        }
        messages = [
            " ".join(str(a) for a in c.args) for c in logger.warning.call_args_list
        ]
        assert any(
            "neptune-graph:TagResource" in m and "Create Neptune Instance" in m
            for m in messages
        ), messages

    def test_explicit_deny_raises_naming_action_and_operation(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break).
        wrapper, _ = self._canned({"neptune-graph:TagResource": "explicitDeny"})
        with pytest.raises(ValueError) as exc:
            wrapper.check_aws_permission(
                "Create Neptune Instance", ["neptune-graph:TagResource"]
            )
        assert "neptune-graph:TagResource" in str(exc.value)
        assert "Create Neptune Instance" in str(exc.value)

    def test_explicit_deny_still_raises_when_mixed_with_implicit_deny(self):
        wrapper, _ = self._canned(
            {
                "neptune-graph:CreateGraph": "implicitDeny",
                "neptune-graph:TagResource": "explicitDeny",
            }
        )
        with pytest.raises(ValueError, match="neptune-graph:TagResource"):
            wrapper.check_aws_permission(
                "op", ["neptune-graph:CreateGraph", "neptune-graph:TagResource"]
            )

    def test_preflight_continues_on_implicit_deny(self):
        # Only CreateGraph is granted; TagResource is an implicit deny.
        wrapper, _, logger = make_wrapper([allow(["neptune-graph:CreateGraph"], ["*"])])
        wrapper.has_create_na_permissions()  # must not raise
        assert logger.warning.called

    def test_preflight_aborts_on_explicit_deny(self):
        # Guard (passes against the pre-fix code except for the new region= keyword; pins behaviour the fix must not break).
        wrapper, _, _ = make_wrapper(
            [
                allow(["neptune-graph:*"], ["*"]),
                deny(["neptune-graph:TagResource"], ["*"]),
            ]
        )
        with pytest.raises(ValueError, match="neptune-graph:TagResource"):
            wrapper.has_create_na_permissions()

    def test_simulation_denied_still_returns_empty_mapping(self):
        # Guard (passes against the pre-fix code except for the new region= keyword; pins behaviour the fix must not break).
        from botocore.exceptions import ClientError

        wrapper, _, logger = make_wrapper()
        wrapper.client.simulate_principal_policy.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied", "Message": "no"}},
            "SimulatePrincipalPolicy",
        )
        assert wrapper.check_aws_permission("op", ["s3:GetObject"]) == {}
        assert logger.warning.called


class TestValidatePermissionsMapping:
    """Requirement: ``validate_permissions`` reports False for an operation whose
    simulation was not fully allowed, even though an implicit deny does not raise."""

    KEY = f"arn:aws:kms:{REGION}:{ACCOUNT}:key/abc"

    @staticmethod
    def policy_without(*missing):
        """Allow-list built from ALLOW_ALL minus ``missing`` actions (implicit deny)."""
        every = [
            "neptune-graph:CreateGraph",
            "neptune-graph:TagResource",
            "neptune-graph:DeleteGraph",
            "neptune-graph:StartGraph",
            "neptune-graph:StopGraph",
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket",
            "kms:Decrypt",
            "kms:GenerateDataKey",
            "kms:DescribeKey",
            "athena:StartQueryExecution",
            "athena:GetQueryExecution",
            "neptune-graph:CreateGraphSnapshot",
            "neptune-graph:RestoreGraphFromSnapshot",
            "neptune-graph:DeleteGraphSnapshot",
        ]
        return [allow([a for a in every if a not in missing], ["*"])]

    def run(self, *missing):
        wrapper, _, _ = make_wrapper(self.policy_without(*missing))
        return wrapper.validate_permissions(
            "s3://imp/", self.KEY, "s3://exp/", self.KEY
        )

    def test_fully_granted_policy_reports_every_operation_true(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break): baseline for the denial tests below.
        result = self.run()
        assert result and all(result.values()), result

    def test_implicitly_denied_operation_is_reported_false(self):
        result = self.run("neptune-graph:DeleteGraph")
        assert result["delete_na_instance"] is False
        assert result["start_graph"] is True

    def test_partially_granted_operation_is_reported_false(self):
        # CreateGraph granted, TagResource not: a mixed group must still be False.
        result = self.run("neptune-graph:TagResource")
        assert result["create_na_instance"] is False
        assert result["create_graph_snapshot"] is False

    def test_denied_object_leg_alone_makes_the_operation_false(self):
        result = self.run("s3:PutObject")
        assert result["export_csv_to_s3"] is False
        assert result["empty_s3_bucket"] is False
        assert result["import_from_s3"] is True

    def test_denied_bucket_leg_alone_is_not_discarded(self):
        # Object actions are allowed; only the bucket-level leg is denied.
        result = self.run("s3:ListBucket")
        assert result["export_csv_to_s3"] is False
        assert result["empty_s3_bucket"] is False
        assert result["create_csv_table_from_s3"] is False
        assert result["import_from_s3"] is True

    def test_denied_kms_leg_makes_the_operation_false(self):
        result = self.run("kms:Decrypt")
        assert result["import_from_s3"] is False
        assert result["export_csv_to_s3"] is False

    def test_explicitly_denied_operation_is_reported_false(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break):
        # explicit deny surfaces via the exception path.
        wrapper, _, _ = make_wrapper(
            [allow(["*"], ["*"]), deny(["neptune-graph:StopGraph"], ["*"])]
        )
        result = wrapper.validate_permissions(None, None, None, None)
        assert result["stop_graph"] is False
        assert result["start_graph"] is True


class TestStsRoleDetection:
    """Requirement: STS ARNs are recognised by the ARN's service segment, not a
    substring of the whole string."""

    @pytest.mark.parametrize(
        "role_name", ["tests-role", "sts-admin", "ghosts", "my-sts-role"]
    )
    def test_iam_role_with_sts_in_its_name_is_left_untouched(self, role_name):
        arn = f"arn:aws:iam::{ACCOUNT}:role/{role_name}"
        assert IamClientWrapper(arn, MagicMock()).role_arn == arn

    def test_iam_role_with_path_containing_sts_is_left_untouched(self):
        arn = f"arn:aws:iam::{ACCOUNT}:role/sts/team/builder"
        assert IamClientWrapper(arn, MagicMock()).role_arn == arn

    def test_assumed_role_arn_is_still_converted(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break).
        wrapper = IamClientWrapper(
            f"arn:aws:sts::{ACCOUNT}:assumed-role/MyRole/session", MagicMock()
        )
        assert wrapper.role_arn == f"arn:aws:iam::{ACCOUNT}:role/MyRole"

    def test_assumed_role_with_sts_in_name_is_converted_once(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break).
        wrapper = IamClientWrapper(
            f"arn:aws:sts::{ACCOUNT}:assumed-role/tests-role/session", MagicMock()
        )
        assert wrapper.role_arn == f"arn:aws:iam::{ACCOUNT}:role/tests-role"


class TestS3ArnInputForms:
    """Requirement: ``_get_s3_bucket_and_object_arns`` normalises every input form."""

    @staticmethod
    def arns(path, partition="aws"):
        from nx_neptune.clients.iam_client import _get_s3_bucket_and_object_arns

        return _get_s3_bucket_and_object_arns(path, partition=partition)

    @pytest.mark.parametrize(
        "s3_path,partition,expected",
        [
            (
                "arn:aws:s3:::b/data/",
                "aws",
                ("arn:aws:s3:::b", "arn:aws:s3:::b/data/*"),
            ),
            ("arn:aws:s3:::b", "aws", ("arn:aws:s3:::b", "arn:aws:s3:::b/*")),
            (
                "arn:aws-cn:s3:::b/data",
                "aws-cn",
                ("arn:aws-cn:s3:::b", "arn:aws-cn:s3:::b/data/*"),
            ),
            (
                "arn:aws-us-gov:s3:::b/a/c/",
                "aws-us-gov",
                ("arn:aws-us-gov:s3:::b", "arn:aws-us-gov:s3:::b/a/c/*"),
            ),
        ],
    )
    def test_arn_form_input_is_reduced_to_bucket_and_prefix(
        self, s3_path, partition, expected
    ):
        assert self.arns(s3_path, partition) == expected

    def test_arn_form_equals_uri_form(self):
        assert self.arns("arn:aws:s3:::b/data/") == self.arns("s3://b/data/")

    @pytest.mark.parametrize(
        "s3_path,expected_object",
        [
            ("s3://b//staging/", "arn:aws:s3:::b/staging/*"),
            ("s3://b//staging", "arn:aws:s3:::b/staging/*"),
            ("s3://b///a/c//", "arn:aws:s3:::b/a/c/*"),
            ("s3://b//", "arn:aws:s3:::b/*"),
            ("b//staging/", "arn:aws:s3:::b/staging/*"),
        ],
    )
    def test_leading_and_trailing_slashes_in_prefix_are_stripped(
        self, s3_path, expected_object
    ):
        bucket, obj = self.arns(s3_path)
        assert bucket == "arn:aws:s3:::b"
        assert obj == expected_object

    @pytest.mark.parametrize(
        "action",
        [
            "s3:ListBucketVersions",
            "s3:ListBucketMultipartUploads",
            "s3:GetBucketLocation",
            "s3:GetBucketVersioning",
        ],
    )
    def test_bucket_level_action_is_simulated_against_the_bucket_arn(self, action):
        wrapper, sim, _ = make_wrapper()
        wrapper._check_s3_permission("op", [action, "s3:GetObject"], "s3://b/p/")
        assert sim.resources_by_action() == {
            action: {"arn:aws:s3:::b"},
            "s3:GetObject": {"arn:aws:s3:::b/p/*"},
        }
        assert len(sim.calls) == 2  # bucket and object legs are separate requests

    def test_only_bucket_level_actions_make_a_single_bucket_request(self):
        wrapper, sim, _ = make_wrapper()
        wrapper._check_s3_permission("op", ["s3:GetBucketLocation"], "s3://b/p/")
        assert len(sim.calls) == 1
        assert sim.calls[0]["ResourceArns"] == ["arn:aws:s3:::b"]


class TestDefaultFactoryAndPartition:
    """Requirement: ``IamClientWrapper.default`` honours ``region=`` and the caller's partition.

    ``boto3.client`` is replaced by a recorder so no network or credentials are
    involved; STS answers ``get_caller_identity`` with the ARN under test.
    """

    @pytest.fixture
    def recorded_clients(self, monkeypatch):
        TestNeptuneRegionFallback.isolate(monkeypatch)
        # A cached shared factory would hide client creation; start clean.
        from nx_neptune.clients.client_factory import ClientFactory

        monkeypatch.setattr(ClientFactory, "_default", None)
        created = []

        def fake_client(service_name=None, **kwargs):
            client = MagicMock()
            client.meta.region_name = IAM_PSEUDO_REGION
            client.get_caller_identity.return_value = {"Arn": self.caller_arn}
            client.simulate_principal_policy.side_effect = self.sim
            created.append((service_name, kwargs))
            return client

        self.caller_arn = f"arn:aws:sts::{ACCOUNT}:assumed-role/test-role/session"
        self.sim = FakePolicySimulator(ALLOW_ALL)
        monkeypatch.setattr(boto3, "client", fake_client)
        # Premise: nothing has been created yet.
        assert created == []
        return created

    @pytest.mark.parametrize("region", ["eu-west-1", "ap-southeast-2"])
    def test_default_creates_its_own_clients_in_the_given_region(
        self, recorded_clients, region
    ):
        IamClientWrapper.default(region=region)
        services = {name: kw for name, kw in recorded_clients}
        assert set(services) == {"sts", "iam"}
        assert {kw.get("region_name") for kw in services.values()} == {region}

    @pytest.mark.parametrize(
        "partition,region",
        [("aws-cn", "cn-north-1"), ("aws-us-gov", "us-gov-west-1")],
    )
    def test_default_preserves_caller_partition_through_simulated_arns(
        self, recorded_clients, partition, region
    ):
        self.caller_arn = f"arn:{partition}:sts::{ACCOUNT}:assumed-role/test-role/s"
        wrapper = IamClientWrapper.default(region=region)
        assert wrapper.role_arn == f"arn:{partition}:iam::{ACCOUNT}:role/test-role"

        wrapper.validate_permissions(
            "s3://imp/data",
            f"arn:{partition}:kms:{region}:{ACCOUNT}:key/k1",
            "s3://exp/out",
            f"arn:{partition}:kms:{region}:{ACCOUNT}:key/k2",
        )
        assert self.sim.calls
        for call in self.sim.calls:
            assert call["PolicySourceArn"].startswith(f"arn:{partition}:iam::")
            for arn in call["ResourceArns"]:
                assert arn == "*" or arn.startswith(f"arn:{partition}:"), arn
        resources = {a for c in self.sim.calls for a in c["ResourceArns"]}
        assert f"arn:{partition}:neptune-graph:{region}:{ACCOUNT}:graph/*" in resources
        assert f"arn:{partition}:s3:::imp/data/*" in resources


class TestResolvedRegionHasNoGlobalSideEffect:
    """Requirement: resolving the region must not install ``boto3.DEFAULT_SESSION``."""

    @pytest.fixture
    def pristine_default_session(self, monkeypatch):
        TestNeptuneRegionFallback.isolate(monkeypatch)
        # Premise: there is no global default session to begin with.
        assert boto3.DEFAULT_SESSION is None

    def test_unconfigured_region_lookup_leaves_default_session_unset(
        self, pristine_default_session
    ):
        wrapper, _, _ = TestNeptuneRegionFallback.unconfigured_wrapper()
        assert wrapper._resolved_region is None
        assert boto3.DEFAULT_SESSION is None

    def test_ambient_region_lookup_leaves_default_session_unset(
        self, pristine_default_session, monkeypatch
    ):
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
        wrapper, _, _ = TestNeptuneRegionFallback.unconfigured_wrapper()
        assert wrapper._resolved_region == "eu-west-1"
        assert boto3.DEFAULT_SESSION is None

    def test_full_preflight_leaves_default_session_unset(
        self, pristine_default_session, monkeypatch
    ):
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
        wrapper, _, _ = TestNeptuneRegionFallback.unconfigured_wrapper()
        wrapper.has_create_na_permissions()
        assert boto3.DEFAULT_SESSION is None


class TestNoRegionPathConfirmsOnlyUnscopedGrants:
    """Requirement: with no region configured, unscoped grants are still confirmed
    and only region-scoped ones go unverified; the warning fires regardless.

    No ``region=`` is passed anywhere. Pins the middle ground so a change that
    confirms nothing, or everything, goes red.
    """

    @pytest.fixture
    def no_region(self, monkeypatch):
        TestNeptuneRegionFallback.isolate(monkeypatch)
        # Premise: nothing anywhere configures a region.
        assert boto3.Session().region_name is None
        assert boto3.DEFAULT_SESSION is None

    @staticmethod
    def run_with_grant(statements):
        wrapper, _, logger = TestNeptuneRegionFallback.unconfigured_wrapper()
        sim = FakePolicySimulator(statements)
        wrapper.client.simulate_principal_policy.side_effect = sim
        wrapper.has_create_na_permissions()
        # Premise: the simulator really evaluated the Neptune actions.
        assert sim.decisions
        return sim, logger

    @pytest.mark.parametrize(
        "resource",
        [
            ["*"],
            [
                f"arn:aws:neptune-graph:*:{ACCOUNT}:graph/*",
                f"arn:aws:neptune-graph:*:{ACCOUNT}:graph-snapshot/*",
            ],
        ],
        ids=["star-resource", "wildcard-region-arn"],
    )
    def test_unscoped_grants_are_confirmed(self, no_region, resource):
        sim, _ = self.run_with_grant([allow(["neptune-graph:*"], resource)])
        assert all(d == "allowed" for _, d in sim.decisions), sim.decisions

    def test_region_pinned_grant_is_not_confirmed(self, no_region):
        pinned = f"arn:aws:neptune-graph:us-east-1:{ACCOUNT}:graph/*"
        sim, _ = self.run_with_grant([allow(["neptune-graph:*"], [pinned])])
        assert {d for _, d in sim.decisions} == {"implicitDeny"}, sim.decisions

    @pytest.mark.parametrize("resource", ["*"])
    def test_warning_fires_even_when_every_decision_is_allowed(
        self, no_region, resource
    ):
        sim, logger = self.run_with_grant([allow(["neptune-graph:*"], [resource])])
        assert all(d == "allowed" for _, d in sim.decisions)
        assert logger.warning.called
        assert "region" in logger.warning.call_args_list[0].args[0].lower()


REGION_LOCKDOWN = [
    allow(["neptune-graph:*"], ["*"]),
    {
        "Effect": "Deny",
        "Action": ["neptune-graph:*"],
        "Resource": ["*"],
        "Condition": {"StringNotEquals": {"aws:RequestedRegion": "ap-south-1"}},
    },
]


class TestFakeSimulatorMissingKeySemantics:
    """The fake must model the real service's per-operator missing-key behaviour."""

    @staticmethod
    def decide(statements, context=None):
        sim = FakePolicySimulator(statements)
        entries = [
            {"ContextKeyName": k, "ContextKeyValues": v, "ContextKeyType": "string"}
            for k, v in (context or {}).items()
        ]
        kwargs = {"ActionNames": ["neptune-graph:CreateGraph"], "ResourceArns": ["*"]}
        if entries:
            kwargs["ContextEntries"] = entries
        sim(**kwargs)
        return sim.decisions[0][1]

    def test_string_equals_with_missing_key_does_not_apply_the_allow(self):
        grant = allow(
            ["neptune-graph:*"],
            ["*"],
            {"StringEquals": {"aws:RequestedRegion": "ap-south-1"}},
        )
        assert self.decide([grant]) == "implicitDeny"

    def test_string_not_equals_with_missing_key_applies_the_deny(self):
        assert self.decide(REGION_LOCKDOWN) == "explicitDeny"

    def test_string_not_equals_with_matching_value_does_not_apply_the_deny(self):
        context = {"aws:RequestedRegion": ["ap-south-1"]}
        assert self.decide(REGION_LOCKDOWN, context) == "allowed"

    def test_string_not_equals_with_other_value_applies_the_deny(self):
        context = {"aws:RequestedRegion": ["us-east-1"]}
        assert self.decide(REGION_LOCKDOWN, context) == "explicitDeny"


class TestRegionLockdownPolicy:
    """Requirement: a deny that is an artifact of our own incomplete request must
    not abort the preflight; a deny on a complete request still must."""

    @pytest.fixture
    def no_region(self, monkeypatch):
        TestNeptuneRegionFallback.isolate(monkeypatch)
        # Premise: nothing configures a region, so aws:RequestedRegion is omitted.
        assert boto3.Session().region_name is None
        assert boto3.DEFAULT_SESSION is None

    @staticmethod
    def no_region_wrapper(statements):
        wrapper, _, logger = TestNeptuneRegionFallback.unconfigured_wrapper()
        sim = FakePolicySimulator(statements)
        wrapper.client.simulate_principal_policy.side_effect = sim
        return wrapper, sim, logger

    def test_no_region_lockdown_policy_does_not_abort_the_preflight(self, no_region):
        wrapper, sim, logger = self.no_region_wrapper(REGION_LOCKDOWN)
        wrapper.has_create_na_permissions()  # must not raise
        # Premise: the request really lacked the region and the policy denied it.
        assert sim.decisions
        assert {d for _, d in sim.decisions} == {"explicitDeny"}
        for call in sim.calls:
            assert "aws:RequestedRegion" not in {
                e["ContextKeyName"] for e in call.get("ContextEntries", [])
            }

    def test_no_region_lockdown_denial_is_warned_about(self, no_region):
        wrapper, _, logger = self.no_region_wrapper(REGION_LOCKDOWN)
        wrapper.has_create_na_permissions()
        assert logger.warning.called
        assert any(
            "region" in " ".join(str(a) for a in c.args).lower()
            for c in logger.warning.call_args_list
        )

    def test_no_region_lockdown_denial_is_recorded_as_false(self, no_region):
        wrapper, _, _ = self.no_region_wrapper(REGION_LOCKDOWN)
        results = wrapper.validate_permissions(None, None, None, None)
        assert results["create_na_instance"] is False

    def test_complete_request_in_allowed_region_passes_cleanly(self):
        # Guard (passes against the pre-fix code; pins behaviour the fix must not break).
        wrapper, sim, logger = make_wrapper(REGION_LOCKDOWN, region="ap-south-1")
        wrapper.has_create_na_permissions()
        assert_clean(sim, logger)

    def test_region_conditioned_deny_on_complete_request_still_raises(self):
        wrapper, sim, _ = make_wrapper(REGION_LOCKDOWN, region="us-east-1")
        with pytest.raises(ValueError, match="neptune-graph"):
            wrapper.has_create_na_permissions()
        assert {d for _, d in sim.decisions} == {"explicitDeny"}

    def test_unconditional_deny_on_complete_request_still_raises(self):
        wrapper, _, _ = make_wrapper(
            [allow(["neptune-graph:*"], ["*"]), deny(["neptune-graph:*"], ["*"])],
            region="ap-south-1",
        )
        with pytest.raises(ValueError, match="neptune-graph"):
            wrapper.has_create_na_permissions()


def grant_on(action, resource_type):
    return [allow([action], [ARN_FOR_TYPE[resource_type]])]


class TestNeptuneResourceTypes:
    """Requirement: an action is confirmed when the policy allows it on ANY of the
    resource types AWS lists for it, and only those; single-type actions stay strict."""

    @pytest.mark.parametrize("action", DUAL_TYPE_ACTIONS)
    @pytest.mark.parametrize("resource_type", ["graph", "graph-snapshot"])
    def test_dual_type_action_is_confirmed_on_either_type(self, action, resource_type):
        assert_confirmed(grant_on(action, resource_type), action)

    def test_create_graph_snapshot_granted_only_on_graph_is_confirmed(self):
        assert_confirmed(
            grant_on("neptune-graph:CreateGraphSnapshot", "graph"),
            "neptune-graph:CreateGraphSnapshot",
        )

    def test_delete_graph_snapshot_granted_on_snapshot_is_confirmed(self):
        assert_confirmed(
            grant_on("neptune-graph:DeleteGraphSnapshot", "graph-snapshot"),
            "neptune-graph:DeleteGraphSnapshot",
        )

    def test_delete_graph_snapshot_granted_only_on_graph_is_unconfirmed(self):
        assert_unconfirmed(
            grant_on("neptune-graph:DeleteGraphSnapshot", "graph"),
            "neptune-graph:DeleteGraphSnapshot",
        )

    @pytest.mark.parametrize(
        "action", [a for a in SINGLE_TYPE_ACTIONS if "Snapshot" not in a]
    )
    def test_graph_only_action_is_confirmed_on_graph(self, action):
        assert_confirmed(grant_on(action, "graph"), action)

    @pytest.mark.parametrize(
        "action", [a for a in SINGLE_TYPE_ACTIONS if "Snapshot" not in a]
    )
    def test_graph_only_action_granted_only_on_snapshot_is_unconfirmed(self, action):
        assert_unconfirmed(grant_on(action, "graph-snapshot"), action)

    @pytest.mark.parametrize("action", sorted(NA_RESOURCE_TYPES))
    def test_action_is_simulated_against_exactly_its_valid_resource_types(self, action):
        wrapper, sim, _ = make_wrapper()
        wrapper._check_neptune_permission("op", [action])
        assert_each_action_on_its_resource_type(sim.calls)
        assert sim.resources_by_action()[action] == valid_neptune_arns(action)

    def test_snapshot_grant_is_not_matched_by_graph_arn(self):
        # Fidelity premise: a graph-snapshot/* grant does not cover graph/*.
        action = "neptune-graph:DeleteGraphSnapshot"
        sim = FakePolicySimulator(grant_on(action, "graph-snapshot"))
        sim(ActionNames=[action], ResourceArns=[GRAPH_ARN])
        assert sim.decisions == [(action, "implicitDeny")]

    def test_mixed_methods_confirm_each_action_on_its_own_valid_grant(self):
        wrapper, sim, logger = make_wrapper(
            [
                allow(["neptune-graph:TagResource"], [GRAPH_ARN]),
                allow(["neptune-graph:CreateGraphSnapshot"], [GRAPH_ARN]),
            ]
        )
        result = wrapper._check_neptune_permission(
            "op",
            ["neptune-graph:CreateGraphSnapshot", "neptune-graph:TagResource"],
        )
        assert result == {
            "neptune-graph:CreateGraphSnapshot": True,
            "neptune-graph:TagResource": True,
        }
        assert not logger.warning.called, logger.warning.call_args_list


class TestListBucketPrefixContext:
    """Requirement: ``s3:ListBucket`` carries an ``s3:prefix`` context entry derived
    from the path's prefix, so a prefix-conditioned bucket policy can match."""

    BUCKET = "arn:aws:s3:::my-bucket"

    @staticmethod
    def prefix_policy(pattern):
        return [
            allow(
                ["s3:ListBucket"],
                [TestListBucketPrefixContext.BUCKET],
                {"StringLike": {"s3:prefix": pattern}},
            )
        ]

    @staticmethod
    def prefixes(sim):
        return [
            v
            for c in sim.calls_for("s3:ListBucket")
            for e in c.get("ContextEntries", [])
            if e["ContextKeyName"] == "s3:prefix"
            for v in e["ContextKeyValues"]
        ]

    def test_list_bucket_carries_s3_prefix_derived_from_the_path(self):
        wrapper, sim, _ = make_wrapper()
        wrapper._check_s3_permission("op", ["s3:ListBucket"], "s3://my-bucket/staging/")
        values = self.prefixes(sim)
        assert values and all(v.startswith("staging") for v in values), values

    def test_prefix_conditioned_bucket_policy_matches_the_path_prefix(self):
        wrapper, sim, logger = make_wrapper(self.prefix_policy(["staging*"]))
        wrapper._check_s3_permission("op", ["s3:ListBucket"], "s3://my-bucket/staging/")
        assert_clean(sim, logger)

    def test_prefix_conditioned_bucket_policy_rejects_a_different_prefix(self):
        # Keeps the condition non-vacuous: another prefix must not be confirmed.
        wrapper, sim, _ = make_wrapper(self.prefix_policy(["staging*"]))
        wrapper._check_s3_permission("op", ["s3:ListBucket"], "s3://my-bucket/other/")
        assert {d for _, d in sim.decisions} == {"implicitDeny"}

    def test_prefixless_path_sends_no_nonempty_prefix(self):
        wrapper, sim, _ = make_wrapper()
        wrapper._check_s3_permission("op", ["s3:ListBucket"], "s3://my-bucket/")
        assert sim.calls_for("s3:ListBucket")  # premise: ListBucket was simulated
        assert [v for v in self.prefixes(sim) if v] == []
