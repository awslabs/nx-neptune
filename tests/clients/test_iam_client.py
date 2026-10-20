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
    IamClientWrapper,
    _convert_sts_to_iam_arn,
    _get_s3_in_arn,
    _s3_simulation_targets,
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
    """Test _get_s3_in_arn with various input formats."""
    result = _get_s3_in_arn(s3_path)
    assert result == expected


def test_get_s3_in_arn_gov_partition():
    assert _get_s3_in_arn("s3://my-bucket/my-folder", "aws-us-gov") == (
        "arn:aws-us-gov:s3:::my-bucket/my-folder"
    )


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
        (
            "arn:aws-us-gov:sts::123456789012:assumed-role/ROLE_NAME/SESSION_NAME",
            "arn:aws-us-gov:iam::123456789012:role/ROLE_NAME",
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
        from unittest.mock import MagicMock, patch

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
        from unittest.mock import MagicMock, patch

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
        from unittest.mock import MagicMock, patch

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

    def test_create_graph_uses_star_and_tag_uses_graph_arn(self, mock_iam_client):
        """CreateGraph stays on *; TagResource is simulated against graph/* (#576)."""
        iam_client, mock_client = mock_iam_client
        mock_client.meta.region_name = "us-west-2"
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

        iam_client.has_create_na_permissions()

        calls = mock_client.simulate_principal_policy.call_args_list
        resources = [c.kwargs["ResourceArns"][0] for c in calls]
        assert "*" in resources
        assert "arn:aws:neptune-graph:us-west-2:123456789012:graph/*" in resources
        create_call = next(
            c for c in calls if "neptune-graph:CreateGraph" in c.kwargs["ActionNames"]
        )
        tag_call = next(
            c for c in calls if "neptune-graph:TagResource" in c.kwargs["ActionNames"]
        )
        assert create_call.kwargs["ResourceArns"] == ["*"]
        assert tag_call.kwargs["ResourceArns"] == [
            "arn:aws:neptune-graph:us-west-2:123456789012:graph/*"
        ]
        assert tag_call.kwargs["ContextEntries"][0]["ContextKeyName"] == (
            "aws:RequestTag/agent"
        )
        assert tag_call.kwargs["ContextEntries"][0]["ContextKeyValues"] == [
            "nx-neptune"
        ]

    def test_least_privilege_tag_resource_denied_on_star(self, mock_iam_client):
        """A graph-scoped TagResource policy fails if simulated against *."""
        iam_client, mock_client = mock_iam_client
        mock_client.meta.region_name = "us-east-1"

        def mock_simulate(**kwargs):
            action_names = kwargs["ActionNames"]
            resource = kwargs["ResourceArns"][0]
            results = []
            for action in action_names:
                if action == "neptune-graph:TagResource" and resource == "*":
                    decision = "implicitDeny"
                else:
                    decision = "allowed"
                results.append({"EvalActionName": action, "EvalDecision": decision})
            return {"EvaluationResults": results}

        mock_client.simulate_principal_policy.side_effect = mock_simulate
        iam_client.has_create_na_permissions()

    def test_s3_list_bucket_uses_bucket_arn(self, mock_iam_client):
        """ListBucket is simulated on the bucket; object actions on prefix/*."""
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
                {"EvalActionName": "s3:DeleteObject", "EvalDecision": "allowed"},
                {"EvalActionName": "s3:ListBucket", "EvalDecision": "allowed"},
            ]
        }
        from unittest.mock import patch

        with patch.object(iam_client, "check_s3_versioning_enabled"):
            iam_client.has_export_to_s3_permissions("s3://stage/proj-1/")

        calls = mock_client.simulate_principal_policy.call_args_list
        by_action = {
            tuple(c.kwargs["ActionNames"]): c.kwargs["ResourceArns"][0] for c in calls
        }
        assert by_action[("s3:ListBucket",)] == "arn:aws:s3:::stage"
        assert by_action[("s3:PutObject", "s3:DeleteObject")] == (
            "arn:aws:s3:::stage/proj-1/*"
        )

    def test_gov_partition_neptune_arn(self, mock_iam_client):
        iam_client, mock_client = mock_iam_client
        iam_client.role_arn = "arn:aws-us-gov:iam::123456789012:role/test-role"
        mock_client.meta.region_name = "us-gov-west-1"
        mock_client.simulate_principal_policy.return_value = {
            "EvaluationResults": [
                {
                    "EvalActionName": "neptune-graph:DeleteGraph",
                    "EvalDecision": "allowed",
                }
            ]
        }
        iam_client.has_delete_na_permissions()
        kwargs = mock_client.simulate_principal_policy.call_args.kwargs
        assert kwargs["ResourceArns"] == [
            "arn:aws-us-gov:neptune-graph:us-gov-west-1:123456789012:graph/*"
        ]


@pytest.mark.parametrize(
    "s3_path,partition,bucket,obj",
    [
        (
            "s3://stage/proj-1/",
            "aws",
            "arn:aws:s3:::stage",
            "arn:aws:s3:::stage/proj-1/*",
        ),
        ("s3://stage/", "aws", "arn:aws:s3:::stage", "arn:aws:s3:::stage/*"),
        (
            "arn:aws-us-gov:s3:::stage/proj-1",
            "aws",
            "arn:aws-us-gov:s3:::stage",
            "arn:aws-us-gov:s3:::stage/proj-1/*",
        ),
    ],
)
def test_s3_simulation_targets(s3_path, partition, bucket, obj):
    assert _s3_simulation_targets(s3_path, partition) == (bucket, obj)
