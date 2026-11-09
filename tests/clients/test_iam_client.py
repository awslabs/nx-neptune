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

import pytest

from nx_neptune.clients.iam_client import (
    IamClientWrapper,
    _convert_sts_to_iam_arn,
    split_s3_arn_to_bucket_and_path,
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


@pytest.mark.parametrize(
    "role_arn",
    [
        "arn:aws:iam::123456789012:role/tests-role",
        "arn:aws:iam::123456789012:role/analysts",
        "arn:aws:iam::123456789012:role/my-posts-role",
        "arn:aws:iam::123456789012:role/analysts/my-role",
        "arn:aws:iam::123456789012:role/path/to/tests-role",
        # Non-aws partition, IAM service
        "arn:aws-us-gov:iam::123456789012:role/tests-role",
    ],
)
def test_wrapper_keeps_iam_role_arn_unchanged_when_it_contains_sts(role_arn):
    """An IAM role ARN is never converted, whatever characters the name/path has."""
    from unittest.mock import MagicMock

    wrapper = IamClientWrapper(role_arn=role_arn, client=MagicMock())
    assert wrapper.role_arn == role_arn


@pytest.mark.parametrize(
    "sts_arn,expected",
    [
        (
            "arn:aws:sts::123456789012:assumed-role/MyRole/session",
            "arn:aws:iam::123456789012:role/MyRole",
        ),
        (
            "arn:aws:sts::123456789012:assumed-role/path/MyRole/session",
            "arn:aws:iam::123456789012:role/path/MyRole",
        ),
        # Non-aws partition, STS service
        (
            "arn:aws-cn:sts::123456789012:assumed-role/MyRole/session",
            "arn:aws-cn:iam::123456789012:role/MyRole",
        ),
    ],
)
def test_wrapper_converts_sts_assumed_role_arn(sts_arn, expected):
    from unittest.mock import MagicMock

    wrapper = IamClientWrapper(role_arn=sts_arn, client=MagicMock())
    assert wrapper.role_arn == expected


_INVALID_STS_MSG = "Input is not a valid STS assumed-role ARN"


@pytest.mark.parametrize("short", ["", "sts", "arn:aws", "arn:sts"])
def test_wrapper_stores_input_with_fewer_than_three_parts_unchanged(short):
    """The len(arn_parts) > 2 guard: short strings are stored as-is, no raise."""
    from unittest.mock import MagicMock

    wrapper = IamClientWrapper(role_arn=short, client=MagicMock())
    assert wrapper.role_arn == short


@pytest.mark.parametrize(
    "bad_arn",
    [
        "arn:aws:sts",  # 3 fields
        "arn:aws:sts::123456789012",  # 5 fields
        "arn:aws:sts:us-east-1:123456789012",  # 5 fields
    ],
)
def test_convert_rejects_sts_arn_with_too_few_fields(bad_arn):
    with pytest.raises(ValueError) as exc:
        _convert_sts_to_iam_arn(bad_arn)
    assert str(exc.value) == _INVALID_STS_MSG


@pytest.mark.parametrize(
    "bad_arn",
    [
        "notarn:aws:sts::1:assumed-role/R/s",
        "ARN:aws:sts::1:assumed-role/R/s",
        ":aws:sts::1:assumed-role/R/s",
    ],
)
def test_convert_rejects_arn_without_arn_prefix(bad_arn):
    with pytest.raises(ValueError) as exc:
        _convert_sts_to_iam_arn(bad_arn)
    assert str(exc.value) == _INVALID_STS_MSG


def test_convert_rejects_empty_partition():
    with pytest.raises(ValueError) as exc:
        _convert_sts_to_iam_arn("arn::sts::1:assumed-role/R/s")
    assert str(exc.value) == _INVALID_STS_MSG


def test_convert_rejects_non_sts_service():
    with pytest.raises(ValueError) as exc:
        _convert_sts_to_iam_arn("arn:aws:iam::1:role/R")
    assert str(exc.value) == _INVALID_STS_MSG


@pytest.mark.parametrize(
    "bad_arn",
    [
        "a:b:sts",  # sts in slot 2 but only 3 fields
        "notarn:aws:sts::1:assumed-role/R/s",
        "arn::sts::1:assumed-role/R/s",
    ],
)
def test_wrapper_propagates_invalid_sts_error(bad_arn):
    """Once slot 2 is 'sts', a malformed ARN raises rather than passing through."""
    from unittest.mock import MagicMock

    with pytest.raises(ValueError) as exc:
        IamClientWrapper(role_arn=bad_arn, client=MagicMock())
    assert str(exc.value) == _INVALID_STS_MSG


# CHARACTERISATION ONLY: documents current behaviour, NOT a requirement.
# These inputs lack a session segment or are not assumed-role ARNs, and the
# converter currently yields a role ARN with an empty role name. If this is
# later fixed to raise, update these tests deliberately.
@pytest.mark.parametrize(
    "arn,current_output",
    [
        ("arn:aws:sts::1:assumed-role/R", "arn:aws:iam::1:role/"),
        ("arn:aws:sts::1:federated-user/x", "arn:aws:iam::1:role/"),
    ],
)
def test_characterisation_convert_yields_empty_role_name(arn, current_output):
    assert _convert_sts_to_iam_arn(arn) == current_output


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


class TestSimulatedResourceArns:
    """The preflight must simulate against the resource ARN IAM really evaluates.

    Simulating Neptune actions against "*" and putting bucket- and object-level S3
    actions on one ARN made IAM report a correctly scoped least-privilege policy as
    insufficient, so the operation aborted
    (https://github.com/awslabs/nx-neptune/issues/576).
    """

    ROLE = "arn:aws:iam::123456789012:role/test-role"
    REGION = "us-west-2"

    def _wrapper(self, role=None, region=REGION):
        """A wrapper over a simulator fake that allows whatever is asked of it.

        These tests assert on the request the preflight builds, not on the
        decision, so an allow-everything simulator keeps them focused on the ARNs.
        """
        from unittest.mock import MagicMock

        client = MagicMock()

        def simulate(**kwargs):
            return {
                "EvaluationResults": [
                    {"EvalActionName": action, "EvalDecision": "allowed"}
                    for action in kwargs["ActionNames"]
                ]
            }

        client.simulate_principal_policy.side_effect = simulate
        return (
            IamClientWrapper(role_arn=role or self.ROLE, client=client, region=region),
            client,
        )

    @staticmethod
    def _requests(client):
        """(ResourceArns, ActionNames) of every simulate call, in order."""
        return [
            (c.kwargs["ResourceArns"], c.kwargs["ActionNames"])
            for c in client.simulate_principal_policy.call_args_list
        ]

    # --- Neptune Analytics resource ARNs -------------------------------------

    def test_graph_actions_simulate_against_the_graph_arn_not_a_wildcard(self):
        wrapper, client = self._wrapper()

        wrapper.has_create_na_permissions()

        arns, actions = self._requests(client)[0]
        assert arns == [
            "arn:aws:neptune-graph:us-west-2:123456789012:graph/*"
        ], "simulating against '*' is what #576 reports"
        assert set(actions) == {
            "neptune-graph:CreateGraph",
            "neptune-graph:TagResource",
        }

    def test_snapshot_actions_simulate_against_the_snapshot_arn(self):
        wrapper, client = self._wrapper()

        wrapper.has_delete_snapshot_permissions()

        arns, actions = self._requests(client)[0]
        assert arns == ["arn:aws:neptune-graph:us-west-2:123456789012:graph-snapshot/*"]
        assert actions == ["neptune-graph:DeleteGraphSnapshot"]

    def test_mixed_actions_are_split_by_resource_type(self):
        """A snapshot action and a graph action cannot share one request."""
        wrapper, client = self._wrapper()

        wrapper.has_create_na_snapshot_permissions()

        by_arn = {arns[0]: set(actions) for arns, actions in self._requests(client)}
        assert by_arn == {
            "arn:aws:neptune-graph:us-west-2:123456789012:graph/*": {
                "neptune-graph:TagResource"
            },
            "arn:aws:neptune-graph:us-west-2:123456789012:graph-snapshot/*": {
                "neptune-graph:CreateGraphSnapshot"
            },
        }

    def test_partition_comes_from_the_role_arn(self):
        wrapper, client = self._wrapper(
            role="arn:aws-us-gov:iam::123456789012:role/test-role"
        )

        wrapper.has_start_na_permissions()

        arns, _ = self._requests(client)[0]
        assert arns == ["arn:aws-us-gov:neptune-graph:us-west-2:123456789012:graph/*"]

    def test_region_falls_back_to_a_wildcard_segment_when_unset(self):
        """Keeps the ARN well formed rather than embedding "None"."""
        from unittest.mock import patch

        wrapper, client = self._wrapper(region=None)
        with patch("nx_neptune.clients.iam_client.boto3") as mock_boto3:
            mock_boto3.DEFAULT_SESSION = None
            mock_boto3.Session.return_value.region_name = None

            wrapper.has_start_na_permissions()

        arns, _ = self._requests(client)[0]
        assert arns == ["arn:aws:neptune-graph:*:123456789012:graph/*"]

    def test_region_prefers_the_default_session_over_a_fresh_one(self):
        """ClientFactory's clients resolve through the default session, so this agrees."""
        from unittest.mock import MagicMock, patch

        wrapper, client = self._wrapper(region=None)
        with patch("nx_neptune.clients.iam_client.boto3") as mock_boto3:
            mock_boto3.DEFAULT_SESSION = MagicMock(region_name="eu-central-1")
            mock_boto3.Session.return_value.region_name = "us-east-1"

            wrapper.has_start_na_permissions()

        arns, _ = self._requests(client)[0]
        assert arns == ["arn:aws:neptune-graph:eu-central-1:123456789012:graph/*"]

    # --- S3 bucket-level vs object-level ------------------------------------

    def test_s3_bucket_and_object_actions_use_different_arns(self):
        wrapper, client = self._wrapper()
        wrapper.check_assume_role = lambda service_name: True
        wrapper.check_s3_versioning_enabled = lambda *a, **k: None

        wrapper.has_export_to_s3_permissions("s3://my-bucket/staging")

        by_arn = {arns[0]: set(actions) for arns, actions in self._requests(client)}
        assert by_arn == {
            "arn:aws:s3:::my-bucket": {"s3:ListBucket"},
            "arn:aws:s3:::my-bucket/staging/*": {"s3:PutObject", "s3:DeleteObject"},
        }

    def test_s3_object_arn_without_a_prefix_covers_the_whole_bucket(self):
        wrapper, client = self._wrapper()
        wrapper.check_assume_role = lambda service_name: True
        wrapper.check_s3_versioning_enabled = lambda *a, **k: None

        wrapper.has_export_to_s3_permissions("s3://my-bucket")

        by_arn = {arns[0]: set(actions) for arns, actions in self._requests(client)}
        assert "arn:aws:s3:::my-bucket/*" in by_arn
        assert by_arn["arn:aws:s3:::my-bucket"] == {"s3:ListBucket"}

    def test_s3_arns_carry_the_role_partition(self):
        wrapper, client = self._wrapper(
            role="arn:aws-cn:iam::123456789012:role/test-role"
        )
        wrapper.check_assume_role = lambda service_name: True
        wrapper.check_s3_versioning_enabled = lambda *a, **k: None

        wrapper.has_delete_s3_permissions("s3://my-bucket/staging")

        for arns, _ in self._requests(client):
            assert arns[0].startswith("arn:aws-cn:s3:::")

    # --- a path naming one object, not a prefix ------------------------------

    def _wrapper_allowing(self, allowed, role=None, region=REGION):
        """A simulator fake that allows an action only on the ARNs in ``allowed``.

        ``allowed`` maps a resource ARN to the actions permitted on it; anything
        else is denied, which is how a real least-privilege policy behaves.
        """
        from unittest.mock import MagicMock

        client = MagicMock()

        def simulate(**kwargs):
            arn = kwargs["ResourceArns"][0]
            permitted = allowed.get(arn, set())
            return {
                "EvaluationResults": [
                    {
                        "EvalActionName": action,
                        "EvalDecision": (
                            "allowed" if action in permitted else "implicitDeny"
                        ),
                    }
                    for action in kwargs["ActionNames"]
                ]
            }

        client.simulate_principal_policy.side_effect = simulate
        wrapper = IamClientWrapper(
            role_arn=role or self.ROLE, client=client, region=region
        )
        wrapper.check_assume_role = lambda service_name: True
        wrapper.check_s3_versioning_enabled = lambda *a, **k: None
        return wrapper, client

    def test_an_exact_object_grant_is_accepted(self):
        """A path can name one object, and nothing in it says so.

        Appending "/*" unconditionally meant a grant written against the object
        itself no longer matched, and the preflight reported the action missing.
        """
        wrapper, client = self._wrapper_allowing(
            {
                "arn:aws:s3:::my-bucket": {"s3:ListBucket"},
                "arn:aws:s3:::my-bucket/data/file.csv": {
                    "s3:PutObject",
                    "s3:DeleteObject",
                },
            }
        )

        wrapper.has_export_to_s3_permissions("s3://my-bucket/data/file.csv")

        tried = [arns[0] for arns, _ in self._requests(client)]
        assert "arn:aws:s3:::my-bucket/data/file.csv/*" in tried, "prefix form first"
        assert "arn:aws:s3:::my-bucket/data/file.csv" in tried, "exact object fallback"

    def test_a_prefix_grant_is_still_accepted_without_a_second_request(self):
        """The common case must not pay for the fallback."""
        wrapper, client = self._wrapper_allowing(
            {
                "arn:aws:s3:::my-bucket": {"s3:ListBucket"},
                "arn:aws:s3:::my-bucket/staging/*": {
                    "s3:PutObject",
                    "s3:DeleteObject",
                },
            }
        )

        wrapper.has_export_to_s3_permissions("s3://my-bucket/staging")

        tried = [arns[0] for arns, _ in self._requests(client)]
        assert "arn:aws:s3:::my-bucket/staging" not in tried

    def test_neither_object_form_granted_still_fails(self):
        """The fallback widens what counts as sufficient; it must not accept nothing."""
        wrapper, _ = self._wrapper_allowing(
            {"arn:aws:s3:::my-bucket": {"s3:ListBucket"}}
        )

        with pytest.raises(ValueError, match="Insufficient permission"):
            wrapper.has_export_to_s3_permissions("s3://my-bucket/staging")

    # --- the public entry points use the same ARNs as the preflights ---------

    def test_validate_permissions_uses_the_real_resource_arns(self):
        """It reported a correctly scoped policy as insufficient while the
        narrower preflights passed, so the two contradicted each other."""
        wrapper, client = self._wrapper_allowing(
            {
                "arn:aws:neptune-graph:us-west-2:123456789012:graph/*": {
                    "neptune-graph:CreateGraph",
                    "neptune-graph:TagResource",
                },
                "arn:aws:s3:::my-bucket": {"s3:ListBucket"},
                "arn:aws:s3:::my-bucket/staging/*": {
                    "s3:PutObject",
                    "s3:DeleteObject",
                },
                # With no KMS key configured these still simulate against "*",
                # which this PR does not change.
                "*": {"kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"},
            }
        )

        results = wrapper.validate_permissions(
            None, None, "s3://my-bucket/staging", None
        )

        assert results["create_na_instance"] is True
        assert results["export_csv_to_s3"] is True

    def test_athena_s3_check_splits_bucket_and_object_actions(self):
        wrapper, client = self._wrapper_allowing(
            {
                "*": {"athena:StartQueryExecution", "athena:GetQueryExecution"},
                "arn:aws:s3:::results-bucket": {"s3:ListBucket"},
                "arn:aws:s3:::results-bucket/out/*": {"s3:GetObject", "s3:PutObject"},
            }
        )

        assert wrapper.has_athena_permissions("s3://results-bucket/out") is True

        by_arn = {arns[0]: set(actions) for arns, actions in self._requests(client)}
        assert by_arn["arn:aws:s3:::results-bucket"] == {"s3:ListBucket"}
        assert by_arn["arn:aws:s3:::results-bucket/out/*"] == {
            "s3:GetObject",
            "s3:PutObject",
        }


@pytest.mark.parametrize(
    "sts_arn,expected",
    [
        (
            "arn:aws-us-gov:sts::123456789012:assumed-role/my-role/session",
            "arn:aws-us-gov:iam::123456789012:role/my-role",
        ),
        (
            "arn:aws-cn:sts::123456789012:assumed-role/path/my-role/session",
            "arn:aws-cn:iam::123456789012:role/path/my-role",
        ),
    ],
)
def test_sts_to_iam_arn_preserves_the_partition(sts_arn, expected):
    """A hard-coded 'aws' partition produced an ARN no policy in aws-cn or
    aws-us-gov could match."""
    assert _convert_sts_to_iam_arn(sts_arn) == expected
