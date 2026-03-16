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
"""
Security test demonstrating that S3 bucket versioning is checked whenever
permission checks involve s3:PutObject or s3:DeleteObject operations.

This ensures data protection by warning users when versioning is not enabled
on buckets used for write or delete operations.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from nx_neptune.clients.iam_client import IamClient
from nx_neptune.instance_management import (
    export_csv_to_s3,
    export_athena_table_to_s3,
    empty_s3_bucket,
)


ROLE_ARN = "arn:aws:iam::123456789012:role/TestRole"
BUCKET_ARN = "s3://test-bucket/data"


def _create_iam_client():
    """Create an IamClient with mocked dependencies."""
    mock_boto_client = MagicMock()
    client = IamClient(role_arn=ROLE_ARN, client=mock_boto_client)
    # Stub out permission checks so tests focus on versioning
    client.check_aws_permission = MagicMock(return_value={})
    client.check_assume_role = MagicMock(return_value=True)
    return client


def test_versioning_checked_on_export():
    """has_export_to_s3_permissions must check S3 versioning separately (uses s3:PutObject and s3:DeleteObject)."""
    print("=== Test: has_export_to_s3_permissions checks versioning ===")

    client = _create_iam_client()
    with patch.object(client, "check_s3_versioning") as mock_versioning:
        client.has_export_to_s3_permissions(BUCKET_ARN)
        mock_versioning.assert_called_once_with(BUCKET_ARN)

    print("  PASS: check_s3_versioning called separately during export permission check")
    print()


def test_versioning_checked_on_delete():
    """has_delete_s3_permissions must check S3 versioning separately (uses s3:DeleteObject)."""
    print("=== Test: has_delete_s3_permissions checks versioning ===")

    client = _create_iam_client()
    with patch.object(client, "check_s3_versioning") as mock_versioning:
        client.has_delete_s3_permissions(BUCKET_ARN)
        mock_versioning.assert_called_once_with(BUCKET_ARN)

    print("  PASS: check_s3_versioning called separately during delete permission check")
    print()


def test_versioning_checked_on_athena_with_s3():
    """has_athena_permissions must check S3 versioning separately when s3_bucket is provided (uses s3:PutObject)."""
    print("=== Test: has_athena_permissions checks versioning ===")

    client = _create_iam_client()
    with patch.object(client, "check_s3_versioning") as mock_versioning:
        client.has_athena_permissions(s3_bucket=BUCKET_ARN)
        mock_versioning.assert_called_once_with(BUCKET_ARN)

    print("  PASS: check_s3_versioning called separately during Athena permission check")
    print()


def test_versioning_not_checked_on_import():
    """has_import_from_s3_permissions must NOT check versioning (only uses s3:GetObject)."""
    print("=== Test: has_import_from_s3_permissions skips versioning ===")

    client = _create_iam_client()
    with patch.object(client, "check_s3_versioning") as mock_versioning:
        client.has_import_from_s3_permissions(BUCKET_ARN)
        mock_versioning.assert_not_called()

    print("  PASS: check_s3_versioning NOT called for read-only import")
    print()


def test_versioning_decoupled_from_kms_check():
    """check_s3_versioning must NOT be called inside _s3_kms_permission_check."""
    print("=== Test: versioning is decoupled from _s3_kms_permission_check ===")

    client = _create_iam_client()
    with patch.object(client, "check_s3_versioning") as mock_versioning:
        client._s3_kms_permission_check(
            "test-op", BUCKET_ARN, None, ["s3:PutObject", "s3:DeleteObject"], []
        )
        mock_versioning.assert_not_called()

    print("  PASS: _s3_kms_permission_check does not call check_s3_versioning")
    print()


def test_export_csv_to_s3_triggers_versioning_check():
    """High-level export_csv_to_s3 must trigger versioning check via has_export_to_s3_permissions."""
    print("=== Test: export_csv_to_s3 triggers versioning check ===")

    s3_arn = "s3://export-bucket/output/"

    mock_na_graph = MagicMock()
    mock_na_graph.na_client.graph_id = "g-test123"
    mock_na_graph.na_client.client = MagicMock()
    mock_na_graph.iam_client = _create_iam_client()
    mock_na_graph.iam_client.role_arn = ROLE_ARN

    with patch.object(
        mock_na_graph.iam_client, "check_s3_versioning"
    ) as mock_versioning, patch(
        "nx_neptune.instance_management._get_bucket_encryption_key_arn",
        return_value=None,
    ), patch(
        "nx_neptune.instance_management._start_export_task",
        return_value="task-123",
    ), patch(
        "nx_neptune.instance_management.TaskFuture"
    ) as MockFuture:
        mock_future = MagicMock()
        mock_future.wait_until_complete = AsyncMock()
        MockFuture.return_value = mock_future

        asyncio.run(export_csv_to_s3(mock_na_graph, s3_arn))

        mock_versioning.assert_called_once_with(s3_arn)

    print("  PASS: export_csv_to_s3 triggers check_s3_versioning")
    print()


def test_empty_s3_bucket_triggers_versioning_check():
    """High-level empty_s3_bucket must trigger versioning check via has_delete_s3_permissions."""
    print("=== Test: empty_s3_bucket triggers versioning check ===")

    s3_arn = "s3://cleanup-bucket/data/"

    mock_iam_client = _create_iam_client()

    with patch.object(
        mock_iam_client, "check_s3_versioning"
    ) as mock_versioning, patch(
        "nx_neptune.instance_management._get_or_create_clients",
        return_value=(mock_iam_client, MagicMock(), MagicMock()),
    ), patch(
        "nx_neptune.instance_management.boto3"
    ) as mock_boto3:
        mock_s3 = MagicMock()
        mock_s3.get_paginator.return_value.paginate.return_value = []
        mock_boto3.client.return_value = mock_s3

        empty_s3_bucket(s3_arn)

        mock_versioning.assert_called_once_with(s3_arn)

    print("  PASS: empty_s3_bucket triggers check_s3_versioning")
    print()


def test_export_athena_table_triggers_versioning_check():
    """High-level export_athena_table_to_s3 must trigger versioning check via has_athena_permissions."""
    print("=== Test: export_athena_table_to_s3 triggers versioning check ===")

    s3_bucket = "s3://athena-output-bucket/results/"
    mock_iam_client = _create_iam_client()
    mock_athena_client = MagicMock()
    mock_athena_client.start_query_execution.return_value = {
        "QueryExecutionId": "query-123"
    }

    with patch.object(
        mock_iam_client, "check_s3_versioning"
    ) as mock_versioning, patch(
        "nx_neptune.instance_management._get_or_create_clients",
        return_value=(mock_iam_client, MagicMock(), mock_athena_client),
    ), patch(
        "nx_neptune.instance_management._get_bucket_encryption_key_arn",
        return_value=None,
    ), patch(
        "nx_neptune.instance_management.wait_until_all_complete",
        new_callable=AsyncMock,
    ), patch(
        "nx_neptune.instance_management.boto3"
    ) as mock_boto3, patch(
        "nx_neptune.instance_management.split_s3_arn_to_bucket_and_path",
        return_value=("athena-output-bucket", "results/"),
    ):
        mock_s3 = MagicMock()
        mock_boto3.client.return_value = mock_s3

        asyncio.run(
            export_athena_table_to_s3(
                sql_queries=["SELECT * FROM t WHERE id = ?"],
                sql_parameters=[["val"]],
                s3_bucket=s3_bucket,
            )
        )

        mock_versioning.assert_called_once_with(s3_bucket)

    print("  PASS: export_athena_table_to_s3 triggers check_s3_versioning")
    print()


def test_versioning_enabled_returns_true():
    """check_s3_versioning returns True when versioning is enabled."""
    print("=== Test: check_s3_versioning returns True when enabled ===")

    client = _create_iam_client()
    mock_s3 = MagicMock()
    mock_s3.get_bucket_versioning.return_value = {"Status": "Enabled"}

    with patch("nx_neptune.clients.iam_client.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = client.check_s3_versioning(BUCKET_ARN)

    assert result is True, f"Expected True, got {result}"
    print("  PASS: returns True when versioning is Enabled")
    print()


def test_versioning_disabled_returns_false():
    """check_s3_versioning returns False and logs warning when versioning is not enabled."""
    print("=== Test: check_s3_versioning returns False when disabled ===")

    client = _create_iam_client()
    mock_s3 = MagicMock()
    mock_s3.get_bucket_versioning.return_value = {}

    with patch("nx_neptune.clients.iam_client.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = client.check_s3_versioning(BUCKET_ARN)

    assert result is False, f"Expected False, got {result}"
    print("  PASS: returns False when versioning is not enabled")
    print()


def test_versioning_suspended_returns_false():
    """check_s3_versioning returns False when versioning is suspended."""
    print("=== Test: check_s3_versioning returns False when suspended ===")

    client = _create_iam_client()
    mock_s3 = MagicMock()
    mock_s3.get_bucket_versioning.return_value = {"Status": "Suspended"}

    with patch("nx_neptune.clients.iam_client.boto3") as mock_boto3:
        mock_boto3.client.return_value = mock_s3
        result = client.check_s3_versioning(BUCKET_ARN)

    assert result is False, f"Expected False, got {result}"
    print("  PASS: returns False when versioning is Suspended")
    print()


def main():
    test_versioning_checked_on_export()
    test_versioning_checked_on_delete()
    test_versioning_checked_on_athena_with_s3()
    test_versioning_not_checked_on_import()
    test_versioning_decoupled_from_kms_check()
    test_export_csv_to_s3_triggers_versioning_check()
    test_empty_s3_bucket_triggers_versioning_check()
    test_export_athena_table_triggers_versioning_check()
    test_versioning_enabled_returns_true()
    test_versioning_disabled_returns_false()
    test_versioning_suspended_returns_false()
    print("=" * 50)
    print("All S3 versioning security tests passed.")


if __name__ == "__main__":
    main()
