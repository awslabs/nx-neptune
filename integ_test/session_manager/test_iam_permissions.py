# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for IAM permission checks against real S3 buckets."""

import os

import pytest

from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path

S3_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")


class TestValidatePermissions:

    def test_validate_permissions_returns_dict(self, session_manager):
        result = session_manager.validate_permissions()
        assert isinstance(result, dict)
        assert len(result) > 0


@pytest.mark.skipif(not S3_BUCKET, reason="NETWORKX_S3_EXPORT_BUCKET_PATH not set")
class TestS3PermissionChecks:

    def test_has_import_permissions(self, iam_client):
        iam_client.has_import_from_s3_permissions(S3_BUCKET)

    def test_has_export_permissions(self, iam_client):
        iam_client.has_export_to_s3_permissions(S3_BUCKET)

    def test_has_delete_permissions(self, iam_client):
        iam_client.has_delete_s3_permissions(S3_BUCKET)

    def test_s3_versioning_enabled(self, iam_client):
        iam_client.check_s3_versioning_enabled(S3_BUCKET)


class TestS3ArnParsing:

    def test_split_s3_arn_bucket_and_path(self):
        bucket, path = split_s3_arn_to_bucket_and_path("s3://my-bucket/some/path/")
        assert bucket == "my-bucket"
        assert path == "some/path/"

    def test_split_s3_arn_bucket_and_path_no_slash(self):
        bucket, path = split_s3_arn_to_bucket_and_path("s3://my-bucket/some/path")
        assert bucket == "my-bucket"
        assert path == "some/path"

    def test_split_s3_arn_no_path(self):
        bucket, path = split_s3_arn_to_bucket_and_path("s3://my-bucket")
        assert bucket == "my-bucket"
        assert path == ""

    def test_split_s3_arn_with_slash(self):
        bucket, path = split_s3_arn_to_bucket_and_path("s3://my-bucket/")
        assert bucket == "my-bucket"
        assert path == ""
