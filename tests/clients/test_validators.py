# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from nx_neptune.validators import (
    CheckResult,
    check_athena_database,
    check_athena_table,
    check_bucket_encryption,
    check_bucket_exists,
    check_bucket_region,
    check_bucket_versioning,
    check_credentials,
    check_graph_name_available,
    check_path_empty,
    validate_resources,
)


@pytest.fixture
def mock_factory():
    with patch("nx_neptune.validators.ClientFactory") as mock_cls:
        factory = MagicMock()
        mock_cls.default.return_value = factory
        yield factory


class TestCheckBucketExists:
    def test_bucket_exists(self, mock_factory):
        mock_factory.s3.return_value.head_bucket.return_value = {}
        result = check_bucket_exists("s3://my-bucket/path/")
        assert result.passed is True
        assert "my-bucket" in result.message

    def test_bucket_not_found(self, mock_factory):
        mock_factory.s3.return_value.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "HeadBucket"
        )
        result = check_bucket_exists("s3://missing-bucket/")
        assert result.passed is False
        assert "not found" in result.message

    def test_bucket_access_denied(self, mock_factory):
        mock_factory.s3.return_value.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "403"}}, "HeadBucket"
        )
        result = check_bucket_exists("s3://private-bucket/")
        assert result.passed is False
        assert "Access denied" in result.message


class TestCheckBucketRegion:
    def test_correct_region(self, mock_factory):
        mock_factory.s3.return_value.get_bucket_location.return_value = {
            "LocationConstraint": "us-west-2"
        }
        result = check_bucket_region("s3://my-bucket/", "us-west-2")
        assert result.passed is True

    def test_wrong_region(self, mock_factory):
        mock_factory.s3.return_value.get_bucket_location.return_value = {
            "LocationConstraint": "eu-west-1"
        }
        result = check_bucket_region("s3://my-bucket/", "us-west-2")
        assert result.passed is False
        assert "eu-west-1" in result.message

    def test_us_east_1_null(self, mock_factory):
        mock_factory.s3.return_value.get_bucket_location.return_value = {
            "LocationConstraint": None
        }
        result = check_bucket_region("s3://my-bucket/", "us-east-1")
        assert result.passed is True


class TestCheckBucketEncryption:
    def test_kms_encrypted(self, mock_factory):
        mock_factory.s3.return_value.get_bucket_encryption.return_value = {
            "ServerSideEncryptionConfiguration": {
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "aws:kms",
                            "KMSMasterKeyID": "arn:aws:kms:us-west-2:123:key/abc",
                        }
                    }
                ]
            }
        }
        result = check_bucket_encryption("s3://my-bucket/")
        assert result.passed is True
        assert "KMS" in result.message

    def test_no_kms(self, mock_factory):
        mock_factory.s3.return_value.get_bucket_encryption.return_value = {
            "ServerSideEncryptionConfiguration": {
                "Rules": [
                    {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                ]
            }
        }
        result = check_bucket_encryption("s3://my-bucket/")
        assert result.passed is False


class TestCheckBucketVersioning:
    def test_versioning_enabled(self, mock_factory):
        mock_factory.s3.return_value.get_bucket_versioning.return_value = {
            "Status": "Enabled"
        }
        result = check_bucket_versioning("s3://my-bucket/")
        assert result.passed is True

    def test_versioning_disabled(self, mock_factory):
        mock_factory.s3.return_value.get_bucket_versioning.return_value = {}
        result = check_bucket_versioning("s3://my-bucket/")
        assert result.passed is False


class TestCheckPathEmpty:
    def test_path_empty(self, mock_factory):
        mock_factory.s3.return_value.list_objects_v2.return_value = {"KeyCount": 0}
        result = check_path_empty("s3://my-bucket/output/")
        assert result.passed is True

    def test_path_not_empty(self, mock_factory):
        mock_factory.s3.return_value.list_objects_v2.return_value = {"KeyCount": 1}
        result = check_path_empty("s3://my-bucket/output/")
        assert result.passed is False


class TestCheckAthenaDatabase:
    def test_database_exists(self, mock_factory):
        mock_factory.athena.return_value.get_database.return_value = {}
        result = check_athena_database("mydb")
        assert result.passed is True

    def test_database_not_found(self, mock_factory):
        mock_factory.athena.return_value.get_database.side_effect = ClientError(
            {"Error": {"Code": "EntityNotFoundException", "Message": "not found"}},
            "GetDatabase",
        )
        result = check_athena_database("missing")
        assert result.passed is False


class TestCheckAthenaTable:
    def test_table_exists(self, mock_factory):
        mock_factory.athena.return_value.get_table_metadata.return_value = {
            "TableMetadata": {"Columns": [{"Name": "~id"}, {"Name": "name"}]}
        }
        result = check_athena_table("mydb", "mytable")
        assert result.passed is True
        assert "2 columns" in result.message


class TestCheckGraphNameAvailable:
    def test_name_available(self, mock_factory):
        mock_factory.neptune.return_value.list_graphs.return_value = {"graphs": []}
        result = check_graph_name_available("my-graph")
        assert result.passed is True

    def test_name_taken(self, mock_factory):
        mock_factory.neptune.return_value.list_graphs.return_value = {
            "graphs": [{"name": "my-graph", "id": "g-abc"}]
        }
        result = check_graph_name_available("my-graph")
        assert result.passed is False
        assert "already exists" in result.message


class TestCheckCredentials:
    def test_valid_credentials(self, mock_factory):
        mock_factory.sts.return_value.get_caller_identity.return_value = {
            "Arn": "arn:aws:iam::123:user/dev"
        }
        result = check_credentials()
        assert result.passed is True

    def test_invalid_credentials(self, mock_factory):
        mock_factory.sts.return_value.get_caller_identity.side_effect = Exception(
            "expired"
        )
        result = check_credentials()
        assert result.passed is False


class TestValidateResources:
    def test_credentials_fail_short_circuits(self, mock_factory):
        mock_factory.sts.return_value.get_caller_identity.side_effect = Exception(
            "no creds"
        )
        results = validate_resources(s3_staging_bucket="s3://bucket/")
        assert len(results) == 1
        assert results[0]["passed"] is False

    def test_s3_checks_run(self, mock_factory):
        mock_factory.sts.return_value.get_caller_identity.return_value = {
            "Arn": "arn:aws:iam::123:user/dev"
        }
        mock_factory.s3.return_value.head_bucket.return_value = {}
        mock_factory.s3.return_value.get_bucket_versioning.return_value = {
            "Status": "Enabled"
        }
        results = validate_resources(s3_staging_bucket="s3://bucket/")
        checks = [r["check"] for r in results]
        assert "credentials" in checks
        assert "s3_bucket_exists" in checks
        assert "s3_bucket_versioning" in checks
