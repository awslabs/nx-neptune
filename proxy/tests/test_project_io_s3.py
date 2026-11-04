# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


class TestSanitizeName:
    def test_spaces_to_underscores(self):
        from nx_neptune_proxy.utils.sanitize import sanitize_s3_key_name

        assert sanitize_s3_key_name("Fraud Detection") == "Fraud_Detection"

    def test_special_characters_stripped(self):
        from nx_neptune_proxy.utils.sanitize import sanitize_s3_key_name

        assert sanitize_s3_key_name("My Project! (v2)") == "My_Project_v2"

    def test_hyphens_kept(self):
        from nx_neptune_proxy.utils.sanitize import sanitize_s3_key_name

        assert sanitize_s3_key_name("fraud-detection") == "fraud-detection"

    def test_underscores_kept(self):
        from nx_neptune_proxy.utils.sanitize import sanitize_s3_key_name

        assert sanitize_s3_key_name("my_project") == "my_project"


class TestParseBucketConfig:
    def test_bucket_only(self):
        from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path

        bucket, prefix = split_s3_arn_to_bucket_and_path("s3://my-bucket")
        assert bucket == "my-bucket"
        assert prefix == ""

    def test_bucket_with_prefix(self):
        from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path

        bucket, prefix = split_s3_arn_to_bucket_and_path("s3://my-bucket/team/exports")
        assert bucket == "my-bucket"
        assert prefix == "team/exports"
