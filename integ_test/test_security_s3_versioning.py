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
Integration test proving that check_s3_versioning raises ValueError when
versioning is not enabled on a real S3 bucket, and passes when it is.

Creates a temporary S3 bucket, tests both states, then cleans up.

Usage:
    export NETWORKX_GRAPH_ID=g-your-graph-id
    pytest integ_test/test_security_s3_versioning.py -v
"""
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from nx_neptune import NETWORKX_GRAPH_ID
from nx_neptune.clients.iam_client import IamClient

BUCKET_PREFIX = "nx-neptune-versioning-test"


@pytest.fixture(scope="module")
def s3_client():
    return boto3.client("s3")


@pytest.fixture(scope="module")
def iam_client():
    if not NETWORKX_GRAPH_ID:
        pytest.skip('Environment Variable "NETWORKX_GRAPH_ID" is not defined')

    sts_arn = boto3.client("sts").get_caller_identity()["Arn"]
    return IamClient(role_arn=sts_arn, client=boto3.client("iam"))


@pytest.fixture(scope="module")
def temp_bucket(s3_client):
    """Create a temporary S3 bucket and delete it after tests."""
    bucket_name = f"{BUCKET_PREFIX}-{uuid.uuid4().hex[:8]}"
    region = s3_client.meta.region_name

    create_kwargs = {"Bucket": bucket_name}
    if region and region != "us-east-1":
        create_kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}

    s3_client.create_bucket(**create_kwargs)

    yield bucket_name

    # Cleanup
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
    except ClientError:
        pass


class TestS3VersioningCheck:
    """Prove check_s3_versioning enforces versioning on a real bucket."""

    def test_raises_when_versioning_disabled(self, iam_client, temp_bucket):
        """New buckets have versioning disabled — must raise."""
        with pytest.raises(ValueError, match="does not have versioning enabled"):
            iam_client.check_s3_versioning_enabled(f"s3://{temp_bucket}/")

    def test_passes_when_versioning_enabled(self, iam_client, temp_bucket, s3_client):
        """After enabling versioning — must return True."""
        s3_client.put_bucket_versioning(
            Bucket=temp_bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )
        assert iam_client.check_s3_versioning_enabled(f"s3://{temp_bucket}/") is True

    def test_raises_when_versioning_suspended(self, iam_client, temp_bucket, s3_client):
        """After suspending versioning — must raise."""
        s3_client.put_bucket_versioning(
            Bucket=temp_bucket,
            VersioningConfiguration={"Status": "Suspended"},
        )
        with pytest.raises(ValueError, match="does not have versioning enabled"):
            iam_client.check_s3_versioning_enabled(f"s3://{temp_bucket}/")
