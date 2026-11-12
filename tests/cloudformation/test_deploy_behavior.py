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
Behavioral tests for deploy.sh: run the script against a stubbed AWS CLI and
assert on what it does per ownership scenario, rather than on its source text.
The core property under test: deploy never uploads to a bucket this account does
not own.
"""


def _put_objects(calls):
    return [c for c in calls if c.startswith("s3api put-object")]


def test_fresh_bucket_is_created_then_uploaded(run_script):
    # Not present (head fails), create succeeds -> upload with owner assertion.
    proc, calls = run_script("deploy.sh", STUB_HEAD="404", STUB_CREATE="ok")
    assert proc.returncode == 0, proc.stderr
    assert any(c.startswith("s3api create-bucket") for c in calls)
    puts = _put_objects(calls)
    assert puts, "expected notebooks.zip to be uploaded"
    assert all("--expected-bucket-owner" in c for c in puts)
    # The old 's3 mb' silent-adopt path must not be used.
    assert not any("s3 mb " in c for c in calls)


def test_bucket_owned_by_another_account_is_refused(run_script):
    # Head fails (exists, not ours) and create reports the name taken elsewhere.
    proc, calls = run_script("deploy.sh", STUB_HEAD="403", STUB_CREATE="exists")
    assert proc.returncode != 0
    assert not _put_objects(calls), "must not upload to a bucket we do not own"


def test_reused_bucket_in_expected_region_is_uploaded(run_script):
    proc, calls = run_script("deploy.sh", STUB_HEAD="0", STUB_LOCATION="us-west-1")
    assert proc.returncode == 0, proc.stderr
    assert not any(c.startswith("s3api create-bucket") for c in calls)
    assert _put_objects(calls)


def test_reused_bucket_in_wrong_region_is_refused(run_script):
    proc, calls = run_script("deploy.sh", STUB_HEAD="0", STUB_LOCATION="eu-west-1")
    assert proc.returncode != 0
    assert not _put_objects(calls), "must not upload to a bucket in the wrong region"
