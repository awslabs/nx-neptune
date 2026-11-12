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
Behavioral tests for teardown.sh: it must delete the assets bucket only when
this account owns it, and leave a bucket owned by another account in place.
"""


def _removes_bucket(calls):
    return any(c.startswith("s3 rb") for c in calls)


def test_owned_bucket_is_removed(run_script):
    proc, calls = run_script("teardown.sh", STUB_HEAD="0")
    assert proc.returncode == 0, proc.stderr
    assert _removes_bucket(calls)


def test_bucket_owned_by_another_account_is_left_in_place(run_script):
    proc, calls = run_script("teardown.sh", STUB_HEAD="403")
    assert proc.returncode == 0, proc.stderr
    assert not _removes_bucket(calls), "must not delete a bucket we do not own"
