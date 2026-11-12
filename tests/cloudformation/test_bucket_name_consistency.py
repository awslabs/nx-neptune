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
deploy.sh and teardown.sh each derive the assets bucket name independently. If
those derivations drift apart, teardown would operate on a different bucket than
deploy created and orphan it. This runs both scripts and compares the bucket name
each actually acts on, rather than comparing their source text.
"""


def _flag_value(calls, prefix, flag):
    for call in calls:
        if call.startswith(prefix):
            parts = call.split()
            if flag in parts:
                return parts[parts.index(flag) + 1]
    return None


def _rb_target(calls):
    for call in calls:
        if call.startswith("s3 rb"):
            return call.split()[2].replace("s3://", "")
    return None


def test_deploy_and_teardown_act_on_the_same_bucket(run_script):
    _, deploy_calls = run_script("deploy.sh", STUB_HEAD="404", STUB_CREATE="ok")
    _, teardown_calls = run_script("teardown.sh", STUB_HEAD="0")

    created = _flag_value(deploy_calls, "s3api create-bucket", "--bucket")
    removed = _rb_target(teardown_calls)

    assert created, "deploy.sh did not create a bucket"
    assert removed, "teardown.sh did not remove a bucket"
    assert created == removed
