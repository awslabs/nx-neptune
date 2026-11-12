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
deploy.sh and teardown.sh run as separate processes, so each re-derives the
assets bucket name from the same inputs. If that formula drifts between the two,
teardown would operate on a different bucket than deploy created. These tests pin
both scripts to one expected value and guard the account/stack scoping that keeps
the name from being derivable from the region alone.
"""
import re
from pathlib import Path

import pytest

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "cloudformation-templates"
EXPECTED = "nx-neptune-assets-${STACK_NAME}-${ACCOUNT_ID}-${REGION}"


def _assets_bucket_expr(script_name: str) -> str:
    """Return the right-hand side of the `ASSETS_BUCKET="..."` assignment."""
    content = (TEMPLATES_DIR / script_name).read_text()
    match = re.search(r'^ASSETS_BUCKET="([^"]*)"', content, re.MULTILINE)
    assert match, f"ASSETS_BUCKET assignment not found in {script_name}"
    return match.group(1)


def test_deploy_and_teardown_derive_same_bucket():
    assert _assets_bucket_expr("deploy.sh") == _assets_bucket_expr("teardown.sh")


@pytest.mark.parametrize("script", ["deploy.sh", "teardown.sh"])
def test_assets_bucket_matches_expected(script):
    assert _assets_bucket_expr(script) == EXPECTED


@pytest.mark.parametrize("script", ["deploy.sh", "teardown.sh"])
def test_assets_bucket_scoped_to_account_and_stack(script):
    # The name must include the account and stack so it is not derivable from the
    # region alone. Dropping either would regress the fix.
    expr = _assets_bucket_expr(script)
    assert "${ACCOUNT_ID}" in expr
    assert "${STACK_NAME}" in expr
