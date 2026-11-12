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
Release gate for the assets-bucket ownership fix.

A third party who knew the target's account ID could pre-create the predictable
assets bucket in their own account; the tool would then upload to it and the
notebook would install code from it. The fix adds four controls. Proving a real
cross-account denial needs two accounts and live AWS, so it stays a manual/integ
check. These tests instead assert — with no AWS, so they run in CI and releases —
that each guard is present and correctly shaped, so a later edit cannot silently
remove it.
"""
import json
from pathlib import Path

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "cloudformation-templates"
TEMPLATE = json.loads((TEMPLATES_DIR / "nx-neptune-sagemaker.json").read_text())
TEMPLATE_TEXT = (TEMPLATES_DIR / "nx-neptune-sagemaker.json").read_text()


def _find_statement(obj, sid):
    """Recursively locate an IAM policy statement by Sid, structure-agnostic."""
    if isinstance(obj, dict):
        if obj.get("Sid") == sid and "Effect" in obj:
            return obj
        for value in obj.values():
            found = _find_statement(value, sid)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_statement(item, sid)
            if found is not None:
                return found
    return None


# --- Control 1: notebook role can only read assets from the stack's account ---

def test_asset_download_requires_same_account():
    stmt = _find_statement(TEMPLATE, "S3AssetDownload")
    assert stmt is not None, "S3AssetDownload statement missing"
    assert stmt.get("Condition") == {
        "StringEquals": {"s3:ResourceAccount": {"Ref": "AWS::AccountId"}}
    }, "S3AssetDownload must be gated on s3:ResourceAccount == AWS::AccountId"
    # The account condition must not cover the AWS-owned graph-notebook bucket,
    # which lives in a different account.
    assert "aws-neptune-notebook" not in json.dumps(stmt["Resource"])


def test_aws_graph_notebook_bucket_is_separate_and_unconditioned():
    stmt = _find_statement(TEMPLATE, "S3GraphNotebookDownload")
    assert stmt is not None, "S3GraphNotebookDownload statement missing"
    assert "Condition" not in stmt, (
        "AWS-owned bucket statement must stay unconditioned, else the account "
        "condition would deny the legitimate cross-account read"
    )
    assert "aws-neptune-notebook" in json.dumps(stmt["Resource"])


# deploy.sh / teardown.sh ownership and region behavior (the --expected-bucket-owner
# assertions, fail-closed create-bucket, and the ownership check before delete) are
# exercised by running the scripts in test_deploy_behavior.py and
# test_teardown_behavior.py.


# --- Control 2: notebook setup fails loudly if the asset read is denied ---

def test_notebook_setup_fails_when_assets_unreadable():
    # Without this, a denied asset copy fell through to 'pip install nx_neptune'
    # from PyPI, installing code the operator did not stage.
    assert "failed to read assets from ${ASSETS_PREFIX}" in TEMPLATE_TEXT
    copy_line = next(
        line for line in TEMPLATE_TEXT.splitlines() if "aws s3 cp" in line and "ASSETS_PREFIX" in line
    )
    assert "exit 1" in copy_line, "OnCreate asset copy must exit non-zero on failure"
