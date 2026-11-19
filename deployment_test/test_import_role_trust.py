# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""!!! WARNING: THIS TEST WRITES TO A LIVE GRAPH AND CREATES REAL IAM ROLES !!!

It runs FIVE S3 imports into the graph named by NETWORKX_GRAPH_ID. The graph is NOT reset:
imported data is ADDED each time. The maintainer chooses the graph; point it at a disposable
one. It also creates and deletes throwaway IAM roles (prefix ``nxn-trusttest-``).

Empirical check of which import-role trust-policy forms Neptune Analytics accepts.

Background: an S3 import can be rejected (with an error that blames S3 access) when the
import role's trust for ``neptune-graph.amazonaws.com`` carries an ``aws:SourceAccount``
condition. ``check_assume_role`` ignores ``Condition``, so the pre-flight check cannot
catch it. Which ``aws:Source*`` keys the service sets is unknown, so this test does NOT
assert which form works. It tries each form on a throwaway role, starts an import through
the library's ``_start_import_task`` path (StartImportTask), and records the outcome per
form. Read the summary printed at the end of the module (or the ``-rA`` / junit
properties) to see which condition key caused a failure.

Forms tried:
  account_only   - StringEquals aws:SourceAccount
  arn_only       - ArnLike aws:SourceArn arn:<partition>:neptune-graph:<region>:<acct>:graph/*
  arn_only_wild  - ArnLike aws:SourceArn arn:<partition>:neptune-graph:<region>:<acct>:*
                   THE FORM THE TEMPLATE NOW SHIPS. Differs from arn_only: the bare ``:*``
                   also covers import-task/, export-task/ and snapshot/ ARNs, so it passes
                   where arn_only fails if the service sends an import-task ARN.
  combined       - account_only + arn_only_wild, the template's PREVIOUS trust statement
                   (cloudformation-templates/nx-neptune-sagemaker.json before this change)
  no_condition   - DIAGNOSTIC CONTROL ONLY. Not a candidate; never a recommendation.

The pair that decides the shipped template is arn_only_wild (does it work?) and combined
(did the old form really fail?). account_only and arn_only isolate which part is at fault.

The S3 prefix must contain CSV import data. Run it deliberately.

Requirements:
  - NX_RUN_TRUST_MEASUREMENT=1     - explicit opt-in; without it the module is skipped,
                                     so `make integ-test` does not create IAM roles or
                                     import five times into the shared graph
  - NETWORKX_GRAPH_ID              - live Neptune Analytics graph
  - NETWORKX_S3_EXPORT_BUCKET_PATH - S3 prefix with CSV data to import. Read for the
                                     imports; the export case writes to a SIBLING prefix
                                     (`<prefix>-trust-export-<id>/`) so it never feeds its
                                     own output back into a later import, and empties it
                                     afterwards on a best-effort basis
  - IAM permissions to create/tag/delete roles and put/delete role policies, and
    iam:PassRole for the created roles
Only roles created by this module (prefix ``nxn-trusttest-``) are ever modified or deleted.
"""

import json
import os
import pathlib
import time
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

from nx_neptune import NETWORKX_GRAPH_ID
from nx_neptune.clients.iam_client import split_s3_arn_to_bucket_and_path
from nx_neptune.instance_management import (
    _get_bucket_encryption_key_arn,
    _start_export_task,
    _start_import_task,
)

S3_BUCKET = os.environ.get("NETWORKX_S3_EXPORT_BUCKET_PATH")

# Opt-in, deliberately separate from the usual integ variables. `make integ-test`
# runs all of integ_test/ with NETWORKX_GRAPH_ID and NETWORKX_S3_EXPORT_BUCKET_PATH
# already set, so gating on those alone meant a routine run created IAM roles in
# the caller's account and added five imports to the shared graph without a reset.
# This is a measurement, not a regression test: ask for it explicitly.
RUN_MEASUREMENT = os.environ.get("NX_RUN_TRUST_MEASUREMENT") == "1"

pytestmark = pytest.mark.skipif(
    not (RUN_MEASUREMENT and S3_BUCKET and NETWORKX_GRAPH_ID),
    reason=(
        "Set NX_RUN_TRUST_MEASUREMENT=1 (plus NETWORKX_S3_EXPORT_BUCKET_PATH and "
        "NETWORKX_GRAPH_ID) to run the trust measurement. It creates IAM roles and "
        "imports five times into the graph without resetting it."
    ),
)

ROLE_PREFIX = "nxn-trusttest-"
# IAM is eventually consistent. If this is too short, the service may reject a role that is
# merely not yet visible, which reads as "trust rejected" (a FALSE NEGATIVE). Re-run any
# surprising rejection, or raise the wait via NX_TRUST_TEST_IAM_WAIT_SECONDS.
IAM_PROPAGATION_SECONDS = int(os.environ.get("NX_TRUST_TEST_IAM_WAIT_SECONDS", "20"))
IMPORT_POLL_SECONDS = 10
IMPORT_MAX_WAIT_SECONDS = 600
TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED"}
FORMS = ["account_only", "arn_only", "arn_only_wild", "combined", "no_condition"]


def _trust_policy(form, partition, region, account):
    source_account = {"StringEquals": {"aws:SourceAccount": account}}
    source_arn = {
        "ArnLike": {
            "aws:SourceArn": f"arn:{partition}:neptune-graph:{region}:{account}:graph/*"
        }
    }
    # The template's SourceArn is a bare ":*", not ":graph/*".
    template_source_arn = {
        "ArnLike": {
            "aws:SourceArn": f"arn:{partition}:neptune-graph:{region}:{account}:*"
        }
    }
    conditions = {
        "account_only": source_account,
        "arn_only": source_arn,
        # The form the template actually ships. Distinct from arn_only: ":*" also covers
        # import-task/, export-task/ and snapshot/ ARNs, so if the service sends an
        # import-task ARN this passes where arn_only fails. Without this form a green run
        # would say nothing about the deployed template.
        "arn_only_wild": template_source_arn,
        "combined": {**source_account, **template_source_arn},
        "no_condition": None,  # diagnostic control only
    }[form]
    statement = {
        "Effect": "Allow",
        "Principal": {"Service": "neptune-graph.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }
    if conditions:
        statement["Condition"] = conditions
    return {"Version": "2012-10-17", "Statement": [statement]}


TEMPLATE_PATH = (
    pathlib.Path(__file__).resolve().parents[1]
    / "cloudformation-templates"
    / "nx-neptune-sagemaker.json"
)


def _template_import_role_actions():
    """The S3 and KMS actions ``ImportRole`` grants in the shipped template.

    Read from the template rather than written out here. A hand-kept subset is
    what let this measurement stay green while ``ImportRole`` was missing
    ``s3:DeleteObject``: the throwaway role never exercised the action, so no
    export was ever attempted through it.
    """
    template = json.loads(TEMPLATE_PATH.read_text())
    role = template["Resources"]["ImportRole"]
    s3_actions, kms_actions = set(), set()
    for policy in role["Properties"]["Policies"]:
        for statement in policy["PolicyDocument"]["Statement"]:
            if statement.get("Effect") != "Allow":
                continue
            action = statement["Action"]
            for entry in [action] if isinstance(action, str) else action:
                service = entry.split(":")[0]
                if service == "s3":
                    s3_actions.add(entry)
                elif service == "kms":
                    kms_actions.add(entry)
    assert s3_actions, f"No S3 actions found on ImportRole in {TEMPLATE_PATH}"
    return sorted(s3_actions), sorted(kms_actions)


def _import_access_policy(partition, s3_path, key_arn):
    """The template's ``ImportRole`` permissions, retargeted at the test's bucket.

    Object-level and bucket-level actions go on different ARNs, the same split
    the library's preflights simulate.
    """
    bucket, prefix = split_s3_arn_to_bucket_and_path(s3_path)
    prefix = prefix.rstrip("/")
    object_arn = (
        f"arn:{partition}:s3:::{bucket}/{prefix}*"
        if prefix
        else f"arn:{partition}:s3:::{bucket}/*"
    )
    s3_actions, kms_actions = _template_import_role_actions()
    bucket_level = [a for a in s3_actions if a == "s3:ListBucket"]
    object_level = [a for a in s3_actions if a != "s3:ListBucket"]

    statements = []
    if object_level:
        statements.append(
            {"Effect": "Allow", "Action": object_level, "Resource": object_arn}
        )
    if bucket_level:
        statements.append(
            {
                "Effect": "Allow",
                "Action": bucket_level,
                "Resource": f"arn:{partition}:s3:::{bucket}",
            }
        )
    if key_arn and kms_actions:
        statements.append(
            {"Effect": "Allow", "Action": kms_actions, "Resource": key_arn}
        )
    return {"Version": "2012-10-17", "Statement": statements}


@pytest.fixture(scope="module")
def env():
    sts = boto3.client("sts")
    account = sts.get_caller_identity()["Account"]
    caller_arn = sts.get_caller_identity()["Arn"]
    session = boto3.session.Session()
    region = session.region_name
    assert region, "No AWS region configured"
    return {
        "account": account,
        "partition": caller_arn.split(":")[1],
        "region": region,
        "run_id": uuid.uuid4().hex[:8],
    }


@pytest.fixture(scope="module")
def iam():
    return boto3.client("iam")


@pytest.fixture(scope="module")
def na_client():
    return boto3.client("neptune-graph")


@pytest.fixture(scope="module")
def created_roles(iam):
    """Names of roles this module created. Finalizer deletes them even if tests fail."""
    names = []
    yield names
    for name in names:
        _delete_role(iam, name)


@pytest.fixture(scope="module")
def exported_prefixes():
    """S3 prefixes this module exported to; emptied when the module finishes.

    Belt and braces alongside the sibling-prefix choice: the export lands
    outside the import source either way, but leaving CSVs behind in the
    maintainer's bucket run after run is its own small mess. Best effort - a
    caller without s3:DeleteObject gets a warning, not a failure.
    """
    prefixes = []
    yield prefixes
    s3 = boto3.client("s3")
    for s3_uri in prefixes:
        bucket, prefix = split_s3_arn_to_bucket_and_path(s3_uri)
        try:
            pages = s3.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix
            )
            keys = [
                {"Key": o["Key"]} for page in pages for o in page.get("Contents", [])
            ]
            for i in range(0, len(keys), 1000):
                s3.delete_objects(Bucket=bucket, Delete={"Objects": keys[i : i + 1000]})
        except ClientError as e:
            print(f"WARNING: could not clean up exported objects at {s3_uri}: {e}")


@pytest.fixture(scope="module")
def outcomes(request):
    """Per-form outcomes, printed as a summary when the module finishes."""
    results = {}
    yield results
    lines = ["", "=== import role trust form outcomes (NETWORKX_GRAPH_ID import) ==="]
    for form in FORMS:
        r = results.get(form)
        label = form
        if form == "no_condition":
            label += " [DIAGNOSTIC CONTROL ONLY]"
        elif form == "arn_only_wild":
            label += " [THE FORM THE TEMPLATE SHIPS — this is the verdict that matters]"
        elif form == "combined":
            label += " [the template's PREVIOUS form, expected to fail]"
        lines.append(f"{label}: {r['verdict'] if r else 'NOT RUN'}")
        if r and r.get("detail"):
            lines.append(f"    {r['detail']}")
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    for line in lines:
        if reporter:
            reporter.write_line(line)
        else:
            print(line)


def _delete_role(iam, name):
    # Hard guard: only ever touch roles this module created.
    if not name.startswith(ROLE_PREFIX):
        return
    try:
        for policy_name in iam.list_role_policies(RoleName=name)["PolicyNames"]:
            iam.delete_role_policy(RoleName=name, PolicyName=policy_name)
        iam.delete_role(RoleName=name)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "NoSuchEntity":
            print(f"WARNING: failed to clean up throwaway role {name}: {e}")


def _wait_for_terminal(na_client, task_id):
    """Poll until the import is terminal; cancel it if it outlives the timeout."""
    deadline = time.time() + IMPORT_MAX_WAIT_SECONDS
    resp = {}
    while time.time() < deadline:
        resp = na_client.get_import_task(taskIdentifier=task_id)
        if resp.get("status") in TERMINAL_STATUSES:
            return resp
        time.sleep(IMPORT_POLL_SECONDS)
    try:
        na_client.cancel_import_task(taskIdentifier=task_id)
    except ClientError:
        pass
    resp["status"] = (
        f"{resp.get('status')} (cancelled after {IMPORT_MAX_WAIT_SECONDS}s)"
    )
    return resp


@pytest.mark.parametrize("form", FORMS)
def test_import_with_trust_form(
    form, env, iam, na_client, created_roles, outcomes, record_property
):
    """Create a throwaway role with this trust form, start an import, record the outcome.

    The conditioned forms are record-only: which ``aws:Source*`` keys the service
    supplies is exactly what is unknown, so a rejection is data, not a failure.

    ``no_condition`` is different. It is the form the template ships, so it *is*
    asserted: if the shipped trust plus the shipped ``ImportRole`` permissions
    cannot complete an import, the stack is broken and this module should say so
    rather than printing a summary nobody reads.
    """
    role_name = f"{ROLE_PREFIX}{env['run_id']}-{form.replace('_', '-')}"
    created_roles.append(
        role_name
    )  # register first so a partial create is still cleaned

    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(
            _trust_policy(form, env["partition"], env["region"], env["account"])
        ),
        Description="Throwaway role for nx-neptune import trust test; safe to delete",
        Tags=[
            {"Key": "created-by", "Value": "nx-neptune-integ-test-import-role-trust"}
        ],
    )
    key_arn = _get_bucket_encryption_key_arn(S3_BUCKET)
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="import-access",
        PolicyDocument=json.dumps(
            _import_access_policy(env["partition"], S3_BUCKET, key_arn)
        ),
    )
    role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    time.sleep(
        IAM_PROPAGATION_SECONDS
    )  # see IAM_PROPAGATION_SECONDS: false-negative risk

    try:
        task_id = _start_import_task(na_client, NETWORKX_GRAPH_ID, S3_BUCKET, role_arn)
    except ClientError as e:
        err = e.response.get("Error", {})
        result = {
            "verdict": "REJECTED at StartImportTask",
            "detail": f"{err.get('Code')}: {err.get('Message')}",
        }
    else:
        final = _wait_for_terminal(na_client, task_id)
        status = final.get("status")
        result = {
            "verdict": f"ACCEPTED, import {status}",
            "detail": f"task {task_id}"
            + (
                f" reason: {final.get('statusReason')}"
                if final.get("statusReason")
                else ""
            ),
        }

    outcomes[form] = result
    if form == "no_condition":
        assert result["verdict"] == "ACCEPTED, import SUCCEEDED", (
            "The shipped trust form plus the shipped ImportRole permissions must "
            f"complete an import. Got: {result['verdict']} - {result['detail']}"
        )
    record_property("trust_form", form)
    record_property(
        "trust_policy",
        json.dumps(
            _trust_policy(form, env["partition"], env["region"], env["account"])
        ),
    )
    record_property("verdict", result["verdict"])
    record_property("detail", result["detail"])


def test_export_with_the_shipped_trust_form(
    env, iam, na_client, created_roles, exported_prefixes, record_property
):
    """The shipped role must also complete an *export*, not just an import.

    Export is the half that was broken: ``has_export_to_s3_permissions`` simulates
    ``s3:DeleteObject``, which ``ImportRole`` did not grant, and nothing here
    attempted an export so the run stayed green. Uses the template's own trust
    form and permission set, retargeted at this test's bucket.
    """
    role_name = f"{ROLE_PREFIX}{env['run_id']}-export"
    created_roles.append(role_name)

    iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(
            _trust_policy(
                "no_condition", env["partition"], env["region"], env["account"]
            )
        ),
        Description="Throwaway role for nx-neptune export trust test; safe to delete",
        Tags=[
            {"Key": "created-by", "Value": "nx-neptune-integ-test-import-role-trust"}
        ],
    )
    key_arn = _get_bucket_encryption_key_arn(S3_BUCKET)
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="import-access",
        PolicyDocument=json.dumps(
            _import_access_policy(env["partition"], S3_BUCKET, key_arn)
        ),
    )
    role_arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
    time.sleep(IAM_PROPAGATION_SECONDS)

    # Sibling prefix, deliberately not under S3_BUCKET: that prefix is the import
    # source, so exporting into it would feed the previous run's CSVs back into
    # the next run's import. Still inside the `{prefix}*` grant the throwaway
    # role gets, so no policy change is needed.
    destination = S3_BUCKET.rstrip("/") + f"-trust-export-{env['run_id']}/"
    exported_prefixes.append(destination)
    task_id = _start_export_task(
        na_client, NETWORKX_GRAPH_ID, destination, role_arn, key_arn
    )
    final = _wait_for_export_terminal(na_client, task_id)
    record_property("export_task", task_id)
    record_property("export_status", str(final.get("status")))

    assert final.get("status") == "SUCCEEDED", (
        "The shipped ImportRole permissions must complete an export. Got "
        f"{final.get('status')}: {final.get('statusReason')}. A missing "
        "s3:DeleteObject shows up here."
    )


def _wait_for_export_terminal(na_client, task_id):
    """Poll an export until terminal; cancel it if it outlives the timeout."""
    deadline = time.time() + IMPORT_MAX_WAIT_SECONDS
    resp = {}
    while time.time() < deadline:
        resp = na_client.get_export_task(taskIdentifier=task_id)
        if resp.get("status") in TERMINAL_STATUSES:
            return resp
        time.sleep(IMPORT_POLL_SECONDS)
    try:
        na_client.cancel_export_task(taskIdentifier=task_id)
    except ClientError:
        pass
    resp["status"] = (
        f"{resp.get('status')} (cancelled after {IMPORT_MAX_WAIT_SECONDS}s)"
    )
    return resp
