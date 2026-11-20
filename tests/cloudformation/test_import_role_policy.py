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
"""``ImportRole`` must grant what the library's preflights actually simulate.

The template is the only place these permissions exist, and nothing read it:
the ``workspace`` fixture in ``conftest.py`` writes ``{}`` in its place, because
those tests only care about what ``deploy.sh`` passes to the CLI. So a missing
action was invisible until an export failed on a live stack - which is how
``s3:DeleteObject`` came to be left out while ``has_export_to_s3_permissions``
simulated it.

These are pure JSON assertions over the real template. No AWS, no stack.
"""

import json
from pathlib import Path

import pytest

TEMPLATE = (
    Path(__file__).resolve().parents[2]
    / "cloudformation-templates"
    / "nx-neptune-sagemaker.json"
)

# The S3 actions each preflight in nx_neptune.clients.iam_client simulates.
# Kept as literals rather than imported, so that relaxing a preflight cannot
# silently relax what the template is required to grant.
PREFLIGHT_S3_ACTIONS = {
    "has_import_from_s3_permissions": {"s3:GetObject"},
    "has_export_to_s3_permissions": {
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
    },
    "has_delete_s3_permissions": {"s3:DeleteObject", "s3:ListBucket"},
    "has_athena_permissions": {"s3:GetObject", "s3:PutObject", "s3:ListBucket"},
}

PREFLIGHT_KMS_ACTIONS = {"kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"}


@pytest.fixture(scope="module")
def template():
    return json.loads(TEMPLATE.read_text())


@pytest.fixture(scope="module")
def import_role(template):
    return template["Resources"]["ImportRole"]


def _granted_actions(role):
    """Every Allow action across the role's inline policies, flattened."""
    actions = set()
    for policy in role["Properties"].get("Policies", []):
        for statement in policy["PolicyDocument"]["Statement"]:
            if statement.get("Effect") != "Allow":
                continue
            action = statement["Action"]
            actions.update([action] if isinstance(action, str) else action)
    return actions


class TestImportRoleCoversThePreflights:
    """Every action a preflight simulates has to be granted, or the call aborts."""

    @pytest.mark.parametrize(
        "preflight,actions",
        sorted((k, frozenset(v)) for k, v in PREFLIGHT_S3_ACTIONS.items()),
    )
    def test_s3_actions_are_granted(self, import_role, preflight, actions):
        missing = actions - _granted_actions(import_role)
        assert not missing, (
            f"ImportRole is missing {sorted(missing)}, which {preflight} simulates; "
            f"the call aborts with 'Insufficient permission'"
        )

    def test_kms_actions_are_granted(self, import_role):
        missing = PREFLIGHT_KMS_ACTIONS - _granted_actions(import_role)
        assert not missing, f"ImportRole is missing {sorted(missing)}"

    def test_delete_object_specifically(self, import_role):
        """Pin of the regression: its absence broke both demo exports."""
        assert "s3:DeleteObject" in _granted_actions(import_role)

    def test_s3_actions_cover_the_bucket_and_its_objects(self, import_role):
        """Bucket-level and object-level actions resolve to different ARNs."""
        resources = set()
        for policy in import_role["Properties"]["Policies"]:
            for statement in policy["PolicyDocument"]["Statement"]:
                if statement.get("Sid") != "StagingBucketObjectAccess":
                    continue
                for entry in statement["Resource"]:
                    resources.add(entry["Fn::Sub"])
        assert resources == {
            "arn:aws:s3:::${StagingBucket}",
            "arn:aws:s3:::${StagingBucket}/*",
        }

    def test_the_role_reaches_nothing_beyond_s3_and_kms(self, import_role):
        """The containment claim in the README: no Neptune, Glue, logs or SageMaker."""
        services = {action.split(":")[0] for action in _granted_actions(import_role)}
        assert services == {"s3", "kms"}


class TestImportRoleTrust:
    """The unconditioned service trust is the whole point of the role."""

    @staticmethod
    def _statements(role):
        return role["Properties"]["AssumeRolePolicyDocument"]["Statement"]

    def test_neptune_graph_can_assume_it(self, import_role):
        principals = {
            s["Principal"]["Service"]
            for s in self._statements(import_role)
            if s.get("Effect") == "Allow"
        }
        assert principals == {"neptune-graph.amazonaws.com"}

    def test_the_trust_carries_no_source_condition(self, import_role):
        """Every conditioned form was rejected on a live graph; see the README.

        This is an assertion about a deliberate decision, not a preference. If
        it starts failing, the measurement in cloudformation-templates/README.md
        has to be re-run before the condition is kept.
        """
        for statement in self._statements(import_role):
            assert "Condition" not in statement


class TestImportRoleIsWiredUp:
    """A correct role that nothing points at would not be used."""

    def test_the_notebook_env_var_points_at_it(self, template):
        """NETWORKX_S3_IAM_ROLE_ARN is what makes the library use this role."""
        body = json.dumps(template["Resources"]["NotebookLifecycleConfig"])
        assert "NETWORKX_S3_IAM_ROLE_ARN" in body
        assert '["ImportRole", "Arn"]'.replace(" ", "") in body.replace(" ", "")

    def test_it_is_exposed_as_a_stack_output(self, template):
        output = template["Outputs"]["ImportRoleArn"]
        assert output["Value"] == {"Fn::GetAtt": ["ImportRole", "Arn"]}

    def test_the_notebook_role_may_pass_it_to_neptune(self, template):
        """Without iam:PassRole on this role, Neptune refuses the import task."""
        # NotebookRole has no inline policies; its permissions come from the
        # NeptuneAccessPolicy managed policy attached to it.
        statements = [
            s
            for s in template["Resources"]["NeptuneAccessPolicy"]["Properties"][
                "PolicyDocument"
            ]["Statement"]
            if s.get("Sid") == "PassRoleToNeptune"
        ]
        assert statements, "PassRoleToNeptune statement is gone"
        passable = json.dumps(statements)
        assert "ImportRole" in passable
        assert "iam:PassRole" in passable

    def test_the_notebook_role_keeps_its_conditioned_trust(self, template):
        """NotebookRole holds much more, so its condition must stay.

        It is deliberately not switched to the unconditioned form: it also has
        Neptune administration, Glue writes and broader S3 access.
        """
        statements = template["Resources"]["NotebookRole"]["Properties"][
            "AssumeRolePolicyDocument"
        ]["Statement"]
        neptune = [
            s
            for s in statements
            if "neptune-graph.amazonaws.com"
            in json.dumps(s.get("Principal", {}).get("Service", ""))
        ]
        for statement in neptune:
            assert "Condition" in statement, (
                "NotebookRole's neptune-graph trust lost its condition; it holds "
                "far more than ImportRole and must not become assumable without one"
            )
