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
import logging
import re
from typing import Mapping, Optional, Tuple, Union

import boto3
import jmespath
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.utils import ArnParser

__all__ = ["IamClientWrapper", "split_s3_arn_to_bucket_and_path"]

from .client_factory import ClientFactory
from .neptune_constants import AGENT_TAG_KEY, AGENT_TAG_VALUE, SERVICE_NA

# S3 actions that apply to the bucket itself (arn:<partition>:s3:::bucket) rather
# than to objects (arn:<partition>:s3:::bucket/key). Any S3 action not listed here
# is treated as object-level. Extend this when a new bucket-level action is checked.
S3_BUCKET_LEVEL_ACTIONS = frozenset(
    {
        "s3:ListBucket",
        "s3:ListBucketVersions",
        "s3:ListBucketMultipartUploads",
        "s3:GetBucketLocation",
        "s3:GetBucketVersioning",
    }
)

# Resource types AWS lists for each Neptune Analytics action. IAM authorizes the
# real call against any of them, so a grant on either one is valid and an action is
# simulated against every type listed. Derived from AWS's machine-readable Service
# Authorization Reference:
#   https://servicereference.us-east-1.amazonaws.com/v1/neptune-graph/neptune-graph.json
# Re-derive it from that file when AWS adds actions; it is not fetched at runtime.
# An action missing here is treated as ``graph``-only.
NA_ACTION_RESOURCE_TYPES: Mapping[str, Tuple[str, ...]] = {
    "neptune-graph:CancelExportTask": ("export-task",),
    "neptune-graph:CancelImportTask": ("import-task",),
    "neptune-graph:CancelQuery": ("graph",),
    "neptune-graph:CreateGraph": ("graph",),
    "neptune-graph:CreateGraphSnapshot": (
        "graph",
        "graph-snapshot",
    ),
    "neptune-graph:CreateGraphUsingImportTask": (
        "graph",
        "import-task",
    ),
    "neptune-graph:CreatePrivateGraphEndpoint": ("graph",),
    "neptune-graph:DeleteDataViaQuery": ("graph",),
    "neptune-graph:DeleteGraph": ("graph",),
    "neptune-graph:DeleteGraphSnapshot": ("graph-snapshot",),
    "neptune-graph:DeletePrivateGraphEndpoint": ("graph",),
    "neptune-graph:GetEngineStatus": ("graph",),
    "neptune-graph:GetExportTask": ("export-task",),
    "neptune-graph:GetGraph": ("graph",),
    "neptune-graph:GetGraphSnapshot": ("graph-snapshot",),
    "neptune-graph:GetGraphSummary": ("graph",),
    "neptune-graph:GetImportTask": ("import-task",),
    "neptune-graph:GetPrivateGraphEndpoint": ("graph",),
    "neptune-graph:GetQueryStatus": ("graph",),
    "neptune-graph:GetStatisticsStatus": ("graph",),
    "neptune-graph:ListExportTasks": ("export-task",),
    "neptune-graph:ListGraphSnapshots": ("graph-snapshot",),
    "neptune-graph:ListGraphs": ("graph",),
    "neptune-graph:ListImportTasks": ("import-task",),
    "neptune-graph:ListPrivateGraphEndpoints": ("graph",),
    "neptune-graph:ListQueries": ("graph",),
    "neptune-graph:ListTagsForResource": (
        "graph",
        "graph-snapshot",
    ),
    "neptune-graph:ReadDataViaQuery": ("graph",),
    "neptune-graph:ResetGraph": ("graph",),
    "neptune-graph:RestoreGraphFromSnapshot": (
        "graph",
        "graph-snapshot",
    ),
    "neptune-graph:StartExportTask": (
        "export-task",
        "graph",
    ),
    "neptune-graph:StartGraph": ("graph",),
    "neptune-graph:StartImportTask": (
        "graph",
        "import-task",
    ),
    "neptune-graph:StopGraph": ("graph",),
    "neptune-graph:TagResource": (
        "graph",
        "graph-snapshot",
    ),
    "neptune-graph:UntagResource": (
        "graph",
        "graph-snapshot",
    ),
    "neptune-graph:UpdateGraph": ("graph",),
    "neptune-graph:WriteDataViaQuery": ("graph",),
}
_DEFAULT_NA_RESOURCE_TYPES = ("graph",)


class IamClientWrapper:
    """
    IAM Client is used to interact with AWS IAM service for role-based operations
    related to Neptune Analytics permissions and access control.

    This client provides methods to verify IAM role permissions for S3 operations,
    check trust relationships for service principals, and validate ARNs.

    The IAM role ARN can be provided as an argument. Otherwise, the NETWORKX_ARN_IAM_ROLE environment variable is used.
    """

    def __init__(
        self,
        role_arn: str,
        client: BaseClient,
        logger: Optional[logging.Logger] = None,
        *,
        region: Optional[str] = None,
    ):
        """
        Constructs an IAM Client object for AWS IAM service interaction, with logger and boto client.

        Args:
            role_arn (str): The ARN of the IAM role to use.
            client (BaseClient): Custom boto3 IAM client.
            logger (logging.Logger): Custom logger. Creates a default logger if None is provided.
            region (Optional[str]): Region used for the Neptune Analytics resource ARN and the
                ``aws:RequestedRegion`` context entry. Defaults to the boto3 session's
                configured region. The IAM client's own region is never used: IAM is a
                global service, so it reports a pseudo-region such as ``aws-global``.
        """
        if role_arn.split(":")[2:3] == ["sts"]:
            self.role_arn = _convert_sts_to_iam_arn(role_arn)
        else:
            self.role_arn = role_arn
        self.client = client
        self.logger = logger or logging.getLogger(__name__)
        self._region = region

    @property
    def _partition(self) -> str:
        """AWS partition (aws, aws-cn, aws-us-gov) parsed from the role ARN."""
        parts = self.role_arn.split(":")
        return parts[1] if len(parts) > 1 and parts[1] else "aws"

    @property
    def _account_id(self) -> str:
        """AWS account id parsed from the role ARN."""
        parts = self.role_arn.split(":")
        return parts[4] if len(parts) > 4 else ""

    @property
    def _resolved_region(self) -> Optional[str]:
        """Explicit region, else the region ``ClientFactory`` clients use; ``None`` if unset.

        Order: the ``region=`` keyword, ``boto3.DEFAULT_SESSION`` (what the module-level
        ``boto3.client(...)`` calls in ``ClientFactory`` resolve through), then a fresh
        ``boto3.Session()``. A fresh Session ignores ``setup_default_session``, so the
        default session must be consulted first to agree with the Neptune clients.

        Never the IAM client's ``meta.region_name``: IAM is global and reports a
        pseudo-region such as ``aws-global`` that no policy can match.
        """
        if self._region:
            return self._region
        default_session = boto3.DEFAULT_SESSION
        if default_session is not None and default_session.region_name:
            return default_session.region_name
        return boto3.Session().region_name

    def _neptune_graph_arn(self, resource_type: str = "graph") -> str:
        """Resource ARN matching any Neptune Analytics ``resource_type`` (``graph`` or
        ``graph-snapshot``) in the caller's account and region."""
        # With no region known, a wildcard keeps the ARN well formed rather than
        # embedding "None". This is a fallback, not a match-anything: IAM matches a
        # policy's Resource against this literal request ARN, so the "*" here is not
        # a wildcard to the policy. A grant pinned to one region will NOT be
        # confirmed (both actions come back implicitDeny), so on this path the
        # preflight warns and continues instead of verifying.
        # Live IAM accepts the wildcard region without error. It also showed that
        # omitting aws:RequestedRegion makes a negated region-lockdown Deny
        # (StringNotEquals) fire as explicitDeny; _check_neptune_permission treats
        # that as non-authoritative on this path rather than aborting.
        region = self._resolved_region or "*"
        return (
            f"arn:{self._partition}:neptune-graph:{region}:"
            f"{self._account_id}:{resource_type}/*"
        )

    def _neptune_context_entries(self) -> list:
        """Context entries a tag- or region-conditioned Neptune Analytics policy keys on."""
        entries = [
            {
                "ContextKeyName": f"aws:RequestTag/{AGENT_TAG_KEY}",
                "ContextKeyValues": [AGENT_TAG_VALUE],
                "ContextKeyType": "string",
            },
            {
                "ContextKeyName": f"aws:ResourceTag/{AGENT_TAG_KEY}",
                "ContextKeyValues": [AGENT_TAG_VALUE],
                "ContextKeyType": "string",
            },
        ]
        region = self._resolved_region
        if region:
            entries.append(
                {
                    "ContextKeyName": "aws:RequestedRegion",
                    "ContextKeyValues": [region],
                    "ContextKeyType": "string",
                }
            )
        else:
            self.logger.warning(
                "No AWS region is configured (set AWS_DEFAULT_REGION, an AWS "
                "profile region, or pass region=). Note: without a region, a "
                "policy scoped to a specific region will not be confirmed, even "
                "if it grants access."
            )
        return entries

    def _check_neptune_permission(self, operation_name: str, permissions: list) -> dict:
        """Simulate Neptune Analytics actions with tag/region context.

        Each action is simulated against every resource type AWS lists for it
        (``NA_ACTION_RESOURCE_TYPES``) and is confirmed if any of them is allowed.
        An implicit deny on one type is therefore not reported unless no type
        allows the action.
        """
        # The ARNs are built here from the role ARN and region, not supplied by a
        # caller, so they skip the user-input validation in check_aws_permission.
        context = self._neptune_context_entries()
        # Without a region the aws:RequestedRegion entry is omitted, which makes a
        # negated region-lockdown Deny fire; such a deny is then not authoritative.
        # Limitation: an unconditional Deny is downgraded the same way on this path,
        # because the response does not expose the matched statement's conditions.
        authoritative = self._resolved_region is not None

        by_type: dict = {}
        for action in permissions:
            for resource_type in NA_ACTION_RESOURCE_TYPES.get(
                action, _DEFAULT_NA_RESOURCE_TYPES
            ):
                by_type.setdefault(resource_type, []).append(action)

        results: dict = {}
        for resource_type, actions in by_type.items():
            simulated = self._simulate(
                operation_name,
                actions,
                self._neptune_graph_arn(resource_type),
                context,
                deny_is_authoritative=authoritative,
                warn_on_implicit_deny=False,
            )
            for action, allowed in simulated.items():
                results[action] = results.get(action, False) or allowed

        for action, allowed in results.items():
            if not allowed:
                self._warn_implicit_deny(action, operation_name)
        return results

    def _check_s3_permission(
        self, operation_name: str, permissions: list, s3_path: str
    ) -> dict:
        """Simulate S3 actions, bucket-level against the bucket ARN and object-level
        against the object ARN, in separate requests."""
        bucket_arn, object_arn = _get_s3_bucket_and_object_arns(
            s3_path, self._partition
        )
        bucket_actions = [p for p in permissions if p in S3_BUCKET_LEVEL_ACTIONS]
        object_actions = [p for p in permissions if p not in S3_BUCKET_LEVEL_ACTIONS]
        # A prefix-conditioned bucket policy keys on s3:prefix, which applies to the
        # bucket-level request only. Omitted when the path has no prefix.
        _, prefix = _split_s3_bucket_and_prefix(s3_path)
        bucket_context = (
            [
                {
                    "ContextKeyName": "s3:prefix",
                    "ContextKeyValues": [f"{prefix}/"],  # verified against live IAM
                    "ContextKeyType": "string",
                }
            ]
            if prefix
            else None
        )
        results: dict = {}
        for actions, arn, context in (
            (bucket_actions, bucket_arn, bucket_context),
            (object_actions, object_arn, None),
        ):
            if actions:
                results.update(
                    self.check_aws_permission(operation_name, actions, arn, context)
                )
        return results

    @classmethod
    def default(
        cls,
        sts_client: Optional[BaseClient] = None,
        iam_client: Optional[BaseClient] = None,
        logger: Optional[logging.Logger] = None,
        *,
        region: Optional[str] = None,
    ) -> "IamClientWrapper":
        """
        Build an IamClientWrapper for the current caller. The role ARN is
        resolved from the STS caller identity (an STS network call). Clients
        not provided are created from the default ClientFactory.

        Args:
            sts_client (Optional[BaseClient]): STS boto3 client.
            iam_client (Optional[BaseClient]): IAM boto3 client.
            logger (Optional[logging.Logger]): Custom logger.
            region (Optional[str]): Region for the simulated Neptune Analytics ARN and
                ``aws:RequestedRegion``; also used for clients created here. Defaults to
                the boto3 session's configured region.

        Returns:
            IamClientWrapper: Wrapper bound to the caller's role ARN.
        """
        factory = ClientFactory(region=region)
        if sts_client is None:
            sts_client = factory.sts()
        if iam_client is None:
            iam_client = factory.iam()
        return cls(
            role_arn=sts_client.get_caller_identity()["Arn"],
            client=iam_client,
            logger=logger,
            region=region,
        )

    def check_assume_role(self, service_name: str) -> bool:
        """
        Check if a specific AWS service has permission to assume the configured IAM role
        by directly examining the trust policy.

        Args:
            service_name (str): The AWS service to check (e.g., 'neptune-graph', 'lambda', 'ec2')

        Returns:
            bool: True if the service can assume the role, False otherwise

        Raises:
            ValueError: If input parameters are invalid or role policy has unexpected structure
            ClientError: If there's an issue with the AWS API call
        """
        try:
            self.logger.debug(
                f"Perform role assume check with role: [{self.role_arn}], and service: [{service_name}]"
            )
            self._validate_arns([self.role_arn])
            # Extract the role name from the ARN (format: arn:aws:iam::account-id:role/role-name)
            iam_role_arn = self.role_arn.split("/")[-1]

            # Get role including assume role policy
            response = self.client.get_role(RoleName=iam_role_arn)  # type: ignore[attr-defined]

            # Use jmespath to extract statements that allow AssumeRole for the service
            statements = jmespath.search(
                "Role.AssumeRolePolicyDocument.Statement[?Effect==`Allow`]", response
            )

            if statements is None:
                raise ValueError(f"Unexpected response structure: {response}")

            # Check if the service is allowed to assume this role
            service_principal = f"{service_name}.amazonaws.com"
            sts_allowed_list = ["sts:AssumeRole", "sts:*"]
            for statement in statements:
                action = statement.get("Action")
                # Action can be a string or a list
                actions = [action] if isinstance(action, str) else action

                if any(a in sts_allowed_list for a in actions):
                    # Only check allow at the end.
                    service = jmespath.search("Principal.Service", statement)
                    services = [service] if isinstance(service, str) else service
                    if services and service_principal in services:
                        return True
            return False

        except ClientError as e:
            raise e

    def check_aws_permission(
        self,
        operation_name: str,
        permissions: list,
        resource_arn: str = "*",
        context_entries: Optional[list] = None,
    ) -> dict:
        """
        Validates if the configured IAM role has the required permissions for a specified resource ARN.

        Args:
            permissions (list): List of permission strings to check (e.g., ['s3:GetObject'])
            resource_arn (str): The resource ARN to check permissions against
            context_entries (Optional[list]): IAM ContextEntries forwarded to the simulator
                (e.g. tag and region keys). Omitted from the request when None.

        Returns:
            dict: A dictionary mapping each permission to a boolean indicating if it's allowed.
                An ``implicitDeny`` is logged as a warning and mapped to False.

        Raises:
            ValueError: If input parameters are invalid, or a permission is explicitly denied
            ClientError: If there's an issue with the AWS API call
        """
        # Validate ARN formats
        if resource_arn != "*":
            self._validate_arns([self.role_arn, resource_arn])
        return self._simulate(
            operation_name, permissions, resource_arn, context_entries
        )

    def _warn_implicit_deny(self, action_name: str, operation_name: str) -> None:
        self.logger.warning(
            f"Permission {action_name} was not explicitly allowed (implicitDeny) "
            f"for operation {operation_name}; it may fail at call time."
        )

    def _simulate(
        self,
        operation_name: str,
        permissions: list,
        resource_arn: str,
        context_entries: Optional[list] = None,
        deny_is_authoritative: bool = True,
        warn_on_implicit_deny: bool = True,
    ) -> dict:
        """Run the policy simulation and interpret its decisions (no ARN validation).

        ``deny_is_authoritative=False`` marks a request known to be incomplete (a
        context key was omitted), where an ``explicitDeny`` may be an artifact of
        that omission: it is warned about and recorded as False instead of raising.
        ``warn_on_implicit_deny=False`` leaves the implicit-deny warning to a caller
        that combines several simulations of the same action.
        """
        allowed_decisions = ["allowed"]

        try:
            self.logger.debug(
                f"Perform role permission check with: \n"
                f" Role [{self.role_arn}], \n"
                f" Permission: [{permissions}]\n"
                f" Resources: [{resource_arn}]\n"
            )
            # Execute the permission check
            request: dict = {
                "PolicySourceArn": self.role_arn,
                "ActionNames": permissions,
                "ResourceArns": [resource_arn],
            }
            if context_entries:
                request["ContextEntries"] = context_entries
            response = self.client.simulate_principal_policy(**request)  # type: ignore[attr-defined]

            # Extract evaluation results using jmespath
            evaluation_results = jmespath.search("EvaluationResults", response)

            # Check if evaluation_results is None or empty
            if not evaluation_results:
                raise ValueError(
                    f"Unexpected result structure: No evaluation results found in response: {response}"
                )

            results = {}
            # Map the results to boolean values
            for result in evaluation_results:
                action_name = result.get("EvalActionName")
                decision = result.get("EvalDecision")

                if not action_name or not decision:
                    raise ValueError(f"Unexpected result structure: {result}")

                if decision == "implicitDeny":
                    # Inconclusive: the simulator cannot model every condition IAM
                    # evaluates at call time, so warn rather than abort.
                    if warn_on_implicit_deny:
                        self._warn_implicit_deny(action_name, operation_name)
                elif decision == "explicitDeny" and not deny_is_authoritative:
                    self.logger.warning(
                        f"Permission {action_name} was explicitly denied for operation "
                        f"{operation_name}, but no region was configured so "
                        f"aws:RequestedRegion was not simulated; a region-based deny "
                        f"may be an artifact of that. Set a region to confirm."
                    )
                elif decision not in allowed_decisions:
                    raise ValueError(
                        f"Insufficient permission, {action_name} need to be grant for operation {operation_name}"
                    )
                results[action_name] = decision in allowed_decisions
            self.logger.debug(
                f"Permission check on resource [{resource_arn}], with result: {results}"
            )
            return results

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "AccessDenied":
                self.logger.warning(
                    "Missing permission [iam:SimulatePrincipalPolicy] for current IAM user role, skipping permission check."
                )
                return {}
            else:
                raise e

    def check_s3_versioning_enabled(
        self, bucket_arn: str, s3_client: Optional[BaseClient] = None
    ):
        """Check whether S3 bucket versioning is enabled.

        Raises ValueError when versioning is not enabled on buckets used for
        write or delete operations.

        Args:
            bucket_arn (str): The S3 bucket path or ARN
            s3_client (BaseClient, optional): Boto3 S3 client. Created if not provided.

        Raises:
            ValueError: If versioning is not enabled or status cannot be determined
        """
        s3_client = s3_client or ClientFactory().s3()
        bucket_name, _ = split_s3_arn_to_bucket_and_path(bucket_arn)
        try:
            response = s3_client.get_bucket_versioning(Bucket=bucket_name)
            if response.get("Status") != "Enabled":
                raise ValueError(
                    f"S3 bucket '{bucket_name}' does not have versioning enabled. "
                    f"Enable versioning before performing write or delete operations."
                )
        except ClientError as e:
            raise ValueError(
                f"Unable to check versioning for bucket '{bucket_name}': {e}"
            ) from e

    def _s3_kms_permission_check(
        self, operation_name, bucket_arn, key_arn, s3_permissions, kms_permissions
    ):
        """Internal helper to check S3 and KMS permissions for the configured IAM role.

        Args:
            operation_name (str): Name of the operation being performed (for error messages)
            bucket_arn (str): The ARN of the S3 bucket
            key_arn (str): The ARN of the KMS key, or None if not using KMS encryption
            s3_permissions (list): List of S3 permissions to check
            kms_permissions (list): List of KMS permissions to check

        Raises:
            ValueError: If the role lacks required permissions or cannot be assumed by Neptune Analytics

        Note:
            If key_arn is provided, both S3 and KMS permissions are checked and the results are merged.
        """
        self.logger.debug(
            f"Permission check on ARN(s): {self.role_arn}, {bucket_arn}, {key_arn}"
        )

        if not self.check_assume_role(SERVICE_NA):
            raise ValueError(f"Missing role assume on principle {SERVICE_NA}")
        # Check S3
        self._check_s3_permission(operation_name, s3_permissions, bucket_arn)

        # Check KMS
        if key_arn is not None:
            self.check_aws_permission(operation_name, kms_permissions, key_arn)

    def has_export_to_s3_permissions(self, bucket_arn, key_arn=None):
        """Check if the configured IAM role has permissions to export data to S3.

        Verifies that the role has the necessary S3 and KMS permissions required
        for exporting graph data from Neptune Analytics to S3.

        Args:
            bucket_arn (str): The ARN of the S3 bucket
            key_arn (str, optional): The ARN of the KMS key, or None if not using KMS encryption.
                                     Defaults to None.

        Raises:
            ValueError: If the role lacks required permissions

        Returns:
            None
        """
        s3_permissions = ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        kms_permissions = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        operation_name = "S3-Export"
        self._s3_kms_permission_check(
            operation_name, bucket_arn, key_arn, s3_permissions, kms_permissions
        )
        self.check_s3_versioning_enabled(bucket_arn)

    def has_delete_s3_permissions(self, bucket_arn):
        """Check if the configured IAM role has permissions to delete data from S3.

        Args:
            bucket_arn (str): The ARN of the S3 bucket

        Raises:
            ValueError: If the role lacks required permissions

        Returns:
            None
        """
        s3_permissions = ["s3:DeleteObject", "s3:ListBucket"]
        operation_name = "S3-Delete"
        self._s3_kms_permission_check(
            operation_name, bucket_arn, None, s3_permissions, []
        )
        self.check_s3_versioning_enabled(bucket_arn)

    def has_create_na_permissions(self):
        """Check if the configured IAM role has permissions to create a Neptune Analytics instance.

        Raises:
            ValueError: If the role lacks required permissions

        Returns:
            None: The function doesn't return a value but raises an exception if permissions are insufficient

        """
        na_permissions = ["neptune-graph:CreateGraph", "neptune-graph:TagResource"]
        operation_name = "Create Neptune Instance"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_start_na_permissions(self):
        """Check if the configured IAM role has permissions to start a Neptune Analytics instance.

        Verifies that the role has the necessary permissions required to start a stopped
        Neptune Analytics instance.

        Raises:
            ValueError: If the role lacks required permissions for starting the instance

        Returns:
            None: The function doesn't return a value but raises an exception if permissions are insufficient

        Required permissions:
            - neptune-graph:StartGraph
        """
        na_permissions = ["neptune-graph:StartGraph"]
        operation_name = "Start Neptune Instance"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_stop_na_permissions(self):
        """Check if the configured IAM role has permissions to stop a Neptune Analytics instance.

        Verifies that the role has the necessary permissions required to stop a running
        Neptune Analytics instance.

        Raises:
            ValueError: If the role lacks required permissions for stopping the instance

        Returns:
            None: The function doesn't return a value but raises an exception if permissions are insufficient

        Required permissions:
            - neptune-graph:StopGraph
        """
        na_permissions = ["neptune-graph:StopGraph"]
        operation_name = "Stop Neptune Instance"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_delete_snapshot_permissions(self):
        """Check if the configured IAM role has permissions to delete a Neptune Analytics snapshot.

        Verifies that the role has the necessary permissions required to delete a snapshot
        of a Neptune Analytics instance.

        Raises:
            ValueError: If the role lacks required permissions for snapshot deletion operations

        Returns:
            None: The function doesn't return a value but raises an exception if permissions are insufficient

        Required permissions:
            - neptune-graph:DeleteGraphSnapshot
        """
        na_permissions = ["neptune-graph:DeleteGraphSnapshot"]
        operation_name = "Delete Neptune Snapshot"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_create_na_from_snapshot_permissions(self):
        """Check if the configured IAM role has permissions to create a Neptune Analytics instance from a snapshot.

        Verifies that the role has the necessary permissions required to restore a Neptune Analytics
        instance from an existing snapshot, including tagging resources and restoring from snapshot.

        Raises:
            ValueError: If the role lacks required permissions for snapshot restore operations

        Returns:
            None: The function doesn't return a value but raises an exception if permissions are insufficient

        Required permissions:
            - neptune-graph:TagResource
            - neptune-graph:RestoreGraphFromSnapshot
        """
        na_permissions = [
            "neptune-graph:TagResource",
            "neptune-graph:RestoreGraphFromSnapshot",
        ]
        operation_name = "Create Neptune Instance From Snapshot"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_create_na_snapshot_permissions(self):
        """Check if the configured IAM role has permissions to create a Neptune Analytics snapshot.

        Verifies that the role has the necessary permissions required to create a snapshot
        of a Neptune Analytics instance, including tagging resources and creating snapshots.

        Raises:
            ValueError: If the role lacks required permissions for snapshot creation operations

        Returns:
            None: The function doesn't return a value but raises an exception if permissions are insufficient

        Required permissions:
            - neptune-graph:TagResource
            - neptune-graph:CreateGraphSnapshot
        """
        na_permissions = [
            "neptune-graph:TagResource",
            "neptune-graph:CreateGraphSnapshot",
        ]
        operation_name = "Create Neptune Analytics Snapshot"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_delete_na_permissions(self):
        """Check if the configured IAM role has permissions to delete Neptune Analytics instance.

        Raises:
            ValueError: If the role lacks required permissions

        Returns:
            None: The function doesn't return a value but raises an exception if permissions are insufficient

        """
        na_permissions = ["neptune-graph:DeleteGraph"]
        operation_name = "Delete Neptune Instance"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_update_na_permissions(self):
        """Check if the configured IAM role has permissions to update a Neptune Analytics instance.

        Verifies that the role has the necessary permissions required to update a
        Neptune Analytics instance configuration.

        Raises:
            ValueError: If the role lacks required permissions for updating the instance

        Returns:
            None

        Required permissions:
            - neptune-graph:UpdateGraph
        """
        na_permissions = ["neptune-graph:UpdateGraph"]
        operation_name = "Update Neptune Instance"
        # Check permission
        self._check_neptune_permission(operation_name, na_permissions)

    def has_import_from_s3_permissions(self, bucket_arn, key_arn=None):
        """Check if the configured IAM role has permissions to import data from S3.

        Verifies that the role has the necessary S3 and KMS permissions required
        for importing graph data from S3 to Neptune Analytics.

        Args:
            bucket_arn (str): The ARN of the S3 bucket
            key_arn (str, optional): The ARN of the KMS key, or None if not using KMS encryption.
                                     Defaults to None.

        Raises:
            ValueError: If the role lacks required permissions

        Returns:
            None
        """
        s3_permissions = ["s3:GetObject"]
        kms_permissions = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        operation_name = "S3-Import"
        self._s3_kms_permission_check(
            operation_name, bucket_arn, key_arn, s3_permissions, kms_permissions
        )

    @staticmethod
    def _validate_arns(arns: Union[str, list]) -> bool:
        """
        Validates a list of ARNs using the ArnParser.

        Args:
            arns: A single ARN string or a list of ARN strings to validate

        Raises:
            ValueError: If any ARN is invalid, with appropriate description

        Returns:
            bool: True if all ARNs are valid
        """
        # Convert single ARN to list if needed
        arn_list = [arns] if isinstance(arns, str) else arns

        # Validate each ARN
        arn_parser = ArnParser()
        for arn in arn_list:
            if arn and arn[-1] == "/":
                raise ValueError(f"Invalid ARN, '{arn}' ended with /")
            try:
                arn_parser.parse_arn(arn)
            except ValueError as e:
                raise ValueError(f"Invalid ARN format for '{arn}': {e}")

        # All ARNs are valid if we reach here
        return True

    def has_athena_permissions(
        self, s3_bucket: Optional[str] = None, kms_key_arn: Optional[str] = None
    ):
        """Check if the role has required Athena/S3/KMS permissions.

        Args:
            s3_bucket: S3 bucket path for query results
            kms_key_arn: KMS key ARN for encryption

        Returns:
            bool: True if all permissions are available

        Raises:
            Exception: If permissions are insufficient
        """
        permissions = ["athena:StartQueryExecution", "athena:GetQueryExecution"]
        operation_name = "Athena query execution"
        self.check_aws_permission(operation_name, permissions)

        if s3_bucket:
            s3_permissions = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
            self._check_s3_permission(operation_name, s3_permissions, s3_bucket)

        if s3_bucket:
            self.check_s3_versioning_enabled(s3_bucket)

        if kms_key_arn:
            kms_permissions = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
            self.check_aws_permission(operation_name, kms_permissions, kms_key_arn)

        return True

    def validate_permissions(
        self,
        arn_s3_bucket_import,
        arn_kms_key_import,
        arn_s3_bucket_export,
        arn_kms_key_export,
    ):
        """
        Validates all required permissions for Neptune Analytics operations.

        This method checks permissions for creating/deleting Neptune Analytics instances and S3/KMS permissions
        for import/export operations.

        Args:
            arn_s3_bucket_import (str): ARN of S3 bucket used for importing data
            arn_kms_key_import (str): ARN of KMS key used for import encryption
            arn_s3_bucket_export (str): ARN of S3 bucket used for exporting data
            arn_kms_key_export (str): ARN of KMS key used for export encryption

        Returns:
            dict: Dictionary mapping operation names to boolean permission status:
                {
                    'create_graph': bool,
                    'delete_na_instance': bool,
                    'import_from_s3': bool,
                    'export_to_s3': bool
                }

        The method checks the following permissions:
            - Neptune Graph: CreateGraph, TagResource, DeleteGraph
            - S3: GetObject (import), PutObject/ListBucket (export)
            - KMS: Decrypt, GenerateDataKey, DescribeKey (for both import/export)
        """
        results = {}

        # S3 paths are split into bucket and object ARNs at check time
        s3_import = arn_s3_bucket_import or None
        s3_export = arn_s3_bucket_export or None

        checks = {
            "create_na_instance": [
                {
                    "permissions": [
                        "neptune-graph:CreateGraph",
                        "neptune-graph:TagResource",
                    ]
                }
            ],
            "delete_na_instance": [{"permissions": ["neptune-graph:DeleteGraph"]}],
            "start_graph": [{"permissions": ["neptune-graph:StartGraph"]}],
            "stop_graph": [{"permissions": ["neptune-graph:StopGraph"]}],
            "import_from_s3": [
                {"permissions": ["s3:GetObject"], "s3_path": s3_import},
                {
                    "permissions": [
                        "kms:Decrypt",
                        "kms:GenerateDataKey",
                        "kms:DescribeKey",
                    ],
                    "arn": arn_kms_key_import,
                },
            ],
            "export_csv_to_s3": [
                {
                    "permissions": ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                    "s3_path": s3_export,
                },
                {
                    "permissions": [
                        "kms:Decrypt",
                        "kms:GenerateDataKey",
                        "kms:DescribeKey",
                    ],
                    "arn": arn_kms_key_export,
                },
            ],
            "empty_s3_bucket": [
                {
                    "permissions": ["s3:PutObject", "s3:ListBucket"],
                    "s3_path": s3_export,
                },
            ],
            "export_athena_table_to_s3": [
                {"permissions": ["s3:GetObject"], "s3_path": s3_import},
                {
                    "permissions": [
                        "kms:Decrypt",
                        "kms:GenerateDataKey",
                        "kms:DescribeKey",
                    ],
                    "arn": arn_kms_key_import,
                },
                {
                    "permissions": [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                    ]
                },
            ],
            "create_csv_table_from_s3": [
                {
                    "permissions": ["s3:GetObject", "s3:ListBucket"],
                    "s3_path": s3_export,
                },
                {
                    "permissions": [
                        "kms:Decrypt",
                        "kms:GenerateDataKey",
                        "kms:DescribeKey",
                    ],
                    "arn": arn_kms_key_export,
                },
                {
                    "permissions": [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                    ]
                },
            ],
            "create_iceberg_table_from_table": [
                {
                    "permissions": [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                    ]
                },
            ],
            "create_graph_snapshot": [
                {
                    "permissions": [
                        "neptune-graph:TagResource",
                        "neptune-graph:CreateGraphSnapshot",
                    ]
                },
            ],
            "create_na_instance_from_snapshot": [
                {
                    "permissions": [
                        "neptune-graph:TagResource",
                        "neptune-graph:RestoreGraphFromSnapshot",
                    ]
                },
            ],
            "delete_graph_snapshot": [
                {
                    "permissions": [
                        "neptune-graph:DeleteGraphSnapshot",
                    ]
                },
            ],
        }

        for op, permission_pairs in checks.items():
            for pair in permission_pairs:
                try:
                    granted = self._check_pair(op, pair)
                except Exception as e:
                    self.logger.debug(e)
                    results[op] = False
                    break
                # check_aws_permission reports an implicit deny as False without
                # raising, so consult the returned mapping as well as exceptions.
                if not all(granted.values()):
                    results[op] = False
                    break
                results.setdefault(op, True)

        return results

    def _check_pair(self, operation_name: str, pair: dict) -> dict:
        """Run one permission group of ``validate_permissions`` against its proper resource."""
        permissions = pair["permissions"]
        if pair.get("s3_path"):
            return self._check_s3_permission(
                operation_name, permissions, pair["s3_path"]
            )
        if pair.get("arn"):
            return self.check_aws_permission(operation_name, permissions, pair["arn"])
        if permissions[0].startswith(f"{SERVICE_NA}:"):
            return self._check_neptune_permission(operation_name, permissions)
        return self.check_aws_permission(operation_name, permissions)


def split_s3_arn_to_bucket_and_path(s3_arn: str) -> tuple:
    """
    Splits out the s3 arn as a bucket and path
    :param s3_arn: S3 URI (e.g., 's3://bucket-name/prefix/') or plain path (e.g., 'bucket-name/prefix')
    :return: (str, str) - a tuple containing the bucket and path
    """

    raw = s3_arn.replace("s3://", "") if s3_arn.startswith("s3://") else s3_arn
    bucket_path = raw.split("/")
    bucket_name = bucket_path.pop(0)
    bucket_path_str = "/".join(bucket_path)

    return bucket_name, bucket_path_str


def _get_s3_in_arn(s3_path: str, partition: str = "aws") -> str:
    """
    Converts an S3 path to an ARN format for use in IAM policy evaluation.

    This method transforms S3 paths by:
    1. Removing any trailing slashes
    2. Replacing the 's3://' prefix with 'arn:<partition>:s3:::'

    Args:
        s3_path (str): The S3 path to convert.
        partition (str): AWS partition for the ARN. Defaults to 'aws'.

    Returns:
        str: The S3 path converted to ARN format (arn:aws:s3:::bucket-name/folder)
             or the original path with trailing slashes removed if it doesn't
             start with 's3://'
    """
    s3_path = s3_path.rstrip("/")
    s3_path = s3_path.replace("s3://", f"arn:{partition}:s3:::", 1)
    return s3_path


def _split_s3_bucket_and_prefix(s3_path: str) -> tuple:
    """Split an ``s3://`` URI, plain path or S3 ARN into (bucket, prefix without slashes)."""
    path = re.sub(r"^(s3://|arn:[^:]*:s3:::)", "", s3_path)
    bucket, prefix = split_s3_arn_to_bucket_and_path(path)
    return bucket, prefix.strip("/")


def _get_s3_bucket_and_object_arns(s3_path: str, partition: str = "aws") -> tuple:
    """
    Build the (bucket ARN, object ARN) pair used to simulate S3 permissions.

    Bucket-level actions (e.g. s3:ListBucket) apply to the bucket ARN; object-level
    actions apply to ``<bucket>/<prefix>/*``, or ``<bucket>/*`` when there is no prefix.
    Accepts ``s3://`` URIs, plain ``bucket/prefix`` paths and S3 ARNs.
    """
    bucket, prefix = _split_s3_bucket_and_prefix(s3_path)
    bucket_arn = f"arn:{partition}:s3:::{bucket}"
    object_arn = f"{bucket_arn}/{prefix}/*" if prefix else f"{bucket_arn}/*"
    return bucket_arn, object_arn


def _convert_sts_to_iam_arn(sts_arn):
    """
    Convert an STS assumed-role ARN to an IAM role ARN, preserving the partition.

    Example:
    arn:aws:sts::ACCOUNT:assumed-role/ROLE/SESSION
    to
    arn:aws:iam::ACCOUNT:role/ROLE

    Also handles roles with paths:
    arn:aws:sts::ACCOUNT:assumed-role/PATH/ROLE/SESSION
    to
    arn:aws:iam::ACCOUNT:role/PATH/ROLE
    """
    parts = sts_arn.split(":")
    if len(parts) < 6 or parts[0] != "arn" or parts[2] != "sts":
        raise ValueError("Input is not a valid STS assumed-role ARN")

    partition = parts[1]
    account_id = parts[4]
    resource_part = ":".join(parts[5:])  # e.g., "assumed-role/path/role-name/session"

    # Drop the "assumed-role" prefix and the session suffix; the rest is path + name
    role_with_path = "/".join(resource_part.split("/")[1:-1])

    return f"arn:{partition}:iam::{account_id}:role/{role_with_path}"
