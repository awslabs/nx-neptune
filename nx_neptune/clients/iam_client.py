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
from typing import Optional, Union

import boto3
import jmespath
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.utils import ArnParser

__all__ = ["IamClientWrapper", "split_s3_arn_to_bucket_and_path"]

from .client_factory import ClientFactory
from .neptune_constants import SERVICE_NA

# S3 actions that IAM authorizes against the bucket ARN rather than an object ARN.
# Everything else in this module's permission sets is object-level.
S3_BUCKET_LEVEL_ACTIONS = frozenset(
    {
        "s3:ListBucket",
        "s3:ListBucketVersions",
        "s3:ListBucketMultipartUploads",
        "s3:GetBucketLocation",
        "s3:GetBucketVersioning",
    }
)

# Neptune Analytics resource types used in simulated ARNs.
_NA_RESOURCE_TYPE_GRAPH = "graph"
_NA_RESOURCE_TYPE_SNAPSHOT = "graph-snapshot"


class IamClientWrapper:
    """
    IAM Client is used to interact with AWS IAM service for role-based operations
    related to Neptune Analytics permissions and access control.

    This client provides methods to verify IAM role permissions for S3 operations,
    check trust relationships for service principals, and validate ARNs.

    The IAM role ARN is supplied through the ``role_arn`` argument.
    """

    def __init__(
        self,
        role_arn: str,
        client: BaseClient,
        logger: Optional[logging.Logger] = None,
        region: Optional[str] = None,
    ):
        """
        Constructs an IAM Client object for AWS IAM service interaction, with logger and boto client.

        Args:
            role_arn (str): The ARN of the IAM role to use.
            client (BaseClient): Custom boto3 IAM client.
            logger (logging.Logger): Custom logger. Creates a default logger if None is provided.
            region (str, optional): Region for the simulated Neptune Analytics resource
                ARNs. Defaults to the region ``ClientFactory`` resolves.
        """
        if _is_sts_arn(role_arn):
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
        """The region ``ClientFactory``'s clients use; ``None`` if none is configured.

        Order: the ``region=`` keyword, ``boto3.DEFAULT_SESSION`` (what the
        module-level ``boto3.client(...)`` calls in ``ClientFactory`` resolve
        through), then a fresh ``boto3.Session()``. A fresh Session ignores
        ``setup_default_session``, so the default session has to be consulted first
        for this to agree with the Neptune clients.

        Never the IAM client's ``meta.region_name``: IAM is global and reports a
        pseudo-region such as ``aws-global`` that no policy can match.
        """
        if self._region:
            return self._region
        default_session = boto3.DEFAULT_SESSION
        if default_session is not None and default_session.region_name:
            return default_session.region_name
        return boto3.Session().region_name

    def _neptune_graph_arn(self, resource_type: str = _NA_RESOURCE_TYPE_GRAPH) -> str:
        """Resource ARN matching any Neptune Analytics ``resource_type`` in the
        caller's account and region.

        With no region configured a wildcard keeps the ARN well formed rather than
        embedding "None". That is a fallback, not a match-anything: IAM matches a
        policy's Resource against this literal request ARN, so a grant pinned to one
        region will not match it.
        """
        region = self._resolved_region or "*"
        return (
            f"arn:{self._partition}:neptune-graph:{region}:"
            f"{self._account_id}:{resource_type}/*"
        )

    def _check_neptune_permission(self, operation_name: str, permissions: list) -> dict:
        """Simulate Neptune Analytics actions against the resource ARN IAM evaluates.

        Snapshot actions are authorized against a ``graph-snapshot`` ARN, everything
        else against a ``graph`` ARN, so the two groups go in separate requests.

        Known limitation: AWS lists two resource types for some actions
        (``CreateGraphSnapshot``, ``RestoreGraphFromSnapshot``, ``TagResource``),
        which are pinned to one type here. A grant scoped only to the other type is
        not confirmed.
        """
        by_type: dict = {}
        for action in permissions:
            resource_type = (
                _NA_RESOURCE_TYPE_SNAPSHOT
                if "Snapshot" in action
                else _NA_RESOURCE_TYPE_GRAPH
            )
            by_type.setdefault(resource_type, []).append(action)

        results: dict = {}
        for resource_type, actions in by_type.items():
            # Built here from the role ARN and region rather than supplied by a
            # caller, so these skip the user-input validation in check_aws_permission.
            results.update(
                self.check_aws_permission(
                    operation_name,
                    actions,
                    self._neptune_graph_arn(resource_type),
                    validate_arns=False,
                )
            )
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

        results: dict = {}
        if bucket_actions:
            results.update(
                self.check_aws_permission(operation_name, bucket_actions, bucket_arn)
            )
        if object_actions:
            results.update(
                self._check_s3_object_actions(operation_name, object_actions, s3_path)
            )
        return results

    def _check_s3_object_actions(
        self, operation_name: str, actions: list, s3_path: str
    ) -> dict:
        """Simulate object-level S3 actions, accepting a prefix or an exact-object grant.

        The path may name a prefix (``s3://bucket/staging``) or a single object
        (``s3://bucket/data/file.csv``), and nothing in the path distinguishes them.
        The prefix ARN ``<bucket>/<prefix>/*`` is tried first, since that is the
        common case; a grant written against the object itself does not match that
        ARN, so an exact ``<bucket>/<prefix>`` is tried before reporting the action
        missing. Either grant authorizes the real request, so either is sufficient
        here.
        """
        _, object_arn = _get_s3_bucket_and_object_arns(s3_path, self._partition)
        exact_arn = _s3_exact_object_arn(s3_path, self._partition)

        try:
            return self.check_aws_permission(operation_name, actions, object_arn)
        except ValueError:
            # No prefix means the object ARN already covers the bucket root, so
            # there is no narrower exact-object grant left to try.
            if exact_arn is None:
                raise
            self.logger.debug(
                f"Object actions {actions} not allowed on [{object_arn}], "
                f"retrying as an exact object grant on [{exact_arn}]"
            )
            return self.check_aws_permission(operation_name, actions, exact_arn)

    @classmethod
    def default(
        cls,
        sts_client: Optional[BaseClient] = None,
        iam_client: Optional[BaseClient] = None,
        logger: Optional[logging.Logger] = None,
    ) -> "IamClientWrapper":
        """
        Build an IamClientWrapper for the current caller. The role ARN is
        resolved from the STS caller identity (an STS network call). Clients
        not provided are created from the default ClientFactory.

        Args:
            sts_client (Optional[BaseClient]): STS boto3 client.
            iam_client (Optional[BaseClient]): IAM boto3 client.
            logger (Optional[logging.Logger]): Custom logger.

        Returns:
            IamClientWrapper: Wrapper bound to the caller's role ARN.
        """
        factory = ClientFactory()
        if sts_client is None:
            sts_client = factory.sts()
        if iam_client is None:
            iam_client = factory.iam()
        return cls(
            role_arn=sts_client.get_caller_identity()["Arn"],
            client=iam_client,
            logger=logger,
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
        validate_arns: bool = True,
    ) -> dict:
        """
        Validates if the configured IAM role has the required permissions for a specified resource ARN.

        Args:
            permissions (list): List of permission strings to check (e.g., ['s3:GetObject'])
            resource_arn (str): The resource ARN to check permissions against
            validate_arns (bool): Validate the ARN format first. False for ARNs this
                module builds itself, which are not caller input.

        Returns:
            dict: A dictionary mapping each permission to a boolean indicating if it's allowed

        Raises:
            ValueError: If input parameters are invalid
            ClientError: If there's an issue with the AWS API call
        """
        allowed_decisions = ["allowed"]

        try:
            # Validate ARN formats
            if validate_arns and resource_arn != "*":
                self._validate_arns([self.role_arn, resource_arn])
            self.logger.debug(
                f"Perform role permission check with: \n"
                f" Role [{self.role_arn}], \n"
                f" Permission: [{permissions}]\n"
                f" Resources: [{resource_arn}]\n"
            )
            # Execute the permission check
            response = self.client.simulate_principal_policy(  # type: ignore[attr-defined]
                PolicySourceArn=self.role_arn,
                ActionNames=permissions,
                ResourceArns=[resource_arn],
            )

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

                if decision not in allowed_decisions:
                    raise ValueError(
                        f"Insufficient permission, {action_name} need to be grant for operation {operation_name}"
                    )
                # Map the decision to a boolean - check against list of allowed decisions
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
        # Check S3: bucket-level and object-level actions go to different ARNs
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

        # Raw S3 paths; _check_s3_permission builds the bucket and object ARNs,
        # which differ per action and cannot share one ARN.
        s3_import = arn_s3_bucket_import or None
        s3_export = arn_s3_bucket_export or None

        checks = {
            "create_na_instance": [
                {
                    "kind": "neptune",
                    "permissions": [
                        "neptune-graph:CreateGraph",
                        "neptune-graph:TagResource",
                    ],
                }
            ],
            "delete_na_instance": [
                {"kind": "neptune", "permissions": ["neptune-graph:DeleteGraph"]}
            ],
            "start_graph": [
                {"kind": "neptune", "permissions": ["neptune-graph:StartGraph"]}
            ],
            "stop_graph": [
                {"kind": "neptune", "permissions": ["neptune-graph:StopGraph"]}
            ],
            "import_from_s3": [
                {"kind": "s3", "permissions": ["s3:GetObject"], "arn": s3_import},
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
                    "kind": "s3",
                    "permissions": ["s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                    "arn": s3_export,
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
                    "kind": "s3",
                    "permissions": ["s3:PutObject", "s3:ListBucket"],
                    "arn": s3_export,
                },
            ],
            "export_athena_table_to_s3": [
                {"kind": "s3", "permissions": ["s3:GetObject"], "arn": s3_import},
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
                    "kind": "s3",
                    "permissions": ["s3:GetObject", "s3:ListBucket"],
                    "arn": s3_export,
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
                    "kind": "neptune",
                    "permissions": [
                        "neptune-graph:TagResource",
                        "neptune-graph:CreateGraphSnapshot",
                    ],
                },
            ],
            "create_na_instance_from_snapshot": [
                {
                    "kind": "neptune",
                    "permissions": [
                        "neptune-graph:TagResource",
                        "neptune-graph:RestoreGraphFromSnapshot",
                    ],
                },
            ],
            "delete_graph_snapshot": [
                {
                    "kind": "neptune",
                    "permissions": [
                        "neptune-graph:DeleteGraphSnapshot",
                    ],
                },
            ],
        }

        for op, permission_pairs in checks.items():
            for pair in permission_pairs:
                try:
                    kind = pair.get("kind")
                    arn = pair.get("arn")
                    if kind == "neptune":
                        # Splits graph and graph-snapshot actions across requests.
                        self._check_neptune_permission(op, pair["permissions"])
                    elif kind == "s3" and arn:
                        # Splits bucket-level and object-level actions across requests.
                        self._check_s3_permission(op, pair["permissions"], arn)
                    elif arn:
                        self.check_aws_permission(op, pair["permissions"], arn)
                    else:
                        self.check_aws_permission(op, pair["permissions"])
                except Exception as e:
                    self.logger.debug(e)
                    results[op] = False
                    break
                results.setdefault(op, True)

        return results


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


def _s3_exact_object_arn(s3_path: str, partition: str = "aws") -> Optional[str]:
    """ARN for the path treated as one object, or ``None`` when it has no prefix.

    ``_get_s3_bucket_and_object_arns`` reads the path as a prefix and appends
    ``/*``. A policy granting an action on a single object does not match that, so
    this gives the preflight the exact ARN to fall back on.
    """
    bucket, prefix = _split_s3_bucket_and_prefix(s3_path)
    if not prefix:
        return None
    return f"arn:{partition}:s3:::{bucket}/{prefix}"


def _is_sts_arn(arn: str) -> bool:
    """True when the ARN's service field is exactly ``sts``.

    The single place the STS match is defined. It reads the service field rather
    than searching the whole string, so a plain IAM role whose name merely
    contains "sts" (``tests-role``, ``analysts``) is not treated as an STS ARN,
    and neither is a service that only resembles it (``stsx``, ``x-sts``).

    Too few components is False rather than an ``IndexError``, which lets the
    caller store an unrecognised string unchanged. The comparison is
    case-sensitive; AWS always writes the service field in lowercase.
    """
    parts = arn.split(":")
    return len(parts) > 2 and parts[2] == "sts"


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
    if (
        not _is_sts_arn(sts_arn)
        or len(parts) < 6
        or parts[0] != "arn"
        # An empty partition cannot produce a usable IAM ARN.
        or not parts[1]
    ):
        raise ValueError("Input is not a valid STS assumed-role ARN")

    partition = parts[1]
    account_id = parts[4]
    resource_part = ":".join(parts[5:])  # e.g., "assumed-role/path/role-name/session"

    # Drop the "assumed-role" prefix and the session suffix; the rest is path + name
    role_with_path = "/".join(resource_part.split("/")[1:-1])

    return f"arn:{partition}:iam::{account_id}:role/{role_with_path}"
