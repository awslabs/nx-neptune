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
from typing import Optional, Union

import jmespath
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.utils import ArnParser

__all__ = ["IamClientWrapper", "split_s3_arn_to_bucket_and_path"]

from .client_factory import ClientFactory
from .neptune_constants import APP_ID_NX, SERVICE_NA

_S3_BUCKET_ACTIONS = {
    "s3:ListBucket",
    "s3:GetBucketVersioning",
    "s3:GetEncryptionConfiguration",
    "s3:GetBucketLocation",
}
_NEPTUNE_STAR_ACTIONS = {
    "neptune-graph:CreateGraph",
    "neptune-graph:CreateGraphUsingImportTask",
    "neptune-graph:ListGraphs",
}
_NEPTUNE_SNAPSHOT_ACTIONS = {
    "neptune-graph:CreateGraphSnapshot",
    "neptune-graph:DeleteGraphSnapshot",
    "neptune-graph:GetGraphSnapshot",
    "neptune-graph:RestoreGraphFromSnapshot",
}


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
    ):
        """
        Constructs an IAM Client object for AWS IAM service interaction, with logger and boto client.

        Args:
            role_arn (str): The ARN of the IAM role to use.
            client (BaseClient): Custom boto3 IAM client.
            logger (logging.Logger): Custom logger. Creates a default logger if None is provided.
        """
        if "sts" in role_arn:
            self.role_arn = _convert_sts_to_iam_arn(role_arn)
        else:
            self.role_arn = role_arn
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

    def _arn_partition(self) -> str:
        if not isinstance(self.role_arn, str):
            return "aws"
        parts = self.role_arn.split(":")
        return parts[1] if len(parts) > 2 else "aws"

    def _arn_account(self) -> str:
        if not isinstance(self.role_arn, str):
            return ""
        parts = self.role_arn.split(":")
        return parts[4] if len(parts) > 4 else ""

    def _simulation_region(self) -> str:
        region = getattr(getattr(self.client, "meta", None), "region_name", None)
        if isinstance(region, str) and region:
            return region
        try:
            import boto3

            session_region = boto3.session.Session().region_name
            if session_region:
                return session_region
        except Exception:
            pass
        return "us-east-1"

    def _neptune_resource_arn(
        self, resource_type: str, region: Optional[str] = None
    ) -> str:
        account = self._arn_account()
        if not account:
            return "*"
        region = region or self._simulation_region()
        return (
            f"arn:{self._arn_partition()}:neptune-graph:{region}:"
            f"{account}:{resource_type}/*"
        )

    def _neptune_context_entries(self, region: Optional[str] = None) -> list:
        region = region or self._simulation_region()
        return [
            {
                "ContextKeyName": "aws:RequestTag/agent",
                "ContextKeyValues": [APP_ID_NX],
                "ContextKeyType": "string",
            },
            {
                "ContextKeyName": "aws:ResourceTag/agent",
                "ContextKeyValues": [APP_ID_NX],
                "ContextKeyType": "string",
            },
            {
                "ContextKeyName": "aws:RequestedRegion",
                "ContextKeyValues": [region],
                "ContextKeyType": "string",
            },
        ]

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
            context_entries (list, optional): IAM simulation context keys (tags, region)

        Returns:
            dict: A dictionary mapping each permission to a boolean indicating if it's allowed

        Raises:
            ValueError: If input parameters are invalid
            ClientError: If there's an issue with the AWS API call
        """
        allowed_decisions = ["allowed"]

        try:
            # Validate ARN formats (skip non-strings from mocked callers)
            if resource_arn != "*":
                to_validate = [
                    arn for arn in (self.role_arn, resource_arn) if isinstance(arn, str)
                ]
                if to_validate:
                    self._validate_arns(to_validate)
            self.logger.debug(
                f"Perform role permission check with: \n"
                f" Role [{self.role_arn}], \n"
                f" Permission: [{permissions}]\n"
                f" Resources: [{resource_arn}]\n"
            )
            simulate_kwargs = {
                "PolicySourceArn": self.role_arn,
                "ActionNames": permissions,
                "ResourceArns": [resource_arn],
            }
            if context_entries:
                simulate_kwargs["ContextEntries"] = context_entries
            # Execute the permission check
            response = self.client.simulate_principal_policy(  # type: ignore[attr-defined]
                **simulate_kwargs
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

    def _check_neptune_permission(self, operation_name: str, permissions: list) -> dict:
        """Simulate Neptune actions against the ARNs IAM actually evaluates.

        Create/List stay on ``*``. Snapshot actions use ``snapshot/*``.
        Everything else, including TagResource, uses ``graph/*``. Tag
        and region context is supplied so least-privilege policies match.
        """
        region = self._simulation_region()
        context = self._neptune_context_entries(region)
        star = [p for p in permissions if p in _NEPTUNE_STAR_ACTIONS]
        snap = [p for p in permissions if p in _NEPTUNE_SNAPSHOT_ACTIONS]
        graph = [
            p
            for p in permissions
            if p not in _NEPTUNE_STAR_ACTIONS and p not in _NEPTUNE_SNAPSHOT_ACTIONS
        ]
        results = {}
        if star:
            results.update(
                self.check_aws_permission(
                    operation_name, star, "*", context_entries=context
                )
            )
        if graph:
            results.update(
                self.check_aws_permission(
                    operation_name,
                    graph,
                    self._neptune_resource_arn("graph", region),
                    context_entries=context,
                )
            )
        if snap:
            results.update(
                self.check_aws_permission(
                    operation_name,
                    snap,
                    self._neptune_resource_arn("snapshot", region),
                    context_entries=context,
                )
            )
        return results

    def _check_s3_permissions(
        self, operation_name: str, s3_path: str, s3_permissions: list
    ) -> dict:
        """Simulate bucket actions on the bucket ARN and object actions on objects."""
        bucket_arn, object_arn = _s3_simulation_targets(s3_path, self._arn_partition())
        bucket_perms = [p for p in s3_permissions if p in _S3_BUCKET_ACTIONS]
        object_perms = [p for p in s3_permissions if p not in _S3_BUCKET_ACTIONS]
        results = {}
        if bucket_perms:
            results.update(
                self.check_aws_permission(operation_name, bucket_perms, bucket_arn)
            )
        if object_perms:
            results.update(
                self.check_aws_permission(operation_name, object_perms, object_arn)
            )
        return results

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
        # Check S3: ListBucket against the bucket, object actions against objects
        self._check_s3_permissions(operation_name, bucket_arn, s3_permissions)

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
            self._check_s3_permissions(operation_name, s3_bucket, s3_permissions)

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

        s3_import = arn_s3_bucket_import
        s3_export = arn_s3_bucket_export

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
                {"permissions": ["s3:GetObject"], "arn": s3_import},
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
                {"permissions": ["s3:PutObject", "s3:ListBucket"], "arn": s3_export},
            ],
            "export_athena_table_to_s3": [
                {"permissions": ["s3:GetObject"], "arn": s3_import},
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
                {"permissions": ["s3:GetObject", "s3:ListBucket"], "arn": s3_export},
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
                    perms = pair["permissions"]
                    if "arn" in pair and pair["arn"]:
                        if all(p.startswith("s3:") for p in perms):
                            self._check_s3_permissions(op, pair["arn"], perms)
                        else:
                            self.check_aws_permission(op, perms, pair["arn"])
                    elif all(p.startswith("neptune-graph:") for p in perms):
                        self._check_neptune_permission(op, perms)
                    else:
                        self.check_aws_permission(op, perms)
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


def _get_s3_in_arn(s3_path: str, partition: str = "aws") -> str:
    """
    Converts an S3 path to an ARN format for use in IAM policy evaluation.

    This method transforms S3 paths by:
    1. Removing any trailing slashes
    2. Replacing the 's3://' prefix with 'arn:{partition}:s3:::'

    Args:
        s3_path (str): The S3 path to convert.
        partition (str): AWS partition (aws, aws-us-gov, aws-cn). Defaults to aws.

    Returns:
        str: The S3 path converted to ARN format (arn:aws:s3:::bucket-name/folder)
             or the original path with trailing slashes removed if it doesn't
             start with 's3://'
    """
    s3_path = s3_path.rstrip("/")
    if s3_path.startswith("s3://"):
        return s3_path.replace("s3://", f"arn:{partition}:s3:::", 1)
    return s3_path


def _s3_simulation_targets(s3_path: str, partition: str = "aws") -> tuple:
    """Return (bucket_arn, object_arn) for IAM SimulatePrincipalPolicy.

    ListBucket is evaluated against the bucket ARN. Object actions are
    evaluated against ``arn:...:s3:::bucket/prefix/*`` (or ``bucket/*``
    when the path has no prefix).
    """
    if s3_path.startswith("arn:"):
        parts = s3_path.split(":")
        if len(parts) > 1 and parts[1]:
            partition = parts[1]
        rest = s3_path.split(":::", 1)[-1].rstrip("/")
    else:
        rest = s3_path.replace("s3://", "", 1).rstrip("/")
    if "/" in rest:
        bucket, prefix = rest.split("/", 1)
        object_arn = f"arn:{partition}:s3:::{bucket}/{prefix}/*"
    else:
        bucket = rest
        object_arn = f"arn:{partition}:s3:::{bucket}/*"
    return f"arn:{partition}:s3:::{bucket}", object_arn


def _convert_sts_to_iam_arn(sts_arn):
    """
    Convert an STS assumed-role ARN to an IAM role ARN.

    Example:
    arn:aws:sts::ACCOUNT:assumed-role/ROLE/SESSION
    to
    arn:aws:iam::ACCOUNT:role/ROLE

    Also handles roles with paths:
    arn:aws:sts::ACCOUNT:assumed-role/PATH/ROLE/SESSION
    to
    arn:aws:iam::ACCOUNT:role/PATH/ROLE

    Partition is taken from the source ARN (aws, aws-us-gov, aws-cn).
    """
    parts = sts_arn.split(":")
    if len(parts) < 6 or parts[0] != "arn" or parts[2] != "sts":
        raise ValueError("Input is not a valid STS assumed-role ARN")

    partition = parts[1]
    account_id = parts[4]
    resource_part = parts[5]  # e.g., "assumed-role/path/role-name/session"

    # Split by "/" and remove "assumed-role" prefix and session suffix
    resource_parts = resource_part.split("/")
    # parts[0] = "assumed-role", parts[-1] = session name
    # Everything in between is the role path and name
    role_with_path = "/".join(resource_parts[1:-1])

    return f"arn:{partition}:iam::{account_id}:role/{role_with_path}"
