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
from typing import Optional

import jmespath
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from botocore.utils import ArnParser

__all__ = ["IamClient"]

from .neptune_constants import SERVICE_NA


class IamClient:
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
            self.role_arn = convert_sts_to_iam_arn(role_arn)
        else:
            self.role_arn = role_arn
        self.client = client
        self.logger = logger or logging.getLogger(__name__)

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
        self, operation_name: str, permissions: list, resource_arn: str = "*"
    ) -> dict:
        """
        Validates if the configured IAM role has the required permissions for a specified resource ARN.

        Args:
            permissions (list): List of permission strings to check (e.g., ['s3:GetObject'])
            resource_arn (str): The resource ARN to check permissions against

        Returns:
            dict: A dictionary mapping each permission to a boolean indicating if it's allowed

        Raises:
            ValueError: If input parameters are invalid
            ClientError: If there's an issue with the AWS API call
        """
        allowed_decisions = ["allowed"]

        try:
            # Validate ARN formats
            if resource_arn != "*":
                self._validate_arns([self.role_arn, resource_arn])
            self.logger.info(
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
        self.logger.info(
            f"Permission check on ARN(s): {self.role_arn}, {bucket_arn}, {key_arn}"
        )

        bucket_full_path = _get_s3_in_arn(bucket_arn)
        if not self.check_assume_role(SERVICE_NA):
            raise ValueError(f"Missing role assume on principle {SERVICE_NA}")
        # Check S3
        self.check_aws_permission(operation_name, s3_permissions, bucket_full_path)

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
        s3_permissions = ["s3:PutObject", "s3:ListBucket"]
        kms_permissions = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        operation_name = "S3-Export"
        self._s3_kms_permission_check(
            operation_name, bucket_arn, key_arn, s3_permissions, kms_permissions
        )

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
        self.check_aws_permission(operation_name, na_permissions)

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
        self.check_aws_permission(operation_name, na_permissions)

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
    def _validate_arns(arns: str | list) -> bool:
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


def _get_s3_in_arn(s3_path: str) -> str:
    """
    Converts a S3 path to an ARN format for use in IAM policy evaluation.

    This method transforms S3 paths by:
    1. Removing any trailing slashes
    2. Replacing the 's3://' prefix with 'arn:aws:s3:::'

    Args:
        s3_path (str): The S3 path to convert.

    Returns:
        str: The S3 path converted to ARN format (arn:aws:s3:::bucket-name/folder)
             or the original path with trailing slashes removed if it doesn't
             start with 's3://'
    """
    s3_path = s3_path.rstrip("/")
    s3_path = s3_path.replace("s3://", "arn:aws:s3:::", 1)
    return s3_path


def convert_sts_to_iam_arn(sts_arn):
    """
    Convert an STS assumed-role ARN to an IAM role ARN.

    Example:
    arn:aws:sts::ACCOUNT:assumed-role/ROLE/SESSION
    to
    arn:aws:iam::ACCOUNT:role/ROLE
    """
    sts_prefix = "arn:aws:sts::"
    if not sts_arn.startswith(sts_prefix):
        raise ValueError("Input is not a valid STS assumed-role ARN")

    account_part = sts_arn[len(sts_prefix) :]
    account_id = account_part.split(":")[0]
    role_name = sts_arn.split(":")[5].split("/")[1]

    # Compose the IAM ARN
    iam_arn = f"arn:aws:iam::{account_id}:role/{role_name}"

    return iam_arn
