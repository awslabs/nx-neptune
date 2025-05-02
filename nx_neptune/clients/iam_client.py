import logging
import os

import boto3
import jmespath
from botocore.exceptions import ClientError
from botocore.utils import ArnParser

__all__ = ["IamClient"]


class IamClient:
    """
    IAM Client is used to interact with AWS IAM service for role-based operations
    related to Neptune Analytics permissions and access control.

    This client provides methods to verify IAM role permissions for S3 operations,
    check trust relationships for service principals, and validate ARNs.

    The IAM role ARN can be provided as an argument. Otherwise, the ARN_IAM_ROLE environment variable is used.
    """

    def __init__(
        self,
        role_arn=None,
        logger=None,
        client=None,
    ):
        """
        Constructs an IAMClient object for AWS IAM service interaction,
        with optional custom logger and boto client.

        Args:
            role_arn (str, optional): The ARN of the IAM role to use. If None, will try to read from
                                      the ARN_IAM_ROLE environment variable. Defaults to None.
            logger (logging.Logger, optional): Custom logger. Creates a default logger if None is provided.
            client (boto3.client, optional): Custom boto3 IAM client. Creates a default client if None is provided.
        """
        self.role_arn = role_arn or os.getenv("ARN_IAM_ROLE")
        self.logger = logger or logging.getLogger(__name__)
        self.client = client or boto3.client("iam")

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
            response = self.client.get_role(RoleName=iam_role_arn)

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
                    if service_principal in services:
                        return True
            return False

        except ClientError as e:
            raise e

    def check_aws_permission(self, permissions: list, resource_arn: str) -> dict:
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
            self._validate_arns([self.role_arn, resource_arn])
            self.logger.info(
                f"Perform role permission check with: \n"
                f" Role [{self.role_arn}], \n"
                f" Permission: [{permissions}]\n"
                f" Resources: [{resource_arn}]\n"
            )
            # Execute the permission check
            response = self.client.simulate_principal_policy(
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

                # Map the decision to a boolean - check against list of allowed decisions
                results[action_name] = decision in allowed_decisions
            self.logger.debug(
                f"Permission check on resource [{resource_arn}], with result: {results}"
            )
            return results

        except ClientError as e:
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

        service_name = "neptune-graph"
        bucket_full_path = bucket_arn.replace("s3://", "arn:aws:s3:::", 1)

        if not self.check_assume_role(service_name):
            raise ValueError(f"Missing role assume on principle {service_name}")
        # Check S3
        check_result = self.check_aws_permission(s3_permissions, bucket_full_path)

        # Check KMS
        if key_arn is not None:
            kms_result = self.check_aws_permission(kms_permissions, key_arn)
            check_result = check_result | kms_result

        for name, value in check_result.items():
            if value is False:
                raise ValueError(
                    f"Insufficient permission, {name} need to be grant for operation {operation_name}"
                )

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
        s3_permissions = ["s3:PutObject", "s3:DeleteObject"]
        kms_permissions = ["kms:Decrypt", "kms:GenerateDataKey", "kms:DescribeKey"]
        operation_name = "S3-Export"
        self._s3_kms_permission_check(
            operation_name, bucket_arn, key_arn, s3_permissions, kms_permissions
        )

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
            try:
                arn_parser.parse_arn(arn)
            except ValueError as e:
                raise ValueError(f"Invalid ARN format for '{arn}': {e}")

        # All ARNs are valid if we reach here
        return True
