# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AWS error message sanitization — no ARNs, account IDs, or keys in responses."""

from nx_neptune_proxy.utils.sanitize import sanitize_error_message


class TestErrorSanitization:
    """Sensitive AWS identifiers must be stripped from user-facing errors."""

    def test_arn_stripped(self):
        msg = "User arn:aws:iam::123456789012:user/dev is not authorized"
        result = sanitize_error_message(msg)
        assert "123456789012" not in result
        assert "arn:aws" not in result
        assert "[ARN]" in result

    def test_sts_assumed_role_stripped(self):
        msg = "User arn:aws:sts::987654321098:assumed-role/MyRole/session is not authorized"
        result = sanitize_error_message(msg)
        assert "987654321098" not in result
        assert "MyRole" not in result
        assert "[ARN]" in result

    def test_account_id_in_arn_stripped(self):
        msg = "Resource arn:aws:neptune-graph:us-east-1:123456789012:graph/g-123 not found"
        result = sanitize_error_message(msg)
        assert "123456789012" not in result
        assert "arn:aws" not in result

    def test_access_key_stripped(self):
        msg = "The security token for AKIA1234567890ABCDEF is invalid"
        result = sanitize_error_message(msg)
        assert "AKIA1234567890ABCDEF" not in result
        assert "[AWS_ID]" in result

    def test_session_key_stripped(self):
        msg = "Token for ASIA1234567890ABCDEF expired"
        result = sanitize_error_message(msg)
        assert "ASIA1234567890ABCDEF" not in result
        assert "[AWS_ID]" in result

    def test_multiple_arns_stripped(self):
        msg = "arn:aws:iam::111111111111:role/A cannot assume arn:aws:iam::222222222222:role/B"
        result = sanitize_error_message(msg)
        assert "111111111111" not in result
        assert "222222222222" not in result

    def test_normal_message_unchanged(self):
        msg = "Graph not found"
        result = sanitize_error_message(msg)
        assert result == "Graph not found"

    def test_error_code_preserved(self):
        msg = "AccessDeniedException: User arn:aws:iam::123456789012:user/x cannot perform action"
        result = sanitize_error_message(msg)
        assert "AccessDeniedException" in result
        assert "cannot perform action" in result
