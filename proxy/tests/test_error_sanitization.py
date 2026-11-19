# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AWS error message sanitization — no ARNs, account IDs, or keys in responses."""

from botocore.exceptions import ClientError

from nx_neptune_proxy.routers.projection import _graph_delete_error
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

    def test_bare_account_id_stripped(self):
        """The docstring promised account ids; only full ARNs were matched.

        A poll-time AWS error carrying a bare account id was stored on the row
        and served back by GET /status.
        """
        msg = "User: 123456789012 is not authorized to perform: neptune-graph:GetGraph"
        result = sanitize_error_message(msg)
        assert "123456789012" not in result
        assert "[ACCOUNT_ID]" in result

    def test_longer_digit_runs_are_left_alone(self):
        """Bounded by non-digits, so a 13+ digit number is not partly rewritten."""
        msg = "Request id 12345678901234567 took 1234567890123 ms"
        result = sanitize_error_message(msg)
        assert "12345678901234567" in result
        assert "1234567890123" in result
        assert "[ACCOUNT_ID]" not in result

    def test_account_id_inside_an_arn_is_replaced_once_with_the_arn(self):
        """ARNs are substituted first, so the id is not double-rewritten."""
        msg = "Resource arn:aws:iam::123456789012:role/R denied"
        result = sanitize_error_message(msg)
        assert "123456789012" not in result
        assert "[ARN]" in result
        assert "[ACCOUNT_ID]" not in result

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


class TestPersistedGraphDeleteError:
    """What is stored on a row and served by GET /status carries no raw AWS text.

    ``sanitize_error_message`` is best-effort pattern matching, so anything it
    has not been taught about - a resource name, a principal, an id in a shape
    it does not match - would be persisted and then served straight back.
    """

    @staticmethod
    def _error(code, message):
        return ClientError({"Error": {"Code": code, "Message": message}}, "GetGraph")

    def test_only_the_error_code_is_kept(self):
        stored = _graph_delete_error(
            self._error(
                "AccessDeniedException",
                "User proxy-role cannot GetGraph on nxp-customer-prod-graph",
            )
        )
        assert stored == "Could not confirm graph deletion: AccessDeniedException"

    def test_the_raw_message_does_not_survive(self):
        """Names the sanitizer has no pattern for must not reach the row."""
        stored = _graph_delete_error(
            self._error("ThrottlingException", "bucket acme-private-exports throttled")
        )
        assert "acme-private-exports" not in stored

    def test_a_missing_code_still_yields_a_message(self):
        stored = _graph_delete_error(ClientError({"Error": {}}, "GetGraph"))
        assert stored == "Could not confirm graph deletion: UnknownError"
