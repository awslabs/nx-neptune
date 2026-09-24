# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Log output strips ARNs/account ids and omits full tracebacks."""

import logging

import pytest

from nx_neptune_proxy.utils.sanitize import RedactingLogFilter


@pytest.fixture
def redactor():
    return RedactingLogFilter()


def _record(msg, *args, exc_info=None, level=logging.INFO):
    return logging.LogRecord(
        name="test",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )


class TestLogRedaction:
    def test_arn_is_stripped(self, redactor):
        rec = _record(
            "User arn:aws:iam::123456789012:user/dev is not authorized"
        )
        redactor.filter(rec)
        rendered = rec.getMessage()
        assert "arn:aws" not in rendered
        assert "[ARN]" in rendered
        assert "123456789012" not in rendered

    def test_bare_account_id_is_stripped(self, redactor):
        rec = _record("Deploying into account 210987654321 now")
        redactor.filter(rec)
        rendered = rec.getMessage()
        assert "210987654321" not in rendered
        assert "[ACCOUNT_ID]" in rendered

    def test_access_key_id_is_stripped(self, redactor):
        rec = _record("key AKIAIOSFODNN7EXAMPLE leaked")
        redactor.filter(rec)
        rendered = rec.getMessage()
        assert "AKIAIOSFODNN7EXAMPLE" not in rendered
        assert "[AWS_ID]" in rendered

    def test_args_are_formatted_then_redacted(self, redactor):
        rec = _record("Recorded graph_id %s not retrievable", "123456789012")
        redactor.filter(rec)
        rendered = rec.getMessage()
        assert "123456789012" not in rendered
        assert rec.args is None

    def test_traceback_is_omitted(self, redactor):
        try:
            raise RuntimeError("boom in arn:aws:iam::123456789012:role/x")
        except RuntimeError:
            import sys

            rec = _record("Pipeline failed", exc_info=sys.exc_info())

        redactor.filter(rec)
        rendered = rec.getMessage()
        assert "Traceback" not in rendered
        assert "test_log_redaction.py" not in rendered
        assert "[traceback omitted]" in rendered
        assert "RuntimeError" in rendered
        assert "arn:aws" not in rendered
        assert "123456789012" not in rendered
        assert rec.exc_info is None

    def test_clean_message_passes_through(self, redactor):
        rec = _record("Graph ready")
        redactor.filter(rec)
        assert rec.getMessage() == "Graph ready"

    def test_filter_always_returns_true(self, redactor):
        rec = _record("anything")
        assert redactor.filter(rec) is True
