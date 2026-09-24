# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Utilities for sanitizing sensitive data from user-facing messages."""

import logging
import re

# Patterns that leak account info — strip from user-facing messages.
_SENSITIVE_PATTERNS = [
    (re.compile(r"arn:aws[^:\s]*:[^:\s]*:[^:\s]*:\d{12}:[^\s,\"']+"), "[ARN]"),
    (re.compile(r"(AKIA|ASIA|AROA|AIDA)[0-9A-Z]{16}"), "[AWS_ID]"),
    (re.compile(r"\b\d{12}\b"), "[ACCOUNT_ID]"),
]


def sanitize_error_message(message: str) -> str:
    """Remove AWS account IDs, ARNs, and credentials from error messages."""
    for pattern, replacement in _SENSITIVE_PATTERNS:
        message = pattern.sub(replacement, message)
    return message


class RedactingLogFilter(logging.Filter):
    """Redact ARNs/account ids/keys from log records and drop tracebacks."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - never let logging raise
            message = str(record.msg)

        if record.exc_info or record.exc_text:
            exc = record.exc_info[1] if record.exc_info else None
            if exc is not None:
                message = f"{message} ({type(exc).__name__}: {exc})"
            message += " [traceback omitted]"

        record.msg = sanitize_error_message(message)
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return True
