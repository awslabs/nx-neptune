# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Utilities for sanitizing sensitive data from user-facing messages."""

import re

# Patterns that leak account info — strip from user-facing messages
_SENSITIVE_PATTERNS = [
    (re.compile(r"arn:aws[^:\s]*:[^:\s]*:[^:\s]*:\d{12}:[^\s,\"']+"), "[ARN]"),
    (re.compile(r"(AKIA|ASIA|AROA|AIDA)[0-9A-Z]{16}"), "[AWS_ID]"),
]


def sanitize_error_message(message: str) -> str:
    """Remove AWS account IDs, ARNs, and credentials from error messages."""
    for pattern, replacement in _SENSITIVE_PATTERNS:
        message = pattern.sub(replacement, message)
    return message
