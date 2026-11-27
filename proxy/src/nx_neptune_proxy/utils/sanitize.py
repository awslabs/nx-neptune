# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Utilities for sanitizing sensitive data from user-facing messages."""

import re

# Patterns that leak account info — strip from user-facing messages.
# Order matters: ARNs are replaced whole before the bare-account-id pattern
# could pick the account field out of one.
_SENSITIVE_PATTERNS = [
    (re.compile(r"arn:aws[^:\s]*:[^:\s]*:[^:\s]*:\d{12}:[^\s,\"']+"), "[ARN]"),
    (re.compile(r"(AKIA|ASIA|AROA|AIDA)[0-9A-Z]{16}"), "[AWS_ID]"),
    # A bare 12-digit account id, e.g. "User: 123456789012 is not authorized".
    # Bounded by non-digits so a longer number is not partly rewritten.
    (re.compile(r"(?<!\d)\d{12}(?!\d)"), "[ACCOUNT_ID]"),
]


def sanitize_error_message(message: str) -> str:
    """Remove AWS account IDs, ARNs, and credentials from error messages.

    Best-effort redaction of known shapes, not a guarantee that an arbitrary
    AWS message is safe to show. Prefer a fixed message built from the error
    *code* for anything persisted or served by an API; use this where the
    original text still has diagnostic value.
    """
    for pattern, replacement in _SENSITIVE_PATTERNS:
        message = pattern.sub(replacement, message)
    return message


_SAFE_FILENAME_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ "
)


def sanitize_filename(name: str) -> str:
    """Remove characters unsafe for filenames, collapse whitespace, truncate."""
    cleaned = "".join(c if c in _SAFE_FILENAME_CHARS else "_" for c in name)
    cleaned = "_".join(cleaned.split())  # collapse whitespace
    return cleaned[:80] or "project"
