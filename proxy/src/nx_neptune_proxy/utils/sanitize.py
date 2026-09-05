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


_SAFE_FILENAME_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ "
)


def sanitize_filename(name: str) -> str:
    """Remove characters unsafe for filenames, collapse whitespace, truncate."""
    cleaned = "".join(c if c in _SAFE_FILENAME_CHARS else "_" for c in name)
    cleaned = "_".join(cleaned.split())  # collapse whitespace
    return cleaned[:80] or "project"


def sanitize_s3_key_name(name: str) -> str:
    """Sanitize a project name for use as an S3 object key component.

    Ensures the output is safe for S3 keys by:
    - Replacing spaces with underscores
    - Stripping characters outside [a-zA-Z0-9_-]

    S3 key requirements (https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html):
    - Max 1024 bytes UTF-8
    - Safe characters: alphanumeric, !, -, _, ., *, ', (, )
    - Characters to avoid: \\, {, }, ^, %, `, ], [, ", <, >, ~, #, |

    This function is conservative — it only allows alphanumeric, hyphens,
    and underscores to avoid any ambiguity with URL encoding or special
    handling by S3 console/CLI tools.
    """
    name = name.replace(" ", "_")
    return re.sub(r"[^a-zA-Z0-9_-]", "", name)
