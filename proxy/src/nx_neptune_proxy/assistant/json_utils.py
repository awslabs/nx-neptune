# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Recover a JSON object from a model reply (spec §4.4.2 ``extract_json``).

Models sometimes wrap JSON in prose or ```json code fences, or emit multi-line
SQL/openCypher with raw (unescaped) newlines inside string values. This module
recovers a parsed object from such replies and, optionally, validates it into a
pydantic model.
"""

import json
import re
from typing import Optional, Type, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)

# String-valued keys in our contracts that legitimately hold multi-line text
# (SQL / openCypher). If a model emits raw newlines inside these, the JSON is
# invalid; we collapse the whitespace as a recovery step.
_MULTILINE_KEYS = ("sql", "cypher", "query")


def _slice_object(text: str) -> str:
    """Return the substring from the first ``{`` to the last ``}``.

    Handles a leading ```json fence and surrounding prose. Falls back to the
    original text when no braces are found (json.loads then raises a clear
    error).
    """
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fenced:
        return fenced.group(1)
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return text


def _collapse_multiline_values(json_str: str) -> str:
    """Collapse raw newlines inside known multi-line string values.

    Targets ``"sql"``/``"cypher"``/``"query"`` values whose content spans lines
    with unescaped newlines, replacing runs of whitespace with a single space so
    the result parses as valid JSON.
    """

    def repl(match: re.Match) -> str:
        prefix, content, suffix = match.group(1), match.group(2), match.group(3)
        return prefix + " ".join(content.split()) + suffix

    keys = "|".join(_MULTILINE_KEYS)
    pattern = rf'("(?:{keys})"\s*:\s*")(.*?)("(?=\s*[,}}\]]))'
    return re.sub(pattern, repl, json_str, flags=re.DOTALL)


def extract_json(text: str, model_cls: Optional[Type[T]] = None):
    """Extract a JSON object from ``text``.

    Args:
        text: model reply, possibly wrapped in prose or code fences.
        model_cls: optional pydantic model to validate/coerce the result into.

    Returns:
        The validated ``model_cls`` instance if given, else the parsed ``dict``.

    Raises:
        json.JSONDecodeError: if no JSON object can be recovered.
        pydantic.ValidationError: if ``model_cls`` validation fails.
    """
    json_str = _slice_object(text).strip()
    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError:
        parsed = json.loads(_collapse_multiline_values(json_str))

    if model_cls is not None:
        return model_cls.model_validate(parsed)
    return parsed
