# Copyright 2026 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
"""Regression tests: ``create_iceberg_table_from_table`` must validate each
caller-supplied ``table_columns`` entry AND escape embedded double quotes before
interpolating them into the ``AS SELECT`` list, so a column name cannot
terminate the quoted identifier and rewrite the CREATE TABLE ... AS SELECT
statement (SQL injection).

Two layers, both required:
  * ``_validate_sql_identifier`` rejects names containing ``;`` (and other
    non-identifier shapes) outright.
  * The sink doubles any embedded ``"`` (``c.replace('"', '""')``). This closes
    the residual where a column that is *itself* a valid quoted segment (e.g.
    ``'"x FROM t -- "'``) passes validation but, without escaping, would be
    double-wrapped into a stray leading ``""`` that a parser could treat as an
    identifier breakout. With escaping it becomes one correctly-escaped
    identifier, independent of how Athena/Trino parses ``""``.

A name that merely collides with a SQL keyword (e.g. ``FROM``) is emitted as a
quoted identifier (``"FROM"``) and is inert.
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from nx_neptune.instance_management import create_iceberg_table_from_table


def _run_with_columns(table_columns):
    """Invoke ``create_iceberg_table_from_table`` with AWS dependencies mocked
    and return the SQL string that would have been sent to Athena.

    Patches the pre-validation dependencies and the query executor so no real
    AWS calls happen and the emitted SQL can be captured.
    """
    with (
        patch("nx_neptune.instance_management._create_iam_wrapper") as mock_iam,
        patch(
            "nx_neptune.instance_management._get_bucket_encryption_key_arn"
        ) as mock_key,
        patch("nx_neptune.instance_management._execute_athena_query") as mock_exec,
        patch("nx_neptune.instance_management.TaskFuture") as mock_future,
    ):
        mock_iam.return_value = MagicMock()
        mock_key.return_value = "arn:aws:kms:us-west-1:1:key/abc"
        mock_exec.return_value = "qid-123"

        future_inst = MagicMock()
        future_inst.current_status = "SUCCEEDED"

        async def _noop(*a, **k):
            return None

        future_inst.wait_until_complete.side_effect = _noop
        mock_future.return_value = future_inst

        asyncio.run(
            create_iceberg_table_from_table(
                "s3://out/",
                "iceberg_tbl",
                "csv_tbl",
                table_columns=table_columns,
                athena_client=MagicMock(),
            )
        )
        # _execute_athena_query(client, sql, ...) -> sql is positional arg 1.
        return mock_exec.call_args[0][1]


# --- accepted columns: (columns, expected SELECT list) ---------------------
# Each expected value is exactly what should appear after ``AS SELECT`` and
# before ``FROM csv_tbl`` -- i.e. every column double-quoted, embedded quotes
# doubled.
ACCEPTED_CASES = [
    # legit identifiers
    (["id", "name"], '"id","name"'),
    # keyword-named columns are accepted and rendered as quoted identifiers,
    # so they are inert (a column named FROM, not the FROM clause).
    (["FROM", "UNION"], '"FROM","UNION"'),
    # residual case: a column that is itself a valid quoted segment. Escaping
    # doubles the embedded quotes -> one correctly-escaped identifier
    # ('"""..."""'), NOT the pre-fix double-wrap ('""...""') breakout shape.
    (['"x FROM secrets -- "'], '"""x FROM secrets -- """'),
]


@pytest.mark.parametrize("columns,expected_select", ACCEPTED_CASES)
def test_accepted_columns_render_as_escaped_quoted_identifiers(
    columns, expected_select
):
    """Accepted columns produce a valid, quote-escaped SELECT list and never a
    structure-altering breakout."""
    sql = _run_with_columns(columns)
    assert f"AS SELECT {expected_select} FROM csv_tbl" in sql
    # The pre-fix unescaped sink (plain '","'.join with a single wrap) must not
    # be what we emit when escaping actually changes the output.
    unescaped_old = '"' + '","'.join(columns) + '"'
    if unescaped_old != expected_select:
        assert f"AS SELECT {unescaped_old} FROM csv_tbl" not in sql


# --- rejected columns: injection-capable payloads --------------------------
# Each relies on a character/shape the allowlist forbids (``"`` to break out,
# ``;`` to stack, whitespace/comment syntax, wildcard, or empty).
REJECTED_PAYLOADS = [
    'a" FROM other_db.secrets --',  # quote breakout (the finding's scenario)
    'col"name',  # embedded double quote
    "col; DROP TABLE x",  # semicolon statement stacking
    "col FROM t",  # whitespace + keyword (not an identifier)
    "col--comment",  # SQL comment syntax
    "*",  # wildcard is not a valid identifier here
    "",  # empty string
]


@pytest.mark.parametrize("payload", REJECTED_PAYLOADS)
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_dangerous_columns_rejected(mock_iam_wrapper, mock_key_arn, payload):
    """Every payload carrying injection-capable characters raises ValueError
    before Athena is ever called."""
    mock_key_arn.return_value = "arn:aws:kms:us-west-1:1:key/abc"
    mock_iam_wrapper.return_value = MagicMock()

    athena = MagicMock()
    with pytest.raises(ValueError):
        asyncio.run(
            create_iceberg_table_from_table(
                "s3://out/",
                "iceberg_tbl",
                "csv_tbl",
                table_columns=[payload],
                athena_client=athena,
            )
        )
    athena.start_query_execution.assert_not_called()
