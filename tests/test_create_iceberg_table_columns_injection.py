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
"""Regression tests: ``create_iceberg_table_from_table`` must validate and
escape the caller-supplied ``table_columns`` before interpolating them into the
``AS SELECT`` list, so a column name cannot terminate the quoted identifier and
rewrite the CREATE TABLE ... AS SELECT statement (SQL injection).
"""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from nx_neptune.instance_management import create_iceberg_table_from_table

# A column name that closes the double-quoted identifier and appends a
# FROM-clause breakout, per the finding's exploit scenario.
BREAKOUT_PAYLOAD = 'a" FROM other_db.secrets --'
QUOTE_PAYLOAD = 'col"name'


def _patched(monkeypatch_targets):
    """Patch the two dependencies that run BEFORE column validation, so the
    function reaches the validation step without making real AWS calls."""
    return monkeypatch_targets


@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_breakout_column_rejected(mock_iam_wrapper, mock_key_arn):
    """A FROM-clause breakout column must raise ValueError, before any query."""
    mock_key_arn.return_value = "arn:aws:kms:us-west-1:1:key/abc"
    mock_iam_wrapper.return_value = MagicMock()

    athena = MagicMock()
    with pytest.raises(ValueError):
        asyncio.run(
            create_iceberg_table_from_table(
                "s3://out/",
                "iceberg_tbl",
                "csv_tbl",
                table_columns=[BREAKOUT_PAYLOAD],
                athena_client=athena,
            )
        )
    # The injection must be caught before Athena is ever called.
    athena.start_query_execution.assert_not_called()


@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_embedded_quote_column_rejected(mock_iam_wrapper, mock_key_arn):
    """A column name merely containing a double quote is rejected too."""
    mock_key_arn.return_value = "arn:aws:kms:us-west-1:1:key/abc"
    mock_iam_wrapper.return_value = MagicMock()

    athena = MagicMock()
    with pytest.raises(ValueError):
        asyncio.run(
            create_iceberg_table_from_table(
                "s3://out/",
                "iceberg_tbl",
                "csv_tbl",
                table_columns=[QUOTE_PAYLOAD],
                athena_client=athena,
            )
        )
    athena.start_query_execution.assert_not_called()


@patch("nx_neptune.instance_management.TaskFuture")
@patch("nx_neptune.instance_management._execute_athena_query")
@patch("nx_neptune.instance_management._get_bucket_encryption_key_arn")
@patch("nx_neptune.instance_management._create_iam_wrapper")
def test_legit_columns_are_quoted(
    mock_iam_wrapper, mock_key_arn, mock_exec, mock_future
):
    """Legitimate column names still produce a valid, double-quoted SELECT list."""
    mock_key_arn.return_value = "arn:aws:kms:us-west-1:1:key/abc"
    mock_iam_wrapper.return_value = MagicMock()
    mock_exec.return_value = "qid-123"

    # TaskFuture(...).wait_until_complete() -> awaited; current_status SUCCEEDED
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
            table_columns=["id", "name"],
            athena_client=MagicMock(),
        )
    )

    # Capture the SQL that would have been sent to Athena.
    sql = mock_exec.call_args[0][1]
    assert 'AS SELECT "id","name" FROM csv_tbl' in sql
