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
import os
import tempfile
from unittest.mock import patch

import pytest

from nx_neptune.utils.utils import (
    validate_and_get_env,
    read_csv,
)


class TestValidateAndGetEnv:
    """Tests for validate_and_get_env function"""

    @pytest.mark.parametrize(
        "env_vars,env_values",
        [
            (["VAR1"], {"VAR1": "value1"}),
            (["VAR1", "VAR2"], {"VAR1": "value1", "VAR2": "value2"}),
            (
                ["HOME", "USER", "PATH"],
                {"HOME": "/home/test", "USER": "testuser", "PATH": "/usr/bin"},
            ),
        ],
    )
    def test_all_vars_present(self, env_vars, env_values, capsys):
        """Test when all environment variables are present"""
        with patch.dict(os.environ, env_values, clear=True):
            result = validate_and_get_env(env_vars)
            assert result == env_values
            captured = capsys.readouterr()
            for var in env_vars:
                assert f"Using {var}: {env_values[var]}" in captured.out

    @pytest.mark.parametrize(
        "missing_vars",
        [
            (["MISSING_VAR"]),
            (["VAR1", "VAR2"]),
            (["AWS_REGION", "AWS_ACCESS_KEY"]),
        ],
    )
    def test_missing_vars(self, missing_vars, capsys):
        """Test when environment variables are missing"""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError) as exc_info:
                validate_and_get_env(missing_vars)
            assert "Required environment variables missing" in str(exc_info.value)
            captured = capsys.readouterr()
            for var in missing_vars:
                assert (
                    f"Warning: Environment Variable {var} is not defined"
                    in captured.out
                )

    @pytest.mark.parametrize(
        "present_vars,missing_vars",
        [
            ({"VAR1": "value1"}, ["VAR2"]),
            ({"HOME": "/home/test"}, ["MISSING_VAR"]),
            ({"VAR1": "v1", "VAR2": "v2"}, ["VAR3", "VAR4"]),
        ],
    )
    def test_mixed_vars(self, present_vars, missing_vars, capsys):
        """Test with mix of present and missing variables"""
        all_vars = list(present_vars.keys()) + missing_vars
        with patch.dict(os.environ, present_vars, clear=True):
            with pytest.raises(ValueError) as exc_info:
                validate_and_get_env(all_vars)
            assert "Required environment variables missing" in str(exc_info.value)


class TestReadCsv:
    """Tests for read_csv function"""

    @pytest.mark.parametrize(
        "csv_content,expected_header,expected_rows",
        [
            (
                "col1,col2,col3\nval1,val2,val3\n",
                ["col1", "col2", "col3"],
                [{"col1": "val1", "col2": "val2", "col3": "val3"}],
            ),
            (
                "id,name,age\n1,Alice,30\n2,Bob,25\n",
                ["id", "name", "age"],
                [
                    {"id": "1", "name": "Alice", "age": "30"},
                    {"id": "2", "name": "Bob", "age": "25"},
                ],
            ),
            (
                "a,b\nx,y\n",
                ["a", "b"],
                [{"a": "x", "b": "y"}],
            ),
        ],
    )
    def test_read_csv_full(self, csv_content, expected_header, expected_rows):
        """Test reading full CSV file"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(csv_content)
            f.flush()
            try:
                header, rows = read_csv(f.name)
                assert header == expected_header
                assert rows == expected_rows
            finally:
                os.unlink(f.name)

    @pytest.mark.parametrize(
        "csv_content,limit,expected_row_count",
        [
            ("col1,col2\nval1,val2\nval3,val4\nval5,val6\n", 1, 1),
            ("col1,col2\nval1,val2\nval3,val4\nval5,val6\n", 2, 2),
            ("col1,col2\nval1,val2\nval3,val4\nval5,val6\n", 10, 3),
        ],
    )
    def test_read_csv_with_limit(self, csv_content, limit, expected_row_count):
        """Test reading CSV with row limit"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write(csv_content)
            f.flush()
            try:
                header, rows = read_csv(f.name, limit=limit)
                assert len(rows) == expected_row_count
            finally:
                os.unlink(f.name)

    def test_read_csv_empty(self):
        """Test reading empty CSV"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as f:
            f.write("col1,col2\n")
            f.flush()
            try:
                header, rows = read_csv(f.name)
                assert header == ["col1", "col2"]
                assert rows == []
            finally:
                os.unlink(f.name)


class TestValidateSqlIdentifier:
    """Tests for _validate_sql_identifier function"""

    @pytest.mark.parametrize(
        "value",
        [
            '"lambda:db-test"."default"."paysim_transactions"',
            '"my-catalog".my_db.my_table',
            "my_table",
            "Table1",
            "_private",
            "catalog.database.table",
            "db.my_table",
        ],
    )
    def test_valid_identifiers(self, value):
        from nx_neptune.utils.utils import _validate_sql_identifier

        assert _validate_sql_identifier(value) == value

    @pytest.mark.parametrize(
        "value",
        [
            "table; DROP TABLE users--",
            "table name",
            "1table",
            "",
            "table\n",
            "table'",
            "table;",
            ".leading_dot",
            "trailing.",
            '"table;DROP TABLE x"',
        ],
    )
    def test_invalid_identifiers(self, value):
        from nx_neptune.utils.utils import _validate_sql_identifier

        with pytest.raises(ValueError, match="Invalid SQL"):
            _validate_sql_identifier(value)
