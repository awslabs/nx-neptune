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
"""Tests that _build_sql_statement validates the column names and datatypes
parsed from the S3 CSV header before interpolating them into the Athena
CREATE EXTERNAL TABLE statement.
"""

from unittest.mock import MagicMock

import pytest

from nx_neptune.instance_management import _build_sql_statement

FILE_PATHS = [{"Key": "export/prefix_0.csv"}]


def _s3_with_header(header: str) -> MagicMock:
    """A mock S3 client whose single object returns *header* as its first line."""
    s3 = MagicMock()
    body = MagicMock()
    body.readline.return_value = header.encode("utf-8")
    s3.get_object.return_value = {"Body": body}
    return s3


def _build(header: str, table_name: str = "my_table") -> str:
    return _build_sql_statement(
        _s3_with_header(header),
        "my-bucket",
        "",
        "prefix",
        FILE_PATHS,
        {},
        table_name,
    )


class TestBuildSqlStatementLegitimate:
    def test_neptune_export_header(self):
        ddl = _build(
            "~id,name:String,age:Int,score:Double,active:Bool,cnt:Long,ts:Date"
        )
        assert "CREATE EXTERNAL TABLE IF NOT EXISTS my_table" in ddl
        assert "`name` string" in ddl
        assert "`age` int" in ddl
        assert "`cnt` bigint" in ddl  # Long is mapped to bigint
        assert "LOCATION 's3://my-bucket/prefix'" in ddl

    def test_untyped_field_defaults_to_string(self):
        ddl = _build("~id,plainfield")
        assert "`plainfield` string" in ddl

    def test_vector_field_skipped(self):
        ddl = _build("~id,emb:Vector,name:String")
        assert "emb" not in ddl
        assert "`name` string" in ddl


class TestBuildSqlStatementRejects:
    def test_datatype_ddl_breakout(self):
        with pytest.raises(ValueError):
            _build("~id,evil:string) LOCATION 's3://attacker/' --")

    def test_backtick_in_field_name(self):
        with pytest.raises(ValueError):
            _build("~id,ev`il:string")

    def test_semicolon_in_quoted_field_name(self):
        with pytest.raises(ValueError):
            _build('ok,"ev;il":string')

    def test_unknown_datatype(self):
        with pytest.raises(ValueError):
            _build("~id,x:notatype")

    def test_datatype_with_space_injection(self):
        with pytest.raises(ValueError):
            _build("~id,x:string LOCATION")

    def test_table_name_injection(self):
        with pytest.raises(ValueError):
            _build("~id,name:String", table_name="t; DROP TABLE x --")

    def test_complex_type_rejected(self):
        # Complex/parameterized types are not produced by the export path and
        # are rejected outright (previously mis-parsed on struct field names).
        with pytest.raises(ValueError):
            _build("~id,x:struct<name:string>")

    def test_malformed_type_rejected(self):
        # A datatype carrying stray SQL punctuation must not slip through.
        with pytest.raises(ValueError):
            _build("~id,x:int)")


class TestBuildSqlStatementScalarTypes:
    def test_all_export_scalar_types_accepted(self):
        # The scalar type names emitted in Neptune CSV export headers all pass.
        ddl = _build(
            "~id,a:String,b:Int,c:Long,d:Double,e:Float,f:Bool,g:Byte,h:Short,i:Date"
        )
        assert "`a` string" in ddl
        assert "`c` bigint" in ddl  # Long -> bigint
        assert "`f` bool" in ddl
        assert "`g` byte" in ddl
        assert "`h` short" in ddl
