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
"""Tests that generated Athena SQL/DDL validates every caller-supplied
identifier and literal argument, rejecting values that could break out of the
surrounding SQL context.
"""

import pytest

from nx_neptune.utils.utils import (
    generate_create_table_ddl,
    generate_projection_stmt,
)

# The 3-part quoted, aliased table reference used by the S3-vector demo.
VECTOR_TABLE_REF = '"lambda:my-connector"."my-bucket"."my-index" v'


class TestDdlLegitimate:
    """Legitimate inputs from the import demos still produce DDL."""

    def test_basic_ddl(self):
        ddl = generate_create_table_ddl(
            "my_table",
            "s3://my-bucket/path/",
            [("id", "string"), ("year", "int"), ("embedding", "array<float>")],
        )
        assert "CREATE EXTERNAL TABLE" in ddl
        assert "`id` string" in ddl
        assert "LOCATION 's3://my-bucket/path/'" in ddl

    def test_complex_types(self):
        ddl = generate_create_table_ddl(
            "t",
            "s3://my-bucket/p/",
            [("a", "decimal(10,2)"), ("b", "map<string,int>"), ("c", "array<real>")],
        )
        assert "`a` decimal(10,2)" in ddl


class TestDdlRejects:
    def test_s3_location_quote_breakout(self):
        with pytest.raises(ValueError):
            generate_create_table_ddl(
                "t", "s3://b-ucket/p/' ; DROP TABLE x --", [("id", "string")]
            )

    def test_s3_location_not_s3_uri(self):
        with pytest.raises(ValueError):
            generate_create_table_ddl("t", "/local/path", [("id", "string")])

    def test_column_type_statement_injection(self):
        with pytest.raises(ValueError):
            generate_create_table_ddl(
                "t", "s3://my-bucket/p/", [("id", "string); DROP TABLE x --")]
            )

    def test_column_type_not_allowlisted(self):
        with pytest.raises(ValueError):
            generate_create_table_ddl("t", "s3://my-bucket/p/", [("id", "evil")])

    def test_column_name_injection(self):
        with pytest.raises(ValueError):
            generate_create_table_ddl(
                "t", "s3://my-bucket/p/", [("id`; DROP --", "string")]
            )

    def test_table_name_injection(self):
        with pytest.raises(ValueError):
            generate_create_table_ddl(
                "t; DROP TABLE x --", "s3://my-bucket/p/", [("id", "string")]
            )


class TestProjectionLegitimate:
    def test_simple_projection(self):
        stmt = generate_projection_stmt("id", "users", columns=["name", "age"])
        assert 'id AS "~id"' in stmt
        assert 'name AS "name"' in stmt

    def test_projection_with_join(self):
        stmt = generate_projection_stmt(
            col_id="t.id",
            col_label="t.masterCategory",
            col_embedding="v.embedding",
            columns=["t.gender", "t.subCategory"],
            base_table="test_embedding_table as t",
            joins=[(VECTOR_TABLE_REF, "t.id = v.vector_id")],
        )
        assert 't.id AS "~id"' in stmt
        assert "join" in stmt
        assert "on t.id = v.vector_id" in stmt

    def test_projection_with_udf(self):
        stmt = generate_projection_stmt(
            col_id="t.id",
            col_label="t.masterCategory",
            col_vector_id="t.id",
            columns=["t.gender"],
            base_table="test_embedding_table as t",
            connector_name="my-connector",
            vector_bucket="my-bucket",
            vector_index="my-index",
        )
        assert "LAMBDA 'my-connector'" in stmt
        assert "get_embedding('my-bucket', 'my-index', t.id)" in stmt

    def test_join_condition_with_and(self):
        stmt = generate_projection_stmt(
            "t.id",
            "users t",
            joins=[("posts p", "t.id = p.user_id AND t.org = p.org")],
        )
        assert "AND" in stmt


class TestProjectionRejects:
    def test_col_id_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt('id"; DROP --', "users")

    def test_col_label_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt("id", "users", col_label="x, (SELECT 1)")

    def test_col_embedding_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt("id", "users", col_embedding="x); DROP --")

    def test_column_entry_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt("id", "users", columns=["ok", "b); DROP --"])

    def test_base_table_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt("id", "users; DROP TABLE x --")

    def test_join_table_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt(
                "t.id", "users t", joins=[("p; DROP --", "t.id = p.id")]
            )

    def test_join_condition_or_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt(
                "t.id", "users t", joins=[("p", "t.id = p.id OR 1=1; DROP --")]
            )

    def test_join_condition_non_equality(self):
        with pytest.raises(ValueError):
            generate_projection_stmt("t.id", "users t", joins=[("p", "t.id > p.id")])

    def test_connector_name_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt("t.id", "users t", connector_name="c'; DROP --")

    def test_vector_bucket_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt(
                "t.id",
                "users t",
                col_vector_id="t.id",
                vector_bucket="b'--",
                vector_index="i",
            )

    def test_vector_index_injection(self):
        with pytest.raises(ValueError):
            generate_projection_stmt(
                "t.id",
                "users t",
                col_vector_id="t.id",
                vector_bucket="b",
                vector_index="i'--",
            )
