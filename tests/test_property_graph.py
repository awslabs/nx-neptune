# Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

from unittest.mock import AsyncMock, patch

import pytest
from botocore.exceptions import ClientError

from nx_neptune.property_graph import (
    AthenaTableMetadata,
    PropertyGraphSchemaError,
    PropertyGraphSyntaxError,
    StaticTableMetadata,
    parse_property_graph,
    property_graph_to_sql,
)
from nx_neptune.session_manager import SessionManager

# Catalog column names are lower-case, as Glue stores them.
PAYSIM_TABLES = {
    "accounts": [("name", "varchar(64)"), ("region", "string")],
    "transactions": [
        ("nameorig", "string"),
        ("namedest", "string"),
        ("step", "int"),
        ("amount", "double"),
        ("isfraud", "int"),
    ],
}

PAYSIM_DDL = """
CREATE PROPERTY GRAPH financial
  VERTEX TABLES (
    accounts AS customer
      KEY (name)
      LABEL customer
      PROPERTIES (name)
  )
  EDGE TABLES (
    transactions
      SOURCE KEY (nameOrig) REFERENCES customer
      DESTINATION KEY (nameDest) REFERENCES customer
      LABEL transfer
      PROPERTIES (step, amount, isFraud)
  )
"""


def to_sql(ddl, tables):
    return property_graph_to_sql(ddl, StaticTableMetadata(tables))


def single_vertex(properties, columns, key="id"):
    """SQL for one vertex table ``t`` with the given PROPERTIES clause and columns."""
    ddl = f"CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY ({key}) {properties} )"
    return to_sql(ddl, {"t": columns})[0]


# --------------------------------------------------------------------------- #
# Translation
# --------------------------------------------------------------------------- #
def test_paysim_generates_typed_vertex_and_edge_queries():
    vertex, edge = to_sql(PAYSIM_DDL, PAYSIM_TABLES)

    assert vertex == (
        'SELECT DISTINCT "name" AS "~id", \'customer\' AS "~label", '
        '"name" AS "name" FROM "accounts" WHERE "name" IS NOT NULL'
    )
    # Column references use the catalog's spelling; property names keep the DDL's.
    # Each endpoint joins the referenced vertex table's keys.
    assert edge == (
        'SELECT "nameorig" AS "~from", "namedest" AS "~to", '
        '\'transfer\' AS "~label", "step" AS "step:Int", '
        '"amount" AS "amount:Double", "isfraud" AS "isFraud:Int" '
        'FROM "transactions" '
        'JOIN (SELECT DISTINCT "name" AS "~key" FROM "accounts") "~source" '
        'ON "nameorig" = "~source"."~key" '
        'JOIN (SELECT DISTINCT "name" AS "~key" FROM "accounts") "~destination" '
        'ON "namedest" = "~destination"."~key"'
    )


def test_edge_endpoints_join_their_own_vertex_tables():
    edge = to_sql(
        """
        CREATE PROPERTY GRAPH g
          VERTEX TABLES (
            lake.accounts AS customer KEY (id),
            lake.accounts AS merchant KEY (id)
          )
          EDGE TABLES (
            payments
              SOURCE KEY (payer) REFERENCES customer (id)
              DESTINATION KEY (payee) REFERENCES merchant
              NO PROPERTIES
          )
        """,
        {
            "lake.accounts": [("id", "bigint")],
            "payments": [("payer", "bigint"), ("payee", "string")],
        },
    )[2]
    assert (
        'JOIN (SELECT DISTINCT "id" AS "~key" FROM "lake"."accounts") "~source" '
        'ON "payer" = "~source"."~key"'
    ) in edge
    # Neptune ids are strings, so mismatched key types are compared as strings.
    assert (
        'ON CAST("payee" AS VARCHAR) = CAST("~destination"."~key" AS VARCHAR)'
    ) in edge


def test_omitted_properties_means_all_columns():
    sql = single_vertex("", [("id", "bigint"), ("score", "double")])
    assert '"id" AS "id:Long"' in sql
    assert '"score" AS "score:Double"' in sql


@pytest.mark.parametrize(
    "clause", ["PROPERTIES ALL COLUMNS", "PROPERTIES ARE ALL COLUMNS"]
)
def test_all_columns_spellings(clause):
    sql = single_vertex(clause, [("id", "bigint"), ("score", "double")])
    assert '"score" AS "score:Double"' in sql


def test_all_columns_except():
    sql = single_vertex(
        "PROPERTIES ALL COLUMNS EXCEPT (tags)",
        [("id", "bigint"), ("tags", "array<string>"), ("name", "string")],
    )
    assert "tags" not in sql
    assert '"name" AS "name"' in sql


def test_no_properties():
    sql = single_vertex("NO PROPERTIES", [("id", "bigint"), ("name", "string")])
    assert sql == (
        'SELECT DISTINCT "id" AS "~id", \'t\' AS "~label" FROM "t" '
        'WHERE "id" IS NOT NULL'
    )


@pytest.mark.parametrize(
    "sql_type, header",
    [
        ("boolean", "flag:Bool"),
        ("tinyint", "flag:Byte"),
        ("smallint", "flag:Short"),
        ("integer", "flag:Int"),
        ("bigint", "flag:Long"),
        ("real", "flag:Float"),
        ("decimal(10,2)", "flag:Double"),
        ("varchar(20)", "flag"),
        ("char(1)", "flag"),
        ("date", "flag:Date"),
    ],
)
def test_catalog_types_map_to_neptune_headers(sql_type, header):
    sql = single_vertex("PROPERTIES (flag)", [("id", "bigint"), ("flag", sql_type)])
    assert f'"flag" AS "{header}"' in sql


def test_timestamp_is_loaded_as_iso8601_datetime():
    sql = single_vertex("PROPERTIES (ts)", [("id", "bigint"), ("ts", "timestamp")])
    assert 'to_iso8601("ts") AS "ts:Datetime"' in sql


def test_cast_sets_the_type():
    sql = single_vertex(
        "PROPERTIES (CAST(amount AS DECIMAL(12,2)) AS amt, CAST(ts AS TIMESTAMP))",
        [("id", "bigint"), ("amount", "varchar"), ("ts", "varchar")],
    )
    assert 'CAST("amount" AS DECIMAL(12,2)) AS "amt:Double"' in sql
    assert 'to_iso8601(CAST("ts" AS TIMESTAMP)) AS "ts:Datetime"' in sql


def test_property_rename():
    sql = single_vertex(
        "PROPERTIES (raw_name AS name)", [("id", "bigint"), ("raw_name", "string")]
    )
    assert '"raw_name" AS "name"' in sql


def test_label_defaults_to_alias_then_table_name():
    graph = parse_property_graph("""
        CREATE PROPERTY GRAPH g
          VERTEX TABLES ( people AS person KEY (id), lake.places KEY (id) )
          EDGE TABLES (
            knows SOURCE KEY (a) REFERENCES person
                  DESTINATION KEY (b) REFERENCES places
          )
        """)
    assert graph.vertex_tables[0].label == "person"  # alias wins
    assert graph.vertex_tables[1].label == "places"  # table name, not "lake.places"
    assert graph.edge_tables[0].label == "knows"


def test_qualified_table_reference_quotes_each_segment():
    sql = to_sql(
        'CREATE PROPERTY GRAPH g VERTEX TABLES ( "my-catalog".lake.accounts KEY (id) )',
        {"my-catalog.lake.accounts": [("id", "bigint")]},
    )[0]
    assert 'FROM "my-catalog"."lake"."accounts"' in sql


def test_source_destination_key_keyword_optional():
    queries = to_sql(
        """
        CREATE PROPERTY GRAPH g
          VERTEX TABLES ( v KEY (id) NO PROPERTIES )
          EDGE TABLES (
            e SOURCE (src) REFERENCES v DESTINATION (dst) REFERENCES v NO PROPERTIES
          )
        """,
        {"v": [("id", "bigint")], "e": [("src", "bigint"), ("dst", "bigint")]},
    )
    assert '"src" AS "~from"' in queries[1]
    assert '"dst" AS "~to"' in queries[1]


def test_label_with_quote_is_escaped():
    sql = single_vertex('LABEL "O\'Brien" NO PROPERTIES', [("id", "bigint")])
    assert "'O''Brien' AS \"~label\"" in sql


def test_quoted_identifier_with_embedded_quote():
    sql = single_vertex("NO PROPERTIES", [('wei"rd', "bigint")], key='"wei""rd"')
    assert '"wei""rd" AS "~id"' in sql


def test_comments_are_ignored():
    graph = parse_property_graph("""
        -- a line comment
        CREATE PROPERTY GRAPH g /* block */ VERTEX TABLES ( t KEY (id) )
        """)
    assert len(graph.vertex_tables) == 1


# --------------------------------------------------------------------------- #
# Type conflicts across element tables sharing a label
# --------------------------------------------------------------------------- #
CONFLICT_DDL = """
CREATE PROPERTY GRAPH g
  VERTEX TABLES (
    wires KEY (id) LABEL txn PROPERTIES (amount),
    cards KEY (id) LABEL txn {cards_properties}
  )
"""
CONFLICT_TABLES = {
    "wires": [("id", "string"), ("amount", "double")],
    "cards": [("id", "string"), ("amount", "bigint"), ("note", "string")],
}


def test_conflicting_types_fail_with_paste_ready_cast():
    ddl = CONFLICT_DDL.format(cards_properties="PROPERTIES (amount)")
    with pytest.raises(PropertyGraphSchemaError) as exc:
        to_sql(ddl, CONFLICT_TABLES)
    message = str(exc.value)
    assert "Property 'amount' of vertex label 'txn' has conflicting types" in message
    assert "Double in 'wires'" in message and "Long in 'cards'" in message
    assert "in 'cards': PROPERTIES (CAST(amount AS DOUBLE) AS amount)" in message


def test_conflict_under_all_columns_suggests_explicit_list():
    ddl = CONFLICT_DDL.format(cards_properties="")
    with pytest.raises(PropertyGraphSchemaError) as exc:
        to_sql(ddl, CONFLICT_TABLES)
    assert "in 'cards': PROPERTIES (id, CAST(amount AS DOUBLE) AS amount, note)" in str(
        exc.value
    )


def test_integer_conflicts_widen_to_the_larger_integer():
    ddl = CONFLICT_DDL.format(cards_properties="PROPERTIES (amount)")
    tables = {**CONFLICT_TABLES, "wires": [("id", "string"), ("amount", "int")]}
    with pytest.raises(PropertyGraphSchemaError) as exc:
        to_sql(ddl, tables)
    assert "in 'wires': PROPERTIES (CAST(amount AS BIGINT) AS amount)" in str(exc.value)


def test_cast_resolves_the_conflict():
    ddl = CONFLICT_DDL.format(
        cards_properties="PROPERTIES (CAST(amount AS DOUBLE) AS amount)"
    )
    queries = to_sql(ddl, CONFLICT_TABLES)
    assert 'CAST("amount" AS DOUBLE) AS "amount:Double"' in queries[1]


def test_same_property_under_different_labels_may_differ():
    ddl = CONFLICT_DDL.format(cards_properties="PROPERTIES (amount)").replace(
        "LABEL txn PROPERTIES (amount),", "LABEL wire PROPERTIES (amount),"
    )
    assert len(to_sql(ddl, CONFLICT_TABLES)) == 2


# --------------------------------------------------------------------------- #
# Schema errors
# --------------------------------------------------------------------------- #
def test_missing_column_suggests_close_match():
    with pytest.raises(PropertyGraphSchemaError, match="Did you mean 'amount'"):
        single_vertex("PROPERTIES (amout)", [("id", "bigint"), ("amount", "double")])


def test_missing_key_column():
    with pytest.raises(PropertyGraphSchemaError, match="KEY column 'id' not found"):
        single_vertex("", [("ident", "bigint")])


def test_unsupported_type_under_all_columns_suggests_except():
    with pytest.raises(PropertyGraphSchemaError, match=r"ALL COLUMNS EXCEPT \(tags\)"):
        single_vertex("", [("id", "bigint"), ("tags", "array<string>")])


def test_unsupported_type_in_explicit_list():
    with pytest.raises(PropertyGraphSchemaError, match="remove it from the PROPERTIES"):
        single_vertex(
            "PROPERTIES (tags)", [("id", "bigint"), ("tags", "map<string,int>")]
        )


def test_unsupported_cast_target():
    with pytest.raises(PropertyGraphSchemaError, match="Supported CAST targets"):
        single_vertex(
            "PROPERTIES (CAST(x AS VARBINARY) AS x)",
            [("id", "bigint"), ("x", "string")],
        )


def test_invalid_property_name_suggests_rename():
    with pytest.raises(PropertyGraphSchemaError, match='"my col" AS my_col'):
        single_vertex("", [("id", "bigint"), ("my col", "string")])


def test_duplicate_property_name():
    with pytest.raises(PropertyGraphSchemaError, match="defined twice"):
        single_vertex(
            "PROPERTIES (a AS x, b AS x)",
            [("id", "bigint"), ("a", "string"), ("b", "string")],
        )


def test_unknown_table():
    with pytest.raises(PropertyGraphSchemaError, match="Table 'nope' not found"):
        to_sql("CREATE PROPERTY GRAPH g VERTEX TABLES ( nope KEY (id) )", {})


# --------------------------------------------------------------------------- #
# Syntax errors (no catalog needed)
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "vertex",
    ["accounts", "accounts AS acct LABEL account", "accounts PROPERTIES (name)"],
)
def test_vertex_key_is_required(vertex):
    with pytest.raises(PropertyGraphSyntaxError, match=r"has no KEY.*not read from"):
        parse_property_graph(f"CREATE PROPERTY GRAPH g VERTEX TABLES ( {vertex} )")


@pytest.mark.parametrize(
    "endpoints, missing",
    [
        ("DESTINATION KEY (b) REFERENCES v", "SOURCE"),
        ("SOURCE KEY (a) REFERENCES v", "DESTINATION"),
        ("LABEL e", "SOURCE"),
        ("", "SOURCE"),
    ],
)
def test_edge_source_and_destination_are_required(endpoints, missing):
    with pytest.raises(
        PropertyGraphSyntaxError, match=rf"'e' has no {missing}.*not read from"
    ):
        parse_property_graph(f"""
            CREATE PROPERTY GRAPH g
              VERTEX TABLES ( v KEY (id) )
              EDGE TABLES ( e {endpoints} )
            """)


def test_edge_endpoint_requires_references():
    with pytest.raises(PropertyGraphSyntaxError, match="REFERENCES"):
        parse_property_graph("""
            CREATE PROPERTY GRAPH g
              VERTEX TABLES ( v KEY (id) )
              EDGE TABLES ( e SOURCE KEY (a) DESTINATION KEY (b) REFERENCES v )
            """)


def test_composite_key_rejected():
    with pytest.raises(PropertyGraphSyntaxError, match="Composite keys"):
        parse_property_graph("CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY (a, b) )")


def test_dangling_reference_rejected():
    with pytest.raises(PropertyGraphSyntaxError, match="REFERENCES 'ghost'"):
        parse_property_graph("""
            CREATE PROPERTY GRAPH g
              VERTEX TABLES ( v KEY (id) )
              EDGE TABLES (
                e SOURCE KEY (a) REFERENCES ghost
                  DESTINATION KEY (b) REFERENCES v
              )
            """)


def test_reference_columns_must_match_vertex_key():
    with pytest.raises(PropertyGraphSyntaxError, match=r"has KEY \(id\)"):
        parse_property_graph("""
            CREATE PROPERTY GRAPH g
              VERTEX TABLES ( v KEY (id) )
              EDGE TABLES (
                e SOURCE KEY (a) REFERENCES v (other)
                  DESTINATION KEY (b) REFERENCES v (id)
              )
            """)


@pytest.mark.parametrize("ref", ["accounts", "person"])
def test_aliased_vertex_table_is_referenced_by_alias(ref):
    # SQL:2023 references the element table name (its alias), not the
    # underlying table or the label.
    with pytest.raises(
        PropertyGraphSyntaxError, match=r"REFERENCES 'accounts'|REFERENCES 'person'"
    ) as err:
        parse_property_graph(f"""
            CREATE PROPERTY GRAPH g
              VERTEX TABLES ( accounts AS customer KEY (id) LABEL person )
              EDGE TABLES (
                e SOURCE KEY (a) REFERENCES {ref}
                  DESTINATION KEY (b) REFERENCES customer
              )
            """)
    assert "Did you mean REFERENCES customer?" in str(err.value)


def test_element_table_names_must_be_unique():
    with pytest.raises(PropertyGraphSyntaxError, match="'accounts' is used more"):
        parse_property_graph(
            "CREATE PROPERTY GRAPH g VERTEX TABLES ( accounts KEY (id), accounts KEY (id) )"
        )


def test_reference_by_table_name_when_no_alias():
    graph = parse_property_graph("""
        CREATE PROPERTY GRAPH g
          VERTEX TABLES ( accounts KEY (id) )
          EDGE TABLES (
            e SOURCE KEY (a) REFERENCES accounts (id)
              DESTINATION KEY (b) REFERENCES accounts
          )
        """)
    assert graph.edge_tables[0].source_ref == "accounts"


def test_type_suffix_is_no_longer_accepted():
    with pytest.raises(PropertyGraphSyntaxError):
        parse_property_graph(
            "CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY (id) PROPERTIES (x:Int) )"
        )


def test_missing_vertex_tables_rejected():
    with pytest.raises(PropertyGraphSyntaxError):
        parse_property_graph("CREATE PROPERTY GRAPH g EDGE TABLES ( e )")


def test_garbage_input_rejected():
    with pytest.raises(PropertyGraphSyntaxError):
        parse_property_graph("this is not a property graph")


def test_syntax_error_shows_position_expectation_and_line():
    with pytest.raises(PropertyGraphSyntaxError) as err:
        parse_property_graph(
            "CREATE PROPERTY GRAPH g\n"
            "  VERTEX TABLES ( v KEY (id) )\n"
            "  EDGE TABLE ( e SOURCE KEY (a) REFERENCES v DESTINATION KEY (b) REFERENCES v )"
        )
    message = str(err.value)
    assert message.startswith(
        "Syntax error at line 3, column 8: unexpected 'TABLE'. Expected one of: TABLES."
    )
    assert message.splitlines()[1:] == [
        "    EDGE TABLE ( e SOURCE KEY (a) REFERENCES v DE",
        "         ^",
    ]


def test_syntax_error_names_punctuation_and_end_of_input():
    with pytest.raises(PropertyGraphSyntaxError) as err:
        parse_property_graph("CREATE PROPERTY GRAPH g VERTEX TABLES ( v KEY (id)")
    assert "unexpected end of input. Expected one of: ',', LABEL, NO" in str(err.value)
    assert "CNAME" not in str(err.value)


def test_trailing_semicolon_accepted():
    graph = parse_property_graph(
        "CREATE PROPERTY GRAPH g VERTEX TABLES ( v KEY (id) );"
    )
    assert graph.vertex_tables[0].key == "id"


# --------------------------------------------------------------------------- #
# AthenaTableMetadata
# --------------------------------------------------------------------------- #
class FakeAthena:
    def __init__(self, tables):
        self.tables = (
            tables  # {(catalog, database, table): get_table_metadata response}
        )
        self.calls = []

    def get_table_metadata(self, CatalogName, DatabaseName, TableName):
        self.calls.append((CatalogName, DatabaseName, TableName))
        key = (CatalogName, DatabaseName, TableName)
        if key not in self.tables:
            raise ClientError(
                {"Error": {"Code": "MetadataException", "Message": "no such table"}},
                "GetTableMetadata",
            )
        return self.tables[key]


def table_metadata(columns, partition_keys=()):
    return {
        "TableMetadata": {
            "Columns": [{"Name": n, "Type": t} for n, t in columns],
            "PartitionKeys": [{"Name": n, "Type": t} for n, t in partition_keys],
        }
    }


def test_athena_metadata_qualifies_names_and_includes_partition_keys():
    athena = FakeAthena(
        {
            ("AwsDataCatalog", "lake", "events"): table_metadata(
                [("id", "bigint")], partition_keys=[("day", "date")]
            ),
            ("AwsDataCatalog", "other", "t"): table_metadata([("id", "bigint")]),
            ("cat", "db", "t"): table_metadata([("id", "bigint")]),
        }
    )
    metadata = AthenaTableMetadata(athena, database="lake")

    events = metadata.get_columns(("events",))
    assert [(c.name, c.type) for c in events] == [("id", "bigint"), ("day", "date")]
    metadata.get_columns(("other", "t"))
    metadata.get_columns(("cat", "db", "t"))

    assert athena.calls == [
        ("AwsDataCatalog", "lake", "events"),
        ("AwsDataCatalog", "other", "t"),
        ("cat", "db", "t"),
    ]


def test_each_source_table_is_read_once_per_translation():
    athena = FakeAthena(
        {
            ("AwsDataCatalog", "db", "transactions"): table_metadata(
                [("nameorig", "string"), ("namedest", "string")]
            )
        }
    )
    ddl = """
        CREATE PROPERTY GRAPH g
          VERTEX TABLES (
            transactions AS sender KEY (nameOrig) NO PROPERTIES,
            transactions AS recipient KEY (nameDest) NO PROPERTIES
          )
          EDGE TABLES (
            Transactions
              SOURCE KEY (nameOrig) REFERENCES sender
              DESTINATION KEY (nameDest) REFERENCES recipient
          )
        """
    metadata = AthenaTableMetadata(athena, database="db")
    property_graph_to_sql(ddl, metadata)
    assert athena.calls == [("AwsDataCatalog", "db", "transactions")]
    property_graph_to_sql(ddl, metadata)  # nothing is kept between calls
    assert len(athena.calls) == 2


def test_athena_metadata_defaults_to_shared_athena_client():
    athena = FakeAthena({})
    with patch("nx_neptune.property_graph.ClientFactory") as factory:
        factory.return_value.athena.return_value = athena
        metadata = AthenaTableMetadata(database="db")
    with pytest.raises(PropertyGraphSchemaError, match="not found"):
        metadata.get_columns(("t",))
    assert athena.calls == [("AwsDataCatalog", "db", "t")]


def test_athena_metadata_errors():
    metadata = AthenaTableMetadata(FakeAthena({}), catalog="cat")
    with pytest.raises(PropertyGraphSchemaError, match="has no database"):
        metadata.get_columns(("t",))
    with pytest.raises(PropertyGraphSchemaError, match="'cat.db.t' not found"):
        metadata.get_columns(("db", "t"))


# --------------------------------------------------------------------------- #
# SessionManager integration
# --------------------------------------------------------------------------- #
def bare_session():
    session = SessionManager.__new__(SessionManager)
    session._athena_client = FakeAthena(
        {
            ("AwsDataCatalog", "db", name): table_metadata(columns)
            for name, columns in PAYSIM_TABLES.items()
        }
    )
    session._neptune_client = session._sts_client = session._iam_client = None
    return session


@pytest.mark.asyncio
async def test_import_from_graph_schema_resolves_against_athena():
    session = bare_session()
    with patch.object(
        SessionManager, "import_from_table", new=AsyncMock(return_value="t-123")
    ) as delegate:
        result = await session.import_from_graph_schema(
            graph="graph-obj",
            s3_location="s3://bucket/prefix/",
            property_graph=PAYSIM_DDL,
            database="db",
        )

    assert result == "t-123"
    args, kwargs = delegate.call_args
    assert args[:2] == ("graph-obj", "s3://bucket/prefix/")
    assert args[2] == to_sql(PAYSIM_DDL, PAYSIM_TABLES)
    assert kwargs["database"] == "db"
