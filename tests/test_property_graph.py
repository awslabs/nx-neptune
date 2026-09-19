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

from nx_neptune.property_graph import (
    PropertyGraphSyntaxError,
    parse_property_graph,
    property_graph_to_sql,
)

# A paysim-style schema: customers modelled from an accounts dimension table,
# transactions as edges between them, mirroring the import demos.
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
      PROPERTIES (
        step:Int,
        amount:Float,
        isFraud:Int
      )
  )
"""


def test_paysim_generates_vertex_and_edge_queries():
    queries = property_graph_to_sql(PAYSIM_DDL)
    assert len(queries) == 2
    vertex, edge = queries

    assert vertex == (
        'SELECT DISTINCT "name" AS "~id", \'customer\' AS "~label", '
        '"name" AS "name" FROM "accounts" WHERE "name" IS NOT NULL'
    )
    assert edge == (
        'SELECT "nameOrig" AS "~from", "nameDest" AS "~to", '
        '\'transfer\' AS "~label", "step" AS "step:Int", '
        '"amount" AS "amount:Float", "isFraud" AS "isFraud:Int" '
        'FROM "transactions" '
        'WHERE "nameOrig" IS NOT NULL AND "nameDest" IS NOT NULL'
    )


def test_label_defaults_to_alias_then_table():
    graph = parse_property_graph(
        """
        CREATE PROPERTY GRAPH g
          VERTEX TABLES ( people AS person KEY (id) )
          EDGE TABLES (
            knows SOURCE KEY (a) REFERENCES person
                  DESTINATION KEY (b) REFERENCES person
          )
        """
    )
    assert graph.vertex_tables[0].label == "person"  # alias wins
    assert graph.edge_tables[0].label == "knows"  # falls back to table name


def test_property_rename_and_no_properties():
    graph = parse_property_graph(
        """
        CREATE PROPERTY GRAPH g
          VERTEX TABLES (
            t KEY (id) LABEL n PROPERTIES (raw_name AS name, score:Double)
          )
        """
    )
    props = graph.vertex_tables[0].properties
    assert props[0].column == "raw_name" and props[0].name == "name"
    assert props[0].header() == "name"
    assert props[1].header() == "score:Double"


def test_no_properties_clause():
    graph = parse_property_graph(
        "CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY (id) NO PROPERTIES )"
    )
    assert graph.vertex_tables[0].properties == []


def test_source_destination_key_keyword_optional():
    # KEY keyword may be omitted after SOURCE / DESTINATION.
    queries = property_graph_to_sql(
        """
        CREATE PROPERTY GRAPH g
          VERTEX TABLES ( v KEY (id) )
          EDGE TABLES (
            e SOURCE (src) REFERENCES v DESTINATION (dst) REFERENCES v
          )
        """
    )
    assert '"src" AS "~from"' in queries[1]
    assert '"dst" AS "~to"' in queries[1]


def test_dotted_table_reference_quotes_each_segment():
    queries = property_graph_to_sql(
        "CREATE PROPERTY GRAPH g VERTEX TABLES ( lake.accounts KEY (id) )"
    )
    assert 'FROM "lake"."accounts"' in queries[0]


def test_label_with_quote_is_escaped():
    queries = property_graph_to_sql(
        'CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY (id) LABEL "O\'Brien" )'
    )
    assert "'O''Brien' AS \"~label\"" in queries[0]


def test_quoted_identifier_with_embedded_quote():
    queries = property_graph_to_sql(
        'CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY ("wei""rd") )'
    )
    assert '"wei""rd" AS "~id"' in queries[0]


def test_comments_are_ignored():
    queries = property_graph_to_sql(
        """
        -- a line comment
        CREATE PROPERTY GRAPH g /* block */ VERTEX TABLES ( t KEY (id) )
        """
    )
    assert len(queries) == 1


def test_composite_key_rejected():
    with pytest.raises(PropertyGraphSyntaxError, match="Composite keys"):
        parse_property_graph(
            "CREATE PROPERTY GRAPH g VERTEX TABLES ( t KEY (a, b) )"
        )


def test_unknown_property_type_rejected():
    with pytest.raises(PropertyGraphSyntaxError, match="Unknown property type"):
        parse_property_graph(
            "CREATE PROPERTY GRAPH g VERTEX TABLES "
            "( t KEY (id) PROPERTIES (x:Nope) )"
        )


def test_dangling_reference_rejected():
    with pytest.raises(PropertyGraphSyntaxError, match="REFERENCES 'ghost'"):
        parse_property_graph(
            """
            CREATE PROPERTY GRAPH g
              VERTEX TABLES ( v KEY (id) )
              EDGE TABLES (
                e SOURCE KEY (a) REFERENCES ghost
                  DESTINATION KEY (b) REFERENCES v
              )
            """
        )


def test_missing_vertex_tables_rejected():
    with pytest.raises(PropertyGraphSyntaxError):
        parse_property_graph("CREATE PROPERTY GRAPH g EDGE TABLES ( e )")


def test_garbage_input_rejected():
    with pytest.raises(PropertyGraphSyntaxError):
        parse_property_graph("this is not a property graph")


def test_reference_by_table_name_when_no_alias():
    # REFERENCES may target the table name when no alias/label is given.
    graph = parse_property_graph(
        """
        CREATE PROPERTY GRAPH g
          VERTEX TABLES ( accounts KEY (id) )
          EDGE TABLES (
            e SOURCE KEY (a) REFERENCES accounts
              DESTINATION KEY (b) REFERENCES accounts
          )
        """
    )
    assert graph.edge_tables[0].source_ref == "accounts"


@pytest.mark.asyncio
async def test_import_from_graph_schema_delegates_to_import_from_table():
    from nx_neptune.session_manager import SessionManager

    session = SessionManager.__new__(SessionManager)
    with patch.object(
        SessionManager,
        "import_from_table",
        new=AsyncMock(return_value="g-123"),
    ) as delegate:
        result = await session.import_from_graph_schema(
            graph="graph-obj",
            s3_location="s3://bucket/prefix/",
            property_graph=PAYSIM_DDL,
            catalog="cat",
            database="db",
        )

    assert result == "g-123"
    delegate.assert_awaited_once()
    args, kwargs = delegate.call_args
    # positional: (graph, s3_location, sql_queries)
    assert args[0] == "graph-obj"
    assert args[1] == "s3://bucket/prefix/"
    assert len(args[2]) == 2  # one vertex query + one edge query
    assert kwargs["catalog"] == "cat"
    assert kwargs["database"] == "db"
