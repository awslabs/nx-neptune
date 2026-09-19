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

"""SQL/PGQ ``CREATE PROPERTY GRAPH`` front-end for Athena projections.

Instead of hand-writing the Athena ``SELECT`` statements that alias source
columns into Neptune Analytics' Gremlin CSV load format (``~id`` / ``~label``
for vertices, ``~from`` / ``~to`` / ``~label`` for edges), a caller declares a
property-graph schema with the standard SQL/PGQ (SQL:2023) ``CREATE PROPERTY
GRAPH`` DDL, and this module translates it into those projection queries. The
generated queries feed the unchanged ``SessionManager.import_from_table`` path,
so only the front-end changes.

The grammar is defined declaratively in ``_GRAMMAR`` and parsed with
`lark <https://github.com/lark-parser/lark>`_; ``_Builder`` folds the parse tree
into the schema dataclasses below. Grammar accepted (case-insensitive
keywords)::

    CREATE PROPERTY GRAPH <name>
      VERTEX TABLES (
        <table> [ [AS] <alias> ]
          KEY ( <column> )
          [ LABEL <label> ]
          [ PROPERTIES ( <prop> [, <prop>]* ) | NO PROPERTIES ]
        [, ...]
      )
      [ EDGE TABLES (
        <table> [ [AS] <alias> ]
          [ KEY ( <column> ) ]
          SOURCE      [ KEY ( <column> ) ] REFERENCES <vertex> [ ( <column> ) ]
          DESTINATION [ KEY ( <column> ) ] REFERENCES <vertex> [ ( <column> ) ]
          [ LABEL <label> ]
          [ PROPERTIES ( <prop> [, <prop>]* ) | NO PROPERTIES ]
        [, ...]
      ) ]

A ``<prop>`` is ``<column> [AS <name>] [: <NeptuneType>]``. The optional
``:Type`` suffix is an nx-neptune extension matching Neptune's load-format
header convention (e.g. ``amount:Float``); without it the property is loaded as
a string. When ``LABEL`` is omitted the label defaults to the table's
name/alias.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from lark import Lark, Transformer
from lark.exceptions import LarkError, VisitError

# Neptune Analytics Gremlin CSV load-format property types (the suffix after the
# colon in a column header, e.g. ``amount:Float``). ``String`` is the default
# and is emitted without a suffix.
NEPTUNE_TYPES = {
    "String",
    "Byte",
    "Short",
    "Int",
    "Long",
    "Float",
    "Double",
    "Bool",
    "Boolean",
    "Date",
}


class PropertyGraphSyntaxError(ValueError):
    """Raised when a ``CREATE PROPERTY GRAPH`` statement cannot be parsed."""


# --------------------------------------------------------------------------- #
# Schema model
# --------------------------------------------------------------------------- #
@dataclass
class PropertyDef:
    """A single mapped property: source ``column`` -> output ``name[:type]``."""

    column: str
    name: str
    type: Optional[str] = None

    def header(self) -> str:
        """The Neptune load-format column header alias for this property."""
        return f"{self.name}:{self.type}" if self.type else self.name


@dataclass
class VertexTable:
    table: str
    key: str
    label: str
    properties: List[PropertyDef] = field(default_factory=list)


@dataclass
class EdgeTable:
    table: str
    source_key: str
    dest_key: str
    label: str
    key: Optional[str] = None
    source_ref: Optional[str] = None
    dest_ref: Optional[str] = None
    properties: List[PropertyDef] = field(default_factory=list)


@dataclass
class PropertyGraph:
    name: str
    vertex_tables: List[VertexTable] = field(default_factory=list)
    edge_tables: List[EdgeTable] = field(default_factory=list)

    def to_sql_queries(self) -> List[str]:
        """Generate the Athena projection queries (vertices first, then edges)."""
        return [_vertex_query(v) for v in self.vertex_tables] + [
            _edge_query(e) for e in self.edge_tables
        ]


# --------------------------------------------------------------------------- #
# SQL generation
# --------------------------------------------------------------------------- #
def _quote_ident(name: str) -> str:
    """Double-quote a SQL identifier, doubling any embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """Single-quote a SQL string literal, doubling any embedded single quotes."""
    return "'" + value.replace("'", "''") + "'"


def _quote_header(header: str) -> str:
    """Double-quote a load-format header alias (``~id``, ``name:Float``, ...)."""
    return '"' + header.replace('"', '""') + '"'


def _table_ref(table: str) -> str:
    """Render a possibly-dotted table reference, quoting each bare segment."""
    return ".".join(_quote_ident(part) for part in table.split("."))


def _property_selects(properties: List[PropertyDef]) -> List[str]:
    return [
        f"{_quote_ident(p.column)} AS {_quote_header(p.header())}" for p in properties
    ]


def _vertex_query(v: VertexTable) -> str:
    cols = [
        f"{_quote_ident(v.key)} AS {_quote_header('~id')}",
        f"{_quote_literal(v.label)} AS {_quote_header('~label')}",
    ]
    cols.extend(_property_selects(v.properties))
    return (
        "SELECT DISTINCT "
        + ", ".join(cols)
        + f" FROM {_table_ref(v.table)}"
        + f" WHERE {_quote_ident(v.key)} IS NOT NULL"
    )


def _edge_query(e: EdgeTable) -> str:
    cols = [
        f"{_quote_ident(e.source_key)} AS {_quote_header('~from')}",
        f"{_quote_ident(e.dest_key)} AS {_quote_header('~to')}",
        f"{_quote_literal(e.label)} AS {_quote_header('~label')}",
    ]
    cols.extend(_property_selects(e.properties))
    return (
        "SELECT "
        + ", ".join(cols)
        + f" FROM {_table_ref(e.table)}"
        + f" WHERE {_quote_ident(e.source_key)} IS NOT NULL"
        + f" AND {_quote_ident(e.dest_key)} IS NOT NULL"
    )


# --------------------------------------------------------------------------- #
# Grammar
# --------------------------------------------------------------------------- #
_GRAMMAR = r"""
start: "CREATE"i "PROPERTY"i "GRAPH"i name \
       "VERTEX"i "TABLES"i "(" vertex ("," vertex)* ")" \
       ["EDGE"i "TABLES"i "(" edge ("," edge)* ")"]

vertex: name alias? key label? props?
edge:   name alias? key? source dest label? props?

source: "SOURCE"i      _keykw? "(" ident ")" "REFERENCES"i name refcols?
dest:   "DESTINATION"i _keykw? "(" ident ")" "REFERENCES"i name refcols?
_keykw: "KEY"i
refcols: "(" ident ("," ident)* ")"

key:     "KEY"i "(" ident ("," ident)* ")"
label:   "LABEL"i ident
props:   "PROPERTIES"i "(" property ("," property)* ")" -> props
       | "NO"i "PROPERTIES"i                            -> no_props
property: ident as_name? ptype?
as_name: "AS"i ident
ptype:   ":" ident
alias:   "AS"i? ident
name:    ident ("." ident)*
ident:   CNAME | QUOTED

QUOTED:  /"(?:[^"]|"")*"/
CNAME:   /[A-Za-z_][A-Za-z0-9_$]*/
COMMENT: /--[^\n]*/ | /\/\*(.|\n)*?\*\//
%import common.WS
%ignore WS
%ignore COMMENT
"""


# --------------------------------------------------------------------------- #
# Parse-tree markers and transformer
# --------------------------------------------------------------------------- #
# Optional clauses (alias / key / label / props) arrive as positional children
# of the vertex / edge rules, so each reduces to a distinctly-typed marker and
# the builder dispatches on type rather than position.
@dataclass
class _Alias:
    value: str


@dataclass
class _Key:
    columns: List[str]


@dataclass
class _Label:
    value: str


@dataclass
class _Props:
    properties: List[PropertyDef]


@dataclass
class _Endpoint:
    column: str
    ref: str


def _single_key(columns: List[str], context: str) -> str:
    if len(columns) != 1:
        raise PropertyGraphSyntaxError(
            f"Composite keys are not supported: a Neptune {context} maps to a "
            f"single column, got {columns}"
        )
    return columns[0]


class _Builder(Transformer):
    """Fold the lark parse tree into a :class:`PropertyGraph`."""

    def ident(self, items):
        tok = items[0]
        if tok.type == "QUOTED":
            return tok.value[1:-1].replace('""', '"')
        return tok.value

    def name(self, items):
        return ".".join(items)

    def alias(self, items):
        return _Alias(items[0])

    def key(self, items):
        return _Key(list(items))

    def label(self, items):
        return _Label(items[0])

    def as_name(self, items):
        return ("as", items[0])

    def ptype(self, items):
        return ("type", _canonical_type(items[0]))

    def property(self, items):
        column = items[0]
        name = column
        ptype = None
        for kind, value in items[1:]:
            if kind == "as":
                name = value
            else:
                ptype = value
        return PropertyDef(column=column, name=name, type=ptype)

    def props(self, items):
        return _Props(list(items))

    def no_props(self, items):
        return _Props([])

    def refcols(self, items):
        return None  # referenced columns are not needed for the projection

    def source(self, items):
        return _Endpoint(column=items[0], ref=items[1])

    def dest(self, items):
        return _Endpoint(column=items[0], ref=items[1])

    def vertex(self, items):
        table = items[0]
        alias = key = label = props = None
        for child in items[1:]:
            if isinstance(child, _Alias):
                alias = child.value
            elif isinstance(child, _Key):
                key = child
            elif isinstance(child, _Label):
                label = child.value
            elif isinstance(child, _Props):
                props = child.properties
        if key is None:
            raise PropertyGraphSyntaxError(
                f"Vertex table '{table}' is missing a KEY clause"
            )
        return VertexTable(
            table=table,
            key=_single_key(key.columns, "~id"),
            label=label if label is not None else (alias or table),
            properties=props or [],
        )

    def edge(self, items):
        table = items[0]
        alias = key = label = props = None
        endpoints = []
        for child in items[1:]:
            if isinstance(child, _Alias):
                alias = child.value
            elif isinstance(child, _Key):
                key = child
            elif isinstance(child, _Label):
                label = child.value
            elif isinstance(child, _Props):
                props = child.properties
            elif isinstance(child, _Endpoint):
                endpoints.append(child)
        source, dest = endpoints
        return EdgeTable(
            table=table,
            key=_single_key(key.columns, "edge KEY") if key is not None else None,
            source_key=source.column,
            dest_key=dest.column,
            source_ref=source.ref,
            dest_ref=dest.ref,
            label=label if label is not None else (alias or table),
            properties=props or [],
        )

    def start(self, items):
        name = items[0]
        graph = PropertyGraph(name=name)
        for child in items[1:]:
            if isinstance(child, VertexTable):
                graph.vertex_tables.append(child)
            elif isinstance(child, EdgeTable):
                graph.edge_tables.append(child)
        _validate(graph)
        return graph


def _canonical_type(raw: str) -> str:
    for t in NEPTUNE_TYPES:
        if t.lower() == raw.lower():
            return t
    raise PropertyGraphSyntaxError(
        f"Unknown property type '{raw}'. Supported types: "
        f"{', '.join(sorted(NEPTUNE_TYPES))}"
    )


def _validate(graph: PropertyGraph) -> None:
    if not graph.vertex_tables:
        raise PropertyGraphSyntaxError(
            "A property graph must declare at least one VERTEX TABLE"
        )
    labels = {v.label for v in graph.vertex_tables}
    tables = {v.table for v in graph.vertex_tables}
    known = labels | tables
    for e in graph.edge_tables:
        for endpoint, ref in (("SOURCE", e.source_ref), ("DESTINATION", e.dest_ref)):
            if ref is not None and ref not in known:
                raise PropertyGraphSyntaxError(
                    f"Edge table '{e.table}' {endpoint} REFERENCES '{ref}', "
                    f"which is not a declared vertex table or label"
                )


# Building the LALR parser is relatively expensive, so do it once at import.
_PARSER = Lark(_GRAMMAR, parser="lalr", transformer=_Builder())


def parse_property_graph(ddl: str) -> PropertyGraph:
    """Parse a ``CREATE PROPERTY GRAPH`` statement into a :class:`PropertyGraph`."""
    try:
        return _PARSER.parse(ddl)
    except PropertyGraphSyntaxError:
        raise
    except VisitError as exc:
        # lark wraps exceptions raised inside transformer callbacks; surface our
        # own syntax errors unchanged and rewrap anything else.
        if isinstance(exc.orig_exc, PropertyGraphSyntaxError):
            raise exc.orig_exc from None
        raise PropertyGraphSyntaxError(str(exc)) from exc
    except LarkError as exc:
        raise PropertyGraphSyntaxError(str(exc)) from exc


def property_graph_to_sql(ddl: str) -> List[str]:
    """Translate a ``CREATE PROPERTY GRAPH`` statement into Athena projection SQL.

    Returns the vertex projection queries followed by the edge projection
    queries, in the ``~id``/``~label`` and ``~from``/``~to``/``~label`` load
    format expected by ``SessionManager.import_from_table``.
    """
    return parse_property_graph(ddl).to_sql_queries()
