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

Translation has three steps: :func:`parse_property_graph` turns the DDL into a
:class:`PropertyGraph` (the lark grammar is ``_GRAMMAR``), a
:class:`TableMetadataProvider` supplies each source table's columns and types
from the catalog, and :func:`property_graph_to_sql` resolves the two into SQL.
Grammar accepted (case-insensitive keywords)::

    CREATE PROPERTY GRAPH <name>
      VERTEX TABLES (
        <table> [ [AS] <alias> ]
          KEY ( <column> )
          [ LABEL <label> ]
          [ <properties> ]
        [, ...]
      )
      [ EDGE TABLES (
        <table> [ [AS] <alias> ]
          [ KEY ( <column> ) ]
          SOURCE      [ KEY ] ( <column> ) REFERENCES <vertex table> [ ( <column> ) ]
          DESTINATION [ KEY ] ( <column> ) REFERENCES <vertex table> [ ( <column> ) ]
          [ LABEL <label> ]
          [ <properties> ]
        [, ...]
      ) ]

    <properties> ::= PROPERTIES ( <property> [, <property>]* )
                   | PROPERTIES [ARE] ALL COLUMNS [ EXCEPT ( <column> [, ...] ) ]
                   | NO PROPERTIES
    <property>   ::= <column> [ AS <name> ]
                   | CAST ( <column> AS <sql type> ) [ AS <name> ]

As in the standard, each element table is named by its alias, which defaults
to its table name; ``REFERENCES`` names a vertex table that way. Omitting
``<properties>`` means ``ALL COLUMNS``, and an omitted ``LABEL`` defaults to
the element table's name. Edge endpoints are joined to their referenced vertex
table, so edges whose endpoint has no vertex are not loaded. Property types
come from the source columns' catalog types, and ``CAST`` overrides them.
Properties sharing a name under one label must resolve to the same type; the
error for a mismatch includes the ``CAST`` that fixes it.
"""

import difflib
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Union,
    cast,
)

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from lark import Lark, Transformer
from lark.exceptions import (
    LarkError,
    UnexpectedCharacters,
    UnexpectedInput,
    UnexpectedToken,
    VisitError,
)

from .clients.client_factory import ClientFactory
from .clients.response_utils import get_table_column_types, is_entity_not_found

logger = logging.getLogger(__name__)

QualifiedName = Tuple[str, ...]

# Source SQL types (catalog or CAST target, matched on the base type name) that
# map onto a Neptune load-format type. String is emitted without a suffix.
_SQL_TO_NEPTUNE = {
    "boolean": "Bool",
    "tinyint": "Byte",
    "smallint": "Short",
    "int": "Int",
    "integer": "Int",
    "bigint": "Long",
    "float": "Float",
    "real": "Float",
    "double": "Double",
    "decimal": "Double",
    "string": "String",
    "varchar": "String",
    "char": "String",
    "character": "String",
    "date": "Date",
    "timestamp": "Datetime",
}

# The CAST target suggested for each Neptune type.
_NEPTUNE_TO_SQL = {
    "Bool": "BOOLEAN",
    "Byte": "TINYINT",
    "Short": "SMALLINT",
    "Int": "INTEGER",
    "Long": "BIGINT",
    "Float": "REAL",
    "Double": "DOUBLE",
    "String": "VARCHAR",
    "Date": "DATE",
    "Datetime": "TIMESTAMP",
}

_INTEGER_TYPES = ["Byte", "Short", "Int", "Long"]  # narrowest first
_FLOAT_TYPES = ["Float", "Double"]

_KEYWORDS = {
    "ALL",
    "ARE",
    "AS",
    "CAST",
    "COLUMNS",
    "CREATE",
    "DESTINATION",
    "EDGE",
    "EXCEPT",
    "GRAPH",
    "KEY",
    "LABEL",
    "NO",
    "PROPERTIES",
    "PROPERTY",
    "REFERENCES",
    "SOURCE",
    "TABLES",
    "VERTEX",
}

_BARE_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")

# Neptune CSV headers cannot hold these, and a leading '~' marks system columns.
_BAD_PROPERTY_NAME = re.compile(r"^~|[:,\s]")


class PropertyGraphError(ValueError):
    """Base class for problems with a ``CREATE PROPERTY GRAPH`` statement."""


class PropertyGraphSyntaxError(PropertyGraphError):
    """The statement cannot be parsed or is internally inconsistent."""


class PropertyGraphSchemaError(PropertyGraphError):
    """The statement does not match the source tables in the catalog."""


# --------------------------------------------------------------------------- #
# Schema model
# --------------------------------------------------------------------------- #
@dataclass
class PropertyDef:
    """A declared property: source ``column`` exposed as ``name``, optionally CAST."""

    column: str
    name: str
    cast: Optional[str] = None


@dataclass(kw_only=True)
class ElementTable:
    """Fields shared by vertex and edge tables."""

    table: QualifiedName
    label: str
    alias: Optional[str] = None
    properties: List[PropertyDef] = field(default_factory=list)
    all_columns: bool = False
    except_columns: List[str] = field(default_factory=list)

    @property
    def name(self) -> str:
        """The element table's name, as used by ``REFERENCES``."""
        return self.alias or self.table[-1]

    @property
    def table_name(self) -> str:
        return ".".join(self.table)


@dataclass(kw_only=True)
class VertexTable(ElementTable):
    key: str


@dataclass(kw_only=True)
class EdgeTable(ElementTable):
    source_key: str
    dest_key: str
    source_ref: str
    dest_ref: str
    source_ref_columns: Optional[List[str]] = None
    dest_ref_columns: Optional[List[str]] = None
    key: Optional[str] = None


@dataclass
class PropertyGraph:
    name: str
    vertex_tables: List[VertexTable] = field(default_factory=list)
    edge_tables: List[EdgeTable] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Table metadata
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Column:
    name: str
    type: str


class TableMetadataProvider(Protocol):
    """Supplies a source table's columns (name and SQL type) from a catalog."""

    def get_columns(self, table: QualifiedName) -> List[Column]: ...


class StaticTableMetadata:
    """A provider backed by a mapping, for tests and offline translation.

    Keys are table names as written in the DDL (``accounts`` or
    ``db.accounts``, case-insensitive); values are ``(column, sql_type)`` pairs.
    """

    def __init__(self, tables: Mapping[str, Sequence[Tuple[str, str]]]):
        self._tables = {
            name.lower(): [Column(col, typ) for col, typ in cols]
            for name, cols in tables.items()
        }

    def get_columns(self, table: QualifiedName) -> List[Column]:
        key = ".".join(table).lower()
        if key not in self._tables:
            raise PropertyGraphSchemaError(f"Table '{key}' not found")
        return self._tables[key]


class AthenaTableMetadata:
    """A provider that reads table metadata through Athena ``GetTableMetadata``.

    Serves Glue-backed and federated (connector) catalogs alike. Table names may
    be ``table``, ``database.table`` or ``catalog.database.table``; missing parts
    fall back to ``catalog`` and ``database``.
    """

    DEFAULT_CATALOG = "AwsDataCatalog"

    def __init__(
        self,
        athena_client: Optional[BaseClient] = None,
        catalog: Optional[str] = None,
        database: Optional[str] = None,
    ):
        self._client = athena_client or ClientFactory().athena()
        self._catalog = catalog or self.DEFAULT_CATALOG
        self._database = database

    def get_columns(self, table: QualifiedName) -> List[Column]:
        location = self._qualify(table)
        catalog, database, name = location
        try:
            logger.info(
                "Fetching Athena table metadata for %s.%s.%s",
                catalog,
                database,
                name,
            )
            resp = self._client.get_table_metadata(
                CatalogName=catalog, DatabaseName=database, TableName=name
            )
        except ClientError as e:
            where = ".".join(location)
            if is_entity_not_found(e):
                raise PropertyGraphSchemaError(
                    f"Table '{where}' not found or not readable: {e}"
                ) from e
            raise PropertyGraphSchemaError(
                f"Could not read metadata for table '{where}' (requires "
                f"athena:GetTableMetadata, and glue:GetTable for Glue "
                f"catalogs): {e}"
            ) from e
        return [Column(col, typ) for col, typ in get_table_column_types(resp)]

    def _qualify(self, table: QualifiedName) -> Tuple[str, str, str]:
        if len(table) == 3:
            return table[0], table[1], table[2]
        if len(table) == 2:
            return self._catalog, table[0], table[1]
        if len(table) == 1:
            if not self._database:
                raise PropertyGraphSchemaError(
                    f"Table '{table[0]}' has no database: pass database= or "
                    f"write <database>.{table[0]}"
                )
            return self._catalog, self._database, table[0]
        raise PropertyGraphSchemaError(
            f"Table name '{'.'.join(table)}' has too many parts; expected "
            f"[catalog.][database.]table"
        )


# --------------------------------------------------------------------------- #
# Grammar
# --------------------------------------------------------------------------- #
_GRAMMAR = r"""
start: "CREATE"i "PROPERTY"i "GRAPH"i name \
       "VERTEX"i "TABLES"i "(" vertex ("," vertex)* ")" \
       ["EDGE"i "TABLES"i "(" edge ("," edge)* ")"] [";"]

vertex: name alias? key? label? props?
edge:   name alias? key? source? dest? label? props?

source: "SOURCE"i      _keykw? "(" ident ")" "REFERENCES"i name refcols?
dest:   "DESTINATION"i _keykw? "(" ident ")" "REFERENCES"i name refcols?
_keykw: "KEY"i
refcols: "(" ident ("," ident)* ")"

key:     "KEY"i "(" ident ("," ident)* ")"
label:   "LABEL"i ident
props:   "PROPERTIES"i "(" property ("," property)* ")"     -> props
       | "PROPERTIES"i _are? "ALL"i "COLUMNS"i except_cols? -> all_columns
       | "NO"i "PROPERTIES"i                                -> no_props
_are:    "ARE"i
except_cols: "EXCEPT"i "(" ident ("," ident)* ")"
property: ident as_name?                                    -> column_property
        | "CAST"i "(" ident "AS"i sql_type ")" as_name?     -> cast_property
as_name: "AS"i ident
sql_type: CNAME ("(" INT ("," INT)* ")")?
alias:   "AS"i? ident
name:    ident ("." ident)*
ident:   CNAME | QUOTED

QUOTED:  /"(?:[^"]|"")*"/
CNAME:   /[A-Za-z_][A-Za-z0-9_$]*/
COMMENT: /--[^\n]*/ | /\/\*(.|\n)*?\*\//
%import common.INT
%import common.WS
%ignore WS
%ignore COMMENT
"""


# --------------------------------------------------------------------------- #
# Parse-tree markers and transformer
# --------------------------------------------------------------------------- #
# Optional clauses arrive as positional children of the vertex / edge rules,
# so each reduces to a distinctly-typed marker and the builder dispatches on type.
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
    all_columns: bool = False
    except_columns: List[str] = field(default_factory=list)


class _EndpointClause(Enum):
    SOURCE = "SOURCE"
    DESTINATION = "DESTINATION"


@dataclass
class _Endpoint:
    clause: _EndpointClause
    column: str
    ref: str
    ref_columns: Optional[List[str]]


@dataclass
class _Clauses:
    alias: Optional[str] = None
    key: Optional[_Key] = None
    label: Optional[str] = None
    props: Optional[_Props] = None
    endpoints: List[_Endpoint] = field(default_factory=list)


def _collect(children) -> _Clauses:
    clauses = _Clauses()
    for child in children:
        if isinstance(child, _Alias):
            clauses.alias = child.value
        elif isinstance(child, _Key):
            clauses.key = child
        elif isinstance(child, _Label):
            clauses.label = child.value
        elif isinstance(child, _Props):
            clauses.props = child
        elif isinstance(child, _Endpoint):
            clauses.endpoints.append(child)
    return clauses


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
        return tuple(items)

    def alias(self, items):
        return _Alias(items[0])

    def key(self, items):
        return _Key(list(items))

    def label(self, items):
        return _Label(items[0])

    def as_name(self, items):
        return items[0]

    def sql_type(self, items):
        base = items[0].value.upper()
        args = [tok.value for tok in items[1:]]
        return f"{base}({','.join(args)})" if args else base

    def column_property(self, items):
        column = items[0]
        return PropertyDef(column=column, name=items[1] if len(items) > 1 else column)

    def cast_property(self, items):
        column, sql_type = items[0], items[1]
        name = items[2] if len(items) > 2 else column
        return PropertyDef(column=column, name=name, cast=sql_type)

    def props(self, items):
        return _Props(list(items))

    def all_columns(self, items):
        return _Props([], all_columns=True, except_columns=items[0] if items else [])

    def except_cols(self, items):
        return list(items)

    def no_props(self, items):
        return _Props([])

    def refcols(self, items):
        return list(items)

    def source(self, items):
        return _Endpoint(
            _EndpointClause.SOURCE,
            items[0],
            ".".join(items[1]),
            items[2] if len(items) > 2 else None,
        )

    def dest(self, items):
        return _Endpoint(
            _EndpointClause.DESTINATION,
            items[0],
            ".".join(items[1]),
            items[2] if len(items) > 2 else None,
        )

    # KEY, SOURCE and DESTINATION are optional in the grammar only so that
    # leaving one out gets this message rather than a parser error.
    def vertex(self, items):
        table, clauses = items[0], _collect(items[1:])
        if clauses.key is None:
            raise PropertyGraphSyntaxError(
                f"Vertex table '{clauses.alias or table[-1]}' has no KEY: declare "
                f"the column that identifies its vertices, e.g. KEY (id). Primary "
                f"keys are not read from the catalog."
            )
        props = clauses.props or _Props([], all_columns=True)
        return VertexTable(
            table=table,
            alias=clauses.alias,
            key=_single_key(clauses.key.columns, "~id"),
            label=clauses.label or clauses.alias or table[-1],
            properties=props.properties,
            all_columns=props.all_columns,
            except_columns=props.except_columns,
        )

    def edge(self, items):
        table, clauses = items[0], _collect(items[1:])
        endpoints = {e.clause: e for e in clauses.endpoints}
        for clause in _EndpointClause:
            if clause not in endpoints:
                raise PropertyGraphSyntaxError(
                    f"Edge table '{clauses.alias or table[-1]}' has no "
                    f"{clause.value}: declare {clause.value} KEY (<column>) "
                    f"REFERENCES <vertex table>. Foreign keys are not read from "
                    f"the catalog."
                )
        source = endpoints[_EndpointClause.SOURCE]
        dest = endpoints[_EndpointClause.DESTINATION]
        props = clauses.props or _Props([], all_columns=True)
        return EdgeTable(
            table=table,
            alias=clauses.alias,
            key=_single_key(clauses.key.columns, "edge KEY") if clauses.key else None,
            source_key=source.column,
            dest_key=dest.column,
            source_ref=source.ref,
            dest_ref=dest.ref,
            source_ref_columns=source.ref_columns,
            dest_ref_columns=dest.ref_columns,
            label=clauses.label or clauses.alias or table[-1],
            properties=props.properties,
            all_columns=props.all_columns,
            except_columns=props.except_columns,
        )

    def start(self, items):
        graph = PropertyGraph(name=items[0][-1])
        for child in items[1:]:
            if isinstance(child, VertexTable):
                graph.vertex_tables.append(child)
            elif isinstance(child, EdgeTable):
                graph.edge_tables.append(child)
        _validate(graph)
        return graph


def _referenced_vertex(graph: PropertyGraph, ref: str) -> Optional[VertexTable]:
    """The vertex table a REFERENCES target names, by element table name."""
    return next((v for v in graph.vertex_tables if v.name.lower() == ref.lower()), None)


def _unknown_reference(
    graph: PropertyGraph, edge: EdgeTable, clause: _EndpointClause, ref: str
) -> PropertyGraphSyntaxError:
    names = ", ".join(v.name for v in graph.vertex_tables)
    hint = ""
    # A common slip: naming the underlying table or label of an aliased vertex table.
    for v in graph.vertex_tables:
        if ref.lower() in (v.table_name.lower(), v.table[-1].lower(), v.label.lower()):
            hint = f" Did you mean REFERENCES {_ddl_ident(v.name)}?"
            break
    return PropertyGraphSyntaxError(
        f"Edge table '{edge.name}' {clause.value} REFERENCES '{ref}', which is not "
        f"a declared vertex table. REFERENCES names a vertex table by its alias, "
        f"or by its table name when it has no alias; declared: {names}.{hint}"
    )


def _validate(graph: PropertyGraph) -> None:
    seen = set()
    for element in [*graph.vertex_tables, *graph.edge_tables]:
        if element.name.lower() in seen:
            raise PropertyGraphSyntaxError(
                f"Element table name '{element.name}' is used more than once: "
                f"give each use of a table a unique alias, e.g. "
                f"{_ddl_ident(element.table[-1])} AS <name>"
            )
        seen.add(element.name.lower())
    for e in graph.edge_tables:
        for clause, ref, ref_columns in (
            (_EndpointClause.SOURCE, e.source_ref, e.source_ref_columns),
            (_EndpointClause.DESTINATION, e.dest_ref, e.dest_ref_columns),
        ):
            v = _referenced_vertex(graph, ref)
            if v is None:
                raise _unknown_reference(graph, e, clause, ref)
            if ref_columns is not None and [c.lower() for c in ref_columns] != [
                v.key.lower()
            ]:
                raise PropertyGraphSyntaxError(
                    f"Edge table '{e.name}' {clause.value} REFERENCES {ref} "
                    f"({', '.join(ref_columns)}), but vertex table '{v.name}' "
                    f"has KEY ({v.key}); the referenced columns must be its KEY"
                )


# Building the LALR parser is relatively expensive, so do it once at import.
_PARSER = Lark(_GRAMMAR, parser="lalr", transformer=_Builder())


def parse_property_graph(ddl: str) -> PropertyGraph:
    """Parse a ``CREATE PROPERTY GRAPH`` statement into a :class:`PropertyGraph`."""
    try:
        # The embedded _Builder transformer turns the parse tree into a
        # PropertyGraph, but Lark.parse is typed as returning a Tree.
        return cast(PropertyGraph, _PARSER.parse(ddl))
    except PropertyGraphError:
        raise
    except VisitError as exc:
        # lark wraps exceptions raised inside transformer callbacks; surface our
        # own errors unchanged and rewrap anything else.
        if isinstance(exc.orig_exc, PropertyGraphError):
            raise exc.orig_exc from None
        raise PropertyGraphSyntaxError(str(exc)) from exc
    except UnexpectedInput as exc:
        raise PropertyGraphSyntaxError(_syntax_error_message(ddl, exc)) from exc
    except LarkError as exc:
        raise PropertyGraphSyntaxError(str(exc)) from exc


# lark terminal names a user would not recognise.
_TERMINAL_DESCRIPTIONS = {
    "CNAME": "identifier",
    "QUOTED": "identifier",
    "INT": "number",
    "$END": "end of input",
}


def _describe_terminal(name: str) -> str:
    if name in _TERMINAL_DESCRIPTIONS:
        return _TERMINAL_DESCRIPTIONS[name]
    try:
        pattern = _PARSER.get_terminal(name).pattern
    except KeyError:
        return name
    if pattern.type != "str":
        return name
    # Show keywords as written and quote punctuation.
    value = pattern.value.upper()
    return value if value.isalpha() else f"'{value}'"


def _syntax_error_message(ddl: str, exc: UnexpectedInput) -> str:
    """A user-facing message for a parse error: position, culprit, expectations."""
    found = ""
    expected: Sequence[str] = []
    if isinstance(exc, UnexpectedToken):
        tok = exc.token
        found = "end of input" if tok.type == "$END" else repr(str(tok))
        expected = sorted(exc.accepts or exc.expected)
    elif isinstance(exc, UnexpectedCharacters):
        found = repr(exc.char)
        expected = sorted(exc.allowed or [])
    message = f"Syntax error at line {exc.line}, column {exc.column}"
    if found:
        message += f": unexpected {found}"
    message += "."
    if expected:
        names = dict.fromkeys(_describe_terminal(t) for t in expected)
        message += f" Expected one of: {', '.join(names)}."
    lines = ddl.splitlines()
    if isinstance(exc.line, int) and 1 <= exc.line <= len(lines) and exc.column > 0:
        text, col = lines[exc.line - 1], exc.column - 1
        start = max(0, col - 60)  # keep long single-line statements readable
        message += f"\n  {text[start:col + 40]}\n  {' ' * (col - start)}^"
    return message


# --------------------------------------------------------------------------- #
# Resolution against table metadata
# --------------------------------------------------------------------------- #
def _neptune_type(sql_type: str) -> Optional[str]:
    """The Neptune type for a SQL type such as ``decimal(10,2)``, if loadable."""
    base = re.split(r"[\s(<]", sql_type.strip().lower(), maxsplit=1)[0]
    return _SQL_TO_NEPTUNE.get(base)


def _common_type(types: Sequence[str]) -> str:
    """The narrowest Neptune type that all of ``types`` can be cast to."""
    distinct = set(types)
    if distinct <= set(_INTEGER_TYPES):
        return max(distinct, key=_INTEGER_TYPES.index)
    if distinct <= set(_INTEGER_TYPES + _FLOAT_TYPES):
        return "Double"
    if distinct == {"Date", "Datetime"}:
        return "Datetime"
    return "String"


def _ddl_ident(name: str) -> str:
    """Render an identifier for a DDL suggestion, quoting only when needed."""
    if _BARE_IDENT.fullmatch(name) and name.upper() not in _KEYWORDS:
        return name
    return '"' + name.replace('"', '""') + '"'


@dataclass
class _ResolvedProperty:
    name: str
    column: Column
    neptune_type: str
    cast: Optional[str] = None

    def select(self) -> str:
        expr = _quote_ident(self.column.name)
        if self.cast:
            expr = f"CAST({expr} AS {self.cast})"
        if self.neptune_type == "Datetime":
            # Athena renders timestamps as 'yyyy-MM-dd HH:mm:ss'; Neptune needs ISO-8601.
            expr = f"to_iso8601({expr})"
        header = self.name
        if self.neptune_type != "String":
            header = f"{self.name}:{self.neptune_type}"
        return f"{expr} AS {_quote_header(header)}"


class _ElementKind(Enum):
    VERTEX = "vertex"
    EDGE = "edge"


@dataclass
class _ResolvedElement:
    element: ElementTable
    kind: _ElementKind
    key: Optional[Column]  # vertex ~id column
    source: Optional[Column]  # edge ~from column
    dest: Optional[Column]  # edge ~to column
    properties: List[_ResolvedProperty]


def _lookup(
    columns: Dict[str, Column], element: ElementTable, name: str, role: str
) -> Column:
    col = columns.get(name.lower())
    if col is not None:
        return col
    close = difflib.get_close_matches(name.lower(), list(columns), n=1)
    hint = f" Did you mean '{columns[close[0]].name}'?" if close else ""
    raise PropertyGraphSchemaError(
        f"{role} column '{name}' not found in table '{element.table_name}'.{hint} "
        f"Columns: {', '.join(c.name for c in columns.values())}"
    )


def _id_column(
    columns: Dict[str, Column], element: ElementTable, name: str, role: str
) -> Column:
    col = _lookup(columns, element, name, role)
    if _neptune_type(col.type) is None:
        raise PropertyGraphSchemaError(
            f"{role} column '{col.name}' in table '{element.table_name}' has type "
            f"{col.type}, which cannot be used as a Neptune id"
        )
    return col


def _resolve_properties(
    element: ElementTable, columns: Dict[str, Column]
) -> List[_ResolvedProperty]:
    resolved = []
    if element.all_columns:
        excluded = {
            _lookup(columns, element, c, "EXCEPT").name.lower()
            for c in element.except_columns
        }
        for col in columns.values():
            if col.name.lower() in excluded:
                continue
            ntype = _neptune_type(col.type)
            if ntype is None:
                raise PropertyGraphSchemaError(
                    f"Column '{col.name}' in table '{element.table_name}' has type "
                    f"{col.type}, which cannot be loaded as a Neptune property. "
                    f"Leave it out with PROPERTIES ALL COLUMNS EXCEPT "
                    f"({_ddl_ident(col.name)}), or list the properties explicitly."
                )
            resolved.append(_ResolvedProperty(col.name, col, ntype))
    for prop in element.properties:
        col = _lookup(columns, element, prop.column, "Property")
        if prop.cast:
            ntype = _neptune_type(prop.cast)
            if ntype is None:
                raise PropertyGraphSchemaError(
                    f"CAST({prop.column} AS {prop.cast}) in table "
                    f"'{element.table_name}': {prop.cast} cannot be loaded as a "
                    f"Neptune property. Supported CAST targets: "
                    f"{', '.join(_NEPTUNE_TO_SQL.values())}"
                )
        else:
            ntype = _neptune_type(col.type)
            if ntype is None:
                raise PropertyGraphSchemaError(
                    f"Column '{col.name}' in table '{element.table_name}' has type "
                    f"{col.type}, which cannot be loaded as a Neptune property; "
                    f"remove it from the PROPERTIES list."
                )
        resolved.append(_ResolvedProperty(prop.name, col, ntype, prop.cast))

    seen = set()
    for p in resolved:
        if _BAD_PROPERTY_NAME.search(p.name):
            safe = re.sub(r"[:,\s~]+", "_", p.name).strip("_") or "property"
            raise PropertyGraphSchemaError(
                f"Property name '{p.name}' in table '{element.table_name}' cannot "
                f"be a Neptune property name (no ':', ',', whitespace or leading "
                f"'~'). Rename it, e.g. {_ddl_ident(p.column.name)} AS {safe}."
            )
        if p.name in seen:
            raise PropertyGraphSchemaError(
                f"Property '{p.name}' is defined twice in table '{element.table_name}'"
            )
        seen.add(p.name)
    return resolved


def _resolve(element: ElementTable, table_columns: List[Column]) -> _ResolvedElement:
    columns = {c.name.lower(): c for c in table_columns}
    key = source = dest = None
    if isinstance(element, VertexTable):
        key = _id_column(columns, element, element.key, "KEY")
        kind = _ElementKind.VERTEX
    else:
        edge = cast(EdgeTable, element)
        source = _id_column(columns, edge, edge.source_key, "SOURCE KEY")
        dest = _id_column(columns, edge, edge.dest_key, "DESTINATION KEY")
        if edge.key:
            _lookup(columns, edge, edge.key, "KEY")
        kind = _ElementKind.EDGE
    props = _resolve_properties(element, columns)
    return _ResolvedElement(element, kind, key, source, dest, props)


def _properties_fix(
    resolved: _ResolvedElement, fix: _ResolvedProperty, target: str
) -> str:
    """A replacement PROPERTIES clause for ``resolved`` that casts ``fix`` to ``target``."""
    parts = []
    for p in resolved.properties:
        cast_to = _NEPTUNE_TO_SQL[target] if p is fix else p.cast
        expr = _ddl_ident(p.column.name)
        if cast_to:
            expr = f"CAST({expr} AS {cast_to})"
        if cast_to or p.name != p.column.name:
            expr += f" AS {_ddl_ident(p.name)}"
        parts.append(expr)
    return f"PROPERTIES ({', '.join(parts)})"


def _check_type_conflicts(elements: List[_ResolvedElement]) -> None:
    groups: Dict[
        Tuple[_ElementKind, str, str],
        List[Tuple[_ResolvedElement, _ResolvedProperty]],
    ] = {}
    for r in elements:
        for p in r.properties:
            groups.setdefault((r.kind, r.element.label, p.name), []).append((r, p))
    for (kind, label, name), members in groups.items():
        types = {p.neptune_type for _, p in members}
        if len(types) < 2:
            continue
        target = _common_type(list(types))
        found = ", ".join(
            f"{p.neptune_type} in '{r.element.name}' (column {p.column.name}: "
            f"{p.column.type})"
            for r, p in members
        )
        fixes = "\n".join(
            f"  in '{r.element.name}': {_properties_fix(r, p, target)}"
            for r, p in members
            if p.neptune_type != target
        )
        raise PropertyGraphSchemaError(
            f"Property '{name}' of {kind.value} label '{label}' has conflicting types: "
            f"{found}. Cast to a common type ({target}) in the PROPERTIES clause:\n"
            f"{fixes}"
        )


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


def _table_ref(table: QualifiedName) -> str:
    """Render a possibly-qualified table reference, quoting each segment."""
    return ".".join(_quote_ident(part) for part in table)


def _vertex_query(r: _ResolvedElement) -> str:
    assert r.key is not None
    key = _quote_ident(r.key.name)
    cols = [
        f"{key} AS {_quote_header('~id')}",
        f"{_quote_literal(r.element.label)} AS {_quote_header('~label')}",
    ]
    cols.extend(p.select() for p in r.properties)
    return (
        "SELECT DISTINCT "
        + ", ".join(cols)
        + f" FROM {_table_ref(r.element.table)}"
        + f" WHERE {key} IS NOT NULL"
    )


def _endpoint_join(alias: str, column: Column, vertex: _ResolvedElement) -> str:
    """Join an edge endpoint column to the distinct keys of its vertex table.

    Inner-joining drops edges whose endpoint has no vertex (and NULL endpoints);
    without it, Neptune would load them against unlabeled, property-less
    vertices. DISTINCT keeps duplicate vertex rows from multiplying edges.
    """
    assert vertex.key is not None
    joined = f"{_quote_ident(alias)}.{_quote_ident('~key')}"
    left = _quote_ident(column.name)
    if _neptune_type(column.type) != _neptune_type(vertex.key.type):
        # Neptune ids are strings, so compare as strings when the types differ.
        left, joined = f"CAST({left} AS VARCHAR)", f"CAST({joined} AS VARCHAR)"
    return (
        f" JOIN (SELECT DISTINCT {_quote_ident(vertex.key.name)} AS "
        f"{_quote_ident('~key')} FROM {_table_ref(vertex.element.table)}) "
        f"{_quote_ident(alias)} ON {left} = {joined}"
    )


def _edge_query(
    r: _ResolvedElement, source_vertex: _ResolvedElement, dest_vertex: _ResolvedElement
) -> str:
    assert r.source is not None and r.dest is not None
    cols = [
        f"{_quote_ident(r.source.name)} AS {_quote_header('~from')}",
        f"{_quote_ident(r.dest.name)} AS {_quote_header('~to')}",
        f"{_quote_literal(r.element.label)} AS {_quote_header('~label')}",
    ]
    cols.extend(p.select() for p in r.properties)
    return (
        "SELECT "
        + ", ".join(cols)
        + f" FROM {_table_ref(r.element.table)}"
        + _endpoint_join("~source", r.source, source_vertex)
        + _endpoint_join("~destination", r.dest, dest_vertex)
    )


def property_graph_to_sql(
    property_graph: Union[str, PropertyGraph], metadata: TableMetadataProvider
) -> List[str]:
    """Translate a ``CREATE PROPERTY GRAPH`` statement into Athena projection SQL.

    ``metadata`` supplies each source table's columns and types: use
    :class:`AthenaTableMetadata` against a live catalog, or
    :class:`StaticTableMetadata` offline. Returns the vertex projection queries
    followed by the edge projection queries, in the ``~id``/``~label`` and
    ``~from``/``~to``/``~label`` load format expected by
    ``SessionManager.import_from_table``. Each edge query joins its endpoints to
    the referenced vertex tables, so only edges between declared vertices load.

    Raises:
        PropertyGraphSyntaxError: If the statement cannot be parsed.
        PropertyGraphSchemaError: If it does not match the source tables.
    """
    graph = (
        property_graph
        if isinstance(property_graph, PropertyGraph)
        else parse_property_graph(property_graph)
    )
    # Read each source table once per call, even when it backs several element
    # tables; nothing is kept between calls, so schema changes are always seen.
    tables: Dict[QualifiedName, List[Column]] = {}

    def resolve(element: ElementTable) -> _ResolvedElement:
        key = tuple(part.lower() for part in element.table)
        if key not in tables:
            tables[key] = metadata.get_columns(element.table)
        return _resolve(element, tables[key])

    vertices = [resolve(v) for v in graph.vertex_tables]
    edges = [resolve(e) for e in graph.edge_tables]
    _check_type_conflicts(vertices + edges)
    by_name = {r.element.name.lower(): r for r in vertices}
    return [_vertex_query(r) for r in vertices] + [
        _edge_query(
            r,
            by_name[cast(EdgeTable, r.element).source_ref.lower()],
            by_name[cast(EdgeTable, r.element).dest_ref.lower()],
        )
        for r in edges
    ]
