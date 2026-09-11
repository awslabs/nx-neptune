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
"""Unit tests proving labels, property keys, and algorithm parameter values
cannot inject openCypher syntax.

These run in CI (no live Neptune needed), complementing the integration test in
integ_test/graph_operations/test_security_injection.py which covers node IDs and
property values against a real graph.
"""

import pytest

from nx_neptune.clients import Edge, Node
from nx_neptune.clients.opencypher_builder import (
    _escape_identifier,
    _escape_property_key,
    _escape_property_path,
    _to_parameter_list,
    insert_edge,
    insert_node,
    pagerank_query,
    update_edge,
    update_node,
    wcc_query,
)

# The PoC payload from the finding, plus variants that try to break out of a
# string literal, an identifier, or a list.
PARAM_INJECTION_PAYLOAD = 'X"}) MATCH (m) DETACH DELETE m //'
IDENTIFIER_INJECTION_PAYLOAD = "X`}) MATCH (m) DETACH DELETE m //"


class TestParameterValueInjection:
    def test_identifier_param_quoted_string_escaped(self):
        """An identifier-valued param (writeProperty) is emitted as a quoted
        string literal with the payload's quote/backslash escaped, so it cannot
        terminate the literal. (Inside a neptune.algo.* config map these are
        string values, not backtick identifiers.)"""
        rendered = _to_parameter_list({"writeProperty": PARAM_INJECTION_PAYLOAD})
        # value wrapped in double quotes; the embedded double-quote is escaped
        assert rendered == 'writeProperty:"X\\"}) MATCH (m) DETACH DELETE m //"'
        assert '\\"' in rendered  # the quote that would break out is escaped

    def test_enum_param_rejects_bad_value(self):
        """A closed-domain enum param rejects anything outside the documented set."""
        with pytest.raises(ValueError):
            _to_parameter_list({"traversalDirection": PARAM_INJECTION_PAYLOAD})
        with pytest.raises(ValueError):
            _to_parameter_list({"edgeWeightType": "double; DROP"})

    def test_enum_param_accepts_valid_value(self):
        assert (
            _to_parameter_list({"traversalDirection": "inbound"})
            == 'traversalDirection:"inbound"'
        )
        assert (
            _to_parameter_list({"edgeWeightType": "double"})
            == 'edgeWeightType:"double"'
        )

    def test_numeric_param_rejects_string_payload(self):
        """A numeric param cannot carry a string payload."""
        with pytest.raises(ValueError):
            _to_parameter_list({"numOfIterations": PARAM_INJECTION_PAYLOAD})
        with pytest.raises(ValueError):
            _to_parameter_list({"dampingFactor": "0.9}) MATCH (m) DETACH DELETE m //"})

    def test_numeric_param_accepts_numbers(self):
        assert _to_parameter_list({"dampingFactor": 0.9, "numOfIterations": 50}) == (
            "dampingFactor:0.9, numOfIterations:50"
        )

    def test_unknown_param_key_rejected(self):
        """A parameter key outside the allowlist fails closed."""
        with pytest.raises(ValueError):
            _to_parameter_list({"maliciousKey": 1})
        # A payload used AS a key is also rejected.
        with pytest.raises(ValueError):
            _to_parameter_list({PARAM_INJECTION_PAYLOAD: 1})

    def test_list_param_escapes_string_elements(self):
        """List elements are individually string-escaped, not rendered via repr."""
        rendered = _to_parameter_list(
            {"edgeLabels": ['route", }) MATCH (m) DELETE m //']}
        )
        # element is a quoted string literal with the double-quote escaped
        assert rendered.startswith("edgeLabels:[")
        assert '\\"' in rendered  # the embedded double quote was escaped

    def test_empty_list_param_renders_empty_brackets(self):
        """An empty list-valued param renders as [] and does not error."""
        assert _to_parameter_list({"edgeLabels": []}) == "edgeLabels:[]"

    def test_trailing_backslash_cannot_escape_closing_quote(self):
        """A value ending in a backslash must not escape the literal's closing
        quote. The backslash is doubled (escaped first) so the closing quote
        stays intact."""
        rendered = _to_parameter_list({"writeProperty": "X\\"})
        assert rendered == r'writeProperty:"X\\"'

    def test_trailing_backslash_then_quote_breakout_neutralized(self):
        """A backslash-then-quote payload can't break out: the backslash is
        doubled and the quote is independently escaped, so neither terminates
        the string literal."""
        payload = 'X\\") MATCH (m) DETACH DELETE m //'
        rendered = _to_parameter_list({"writeProperty": payload})
        assert rendered == r'writeProperty:"X\\\") MATCH (m) DETACH DELETE m //"'

    def test_unicode_escape_sequence_is_not_interpreted(self):
        """A payload containing the literal characters ``\\u0022`` (an attempt
        to smuggle a double-quote via a unicode escape) is neutralized: the
        backslash is doubled first, so it renders as a literal backslash
        followed by the text ``u0022`` and can never decode to a quote that
        terminates the literal."""
        payload = "X\\u0022 ) MATCH (m) DETACH DELETE m //"
        rendered = _to_parameter_list({"writeProperty": payload})
        assert rendered == r'writeProperty:"X\\u0022 ) MATCH (m) DETACH DELETE m //"'

    def test_error_message_truncates_long_payload(self):
        """A rejected value is not echoed verbatim into the error message; a
        long attacker payload is truncated so it can't flood exceptions/logs."""
        payload = "A" * 500 + '") MATCH (m) DETACH DELETE m //'
        with pytest.raises(ValueError) as exc_info:
            _to_parameter_list({"numOfIterations": payload})
        msg = str(exc_info.value)
        assert payload not in msg  # full payload not reflected
        assert "…" in msg  # truncated
        assert len(msg) < 120  # message stays bounded

    def test_pagerank_query_injection_is_neutralized(self):
        """End-to-end: a vertexLabel payload cannot inject into pagerank_query.
        It is confined inside an escaped double-quoted string literal."""
        query, _ = pagerank_query({"vertexLabel": PARAM_INJECTION_PAYLOAD})
        assert 'vertexLabel:"X\\"}) MATCH (m) DETACH DELETE m //"' in query

    def test_wcc_query_rejects_bad_enum(self):
        with pytest.raises(ValueError):
            wcc_query({"traversalDirection": PARAM_INJECTION_PAYLOAD})


class TestLabelInjection:
    def test_insert_node_label_backtick_escaped(self):
        node = Node(id="a", labels=[IDENTIFIER_INJECTION_PAYLOAD], properties={})
        query, _ = insert_node(node)
        # Label is wrapped in backticks with the embedded backtick doubled.
        assert "`X``}) MATCH (m) DETACH DELETE m //`" in query

    def test_insert_edge_label_backtick_escaped(self):
        src = Node(id="a", labels=["Person"], properties={})
        dest = Node(id="b", labels=["Person"], properties={})
        edge = Edge(
            label=IDENTIFIER_INJECTION_PAYLOAD,
            properties={},
            node_src=src,
            node_dest=dest,
        )
        query, _ = insert_edge(edge)
        assert "`X``}) MATCH (m) DETACH DELETE m //`" in query
        # Legit labels still present, backticked.
        assert "`Person`" in query

    def test_update_node_match_label_escaped(self):
        query, _ = update_node(IDENTIFIER_INJECTION_PAYLOAD, "a", ["a"], {"a.x": "1"})
        assert "`X``}) MATCH (m) DETACH DELETE m //`" in query


class TestEscapeIdentifierHelper:
    def test_wraps_and_doubles_backticks(self):
        assert _escape_identifier("pageRank") == "`pageRank`"
        assert _escape_identifier("a`b") == "`a``b`"

    def test_rejects_non_string(self):
        with pytest.raises(ValueError):
            _escape_identifier(123)


# A property KEY (attribute name) crafted to break out of the ``{key: $N}``
# map and inject a destructive clause, with the trailing ``//`` swallowing the
# rest of the generated query. This is the exact primitive from the finding.
KEY_INJECTION_PAYLOAD = "x: 1}) MATCH (m) DETACH DELETE m //"


class TestPropertyKeyInjection:
    """Prove node/edge property KEYS cannot inject openCypher syntax.

    Property values and node IDs are parameterized ($N); these tests cover the
    remaining key (attribute name) side, at every singular write/update sink.
    """

    def test_insert_node_property_key_backtick_escaped(self):
        node = Node(id="a", labels=["Person"], properties={KEY_INJECTION_PAYLOAD: 1})
        query, _ = insert_node(node)
        # The malicious key is confined inside a backtick identifier: the payload
        # appears backtick-wrapped, and there is no bare `}) MATCH` breakout.
        assert f"`{KEY_INJECTION_PAYLOAD}`" in query
        assert "1}) MATCH (m) DETACH DELETE m //`" in query  # inside backticks
        # The dangerous unquoted breakout must NOT appear.
        assert "{x: 1}) MATCH (m) DETACH DELETE m //" not in query
        # ~id is present and single-escaped (not double-escaped).
        assert "`~id`" in query
        assert "``~id``" not in query

    def test_insert_edge_property_key_backtick_escaped(self):
        src = Node(id="a", labels=["Person"], properties={})
        dest = Node(id="b", labels=["Person"], properties={})
        edge = Edge(
            label="FRIEND_WITH",
            properties={KEY_INJECTION_PAYLOAD: 1},
            node_src=src,
            node_dest=dest,
        )
        query, _ = insert_edge(edge)
        assert f"`{KEY_INJECTION_PAYLOAD}`" in query
        assert "{x: 1}) MATCH (m) DETACH DELETE m //" not in query

    def test_update_node_set_key_escaped_ref_preserved(self):
        # A dotted SET key: only the property segment is escaped, the code-
        # generated reference prefix (``a``) stays bare so the SET is valid.
        node_query = update_node(
            "Person", "a", ["Alice"], {f"a.{KEY_INJECTION_PAYLOAD}": "1"}
        )
        query = node_query[0]
        assert f"a.`{KEY_INJECTION_PAYLOAD}`" in query
        assert "SET a.x: 1}) MATCH (m) DETACH DELETE m //" not in query

    def test_update_edge_set_and_where_keys_escaped(self):
        src = Node(id="a", labels=["Person"], properties={})
        dest = Node(id="b", labels=["Person"], properties={})
        edge = Edge(label="FRIEND_WITH", properties={}, node_src=src, node_dest=dest)
        query, _ = update_edge(
            "a",
            "r",
            edge,
            "b",
            {f"a.{KEY_INJECTION_PAYLOAD}": "Alice"},  # WHERE filter key
            {f"r.{KEY_INJECTION_PAYLOAD}": "1"},  # SET key
        )
        assert f"a.`{KEY_INJECTION_PAYLOAD}`" in query
        assert f"r.`{KEY_INJECTION_PAYLOAD}`" in query
        assert "MATCH (m) DETACH DELETE m //}" not in query
        assert "= 1}) MATCH (m) DETACH DELETE m //" not in query


class TestEscapePropertyKeyHelpers:
    def test_plain_key_wrapped_and_doubled(self):
        assert _escape_property_key("name") == "`name`"
        assert _escape_property_key("~id") == "`~id`"
        # An embedded backtick is doubled so it can't terminate the identifier.
        assert (
            _escape_property_key("a`) DETACH DELETE n //")
            == "`a``) DETACH DELETE n //`"
        )

    def test_path_escapes_only_property_segment(self):
        assert _escape_property_path("a.age") == "a.`age`"
        assert _escape_property_path("r.since") == "r.`since`"

    def test_path_passes_through_structural_predicate(self):
        # id(n) carries no property name — left intact so the WHERE stays valid.
        assert _escape_property_path("id(n)") == "id(n)"

    def test_path_neutralizes_malicious_predicate_lookalike(self):
        # A key that merely starts like id(...) but carries a payload does NOT
        # match the strict shape and is escaped wholesale.
        payload = "id(n) }) MATCH (m) DETACH DELETE m //"
        assert _escape_property_path(payload) == f"`{payload}`"

    def test_path_neutralizes_malicious_bare_key(self):
        payload = "x }) DETACH DELETE m //"
        assert _escape_property_path(payload) == f"`{payload}`"
