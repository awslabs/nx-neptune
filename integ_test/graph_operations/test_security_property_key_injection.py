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
"""Integration test: openCypher injection via node/edge property KEYS.

Companion to ``test_security_injection.py`` (which covers property *values* and
node *IDs* — the inputs that ride the Neptune Analytics ``parameters`` map and
are therefore bound out-of-band). Property *keys* (attribute names) have no
parameter-map channel: openCypher has no ``$``-placeholder for an identifier
position, so a key is interpolated into the compiled query string and is made
safe by client-side backtick-escaping in ``ParameterMapBuilder.read_map`` /
``_escape_property_key`` / ``_escape_property_path``.

This test proves, against a real Neptune Analytics instance, that a malicious
attribute NAME cannot break out of the query:

  1. A canary node survives every property-key injection attempt (graph not
     destroyed / mutated by an injected clause).
  2. Each injection payload is stored as a *literal property name*, not executed.
  3. Legitimate keys still work — plain names, dotted ``ref.prop`` references,
     and the ``id(n)`` structural predicate are interpreted, not corrupted
     (positive controls).

Usage:
    export NETWORKX_GRAPH_ID=g-your-graph-id
    pytest integ_test/graph_operations/test_security_property_key_injection.py -v
"""

import networkx as nx
import pytest

from nx_neptune import NETWORKX_GRAPH_ID, NeptuneGraph
from nx_neptune.clients import Edge, Node

# Payloads used as the property KEY (attribute name). Each tries to close the
# ``{ ... }`` property map / node and append a destructive clause, with a
# trailing ``//`` to comment out the generated remainder.
KEY_INJECTION_PAYLOADS = [
    "x: 1}) MATCH (m) DETACH DELETE m //",
    "y`: 1}) MATCH (m) DETACH DELETE m //",  # embedded backtick breakout attempt
    "z}) DETACH DELETE m //",
    "`~id`}) MATCH (m) DETACH DELETE m //",  # tries to smuggle a fake ~id
]

CANARY_NODE_ID = "canary_property_key_test"


@pytest.fixture(scope="module")
def na_graph():
    if not NETWORKX_GRAPH_ID:
        pytest.skip('Environment Variable "NETWORKX_GRAPH_ID" is not defined')

    g = nx.Graph()
    graph = NeptuneGraph.from_config(graph=g)
    graph.clear_graph()

    canary = Node(
        id=CANARY_NODE_ID, labels=["Canary"], properties={"purpose": "key_injection"}
    )
    graph.add_node(canary)

    yield graph

    graph.clear_graph()


def _canary_alive(na_graph) -> bool:
    results = na_graph.na_client.execute_generic_query(
        "MATCH (n:`Canary`) WHERE id(n) = $id RETURN n",
        parameter_map={"id": CANARY_NODE_ID},
    )
    return len(results) == 1


class TestInsertNodePropertyKeyInjection:
    """A malicious attribute NAME on add_node is stored literally, not executed."""

    def test_insert_node_with_injection_keys(self, na_graph):
        for i, payload in enumerate(KEY_INJECTION_PAYLOADS):
            node = Node(
                id=f"keyinj_node_{i}",
                labels=["KeyInjectionTest"],
                properties={payload: "v"},
            )
            na_graph.add_node(node)

        assert _canary_alive(
            na_graph
        ), "Canary destroyed by node property-key injection"

        # Each payload must come back as a *literal property name* on its node.
        for i, payload in enumerate(KEY_INJECTION_PAYLOADS):
            results = na_graph.na_client.execute_generic_query(
                "MATCH (n) WHERE id(n) = $id RETURN n",
                parameter_map={"id": f"keyinj_node_{i}"},
            )
            assert len(results) == 1
            props = results[0]["n"].get("~properties", {})
            assert payload in props, (
                f"Payload '{payload[:30]}...' was not stored as a literal "
                "property name — it may have been interpreted as query syntax"
            )


class TestInsertEdgePropertyKeyInjection:
    """A malicious attribute NAME on add_edge is stored literally, not executed."""

    def test_insert_edge_with_injection_keys(self, na_graph):
        for i, payload in enumerate(KEY_INJECTION_PAYLOADS):
            src = Node(id=f"keyinj_esrc_{i}", labels=["EdgeKeyTest"], properties={})
            dest = Node(id=f"keyinj_edst_{i}", labels=["EdgeKeyTest"], properties={})
            na_graph.add_node(src)
            na_graph.add_node(dest)
            edge = Edge(
                label="KEY_INJECTION_EDGE",
                properties={payload: "v"},
                node_src=src,
                node_dest=dest,
            )
            na_graph.add_edge(edge)

        assert _canary_alive(
            na_graph
        ), "Canary destroyed by edge property-key injection"

        all_edges = na_graph.get_all_edges()
        seen_prop_names = set()
        for e in all_edges:
            seen_prop_names.update(e.get("~properties", {}).keys())
        for payload in KEY_INJECTION_PAYLOADS:
            assert payload in seen_prop_names, (
                f"Edge payload '{payload[:30]}...' not stored as a literal "
                "property name"
            )


class TestUpdateNodePropertyKeyInjection:
    """A malicious attribute NAME in a dotted SET key (a.<payload>) is escaped:
    only the property segment is quoted, the code-generated ref stays bare, and
    the payload lands as a literal property name."""

    def test_update_node_with_injection_reference_key(self, na_graph):
        payload = KEY_INJECTION_PAYLOADS[0]
        node = Node(id=CANARY_NODE_ID, labels=["Canary"], properties={})
        na_graph.update_node("Canary", "a", node, {f"a.{payload}": "v"})

        assert _canary_alive(na_graph), "Canary destroyed by SET property-key injection"

        results = na_graph.na_client.execute_generic_query(
            "MATCH (n) WHERE id(n) = $id RETURN n",
            parameter_map={"id": CANARY_NODE_ID},
        )
        props = results[0]["n"].get("~properties", {})
        assert payload in props, "Injected SET key not stored as a literal property"


class TestLegitimateKeysStillWork:
    """Positive controls: escaping must not break legitimate identifiers."""

    def test_plain_property_key_roundtrips(self, na_graph):
        node = Node(id="legit_plain", labels=["Legit"], properties={"age": 30})
        na_graph.add_node(node)
        results = na_graph.na_client.execute_generic_query(
            "MATCH (n) WHERE id(n) = $id RETURN n",
            parameter_map={"id": "legit_plain"},
        )
        props = results[0]["n"].get("~properties", {})
        assert props.get("age") == 30

    def test_dotted_reference_key_sets_real_property(self, na_graph):
        # a.age must set a property named 'age' on the matched node — proving the
        # ref.prop path is interpreted (only the prop segment is escaped), not
        # turned into a single quoted `a.age` identifier.
        node = Node(id=CANARY_NODE_ID, labels=["Canary"], properties={})
        na_graph.update_node("Canary", "a", node, {"a.legit_age": "42"})
        results = na_graph.na_client.execute_generic_query(
            "MATCH (n) WHERE id(n) = $id RETURN n",
            parameter_map={"id": CANARY_NODE_ID},
        )
        props = results[0]["n"].get("~properties", {})
        assert (
            props.get("legit_age") == "42"
        ), "Dotted ref.prop key was not interpreted correctly"

    def test_id_predicate_still_matches(self, na_graph):
        # The id(n) structural predicate (used by bfs/descendants filters) must
        # pass through unescaped so WHERE id(n) = ... still matches.
        results = na_graph.na_client.execute_generic_query(
            "MATCH (n) WHERE id(n) = $id RETURN n",
            parameter_map={"id": CANARY_NODE_ID},
        )
        assert len(results) == 1


class TestCanarySurvival:
    def test_canary_survives_all(self, na_graph):
        assert _canary_alive(
            na_graph
        ), "Canary did not survive property-key injection tests"

    def test_graph_not_wiped(self, na_graph):
        assert len(na_graph.get_all_nodes()) > 1, "Graph appears wiped"
