# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Integration tests for NeptuneGraph CRUD operations."""

import pytest
from nx_neptune import Node, Edge


class TestAddAndGetNodes:

    def test_add_single_node(self, neptune_graph):
        node = Node(id="n1", labels=["Person"], properties={"name": "Alice"})
        neptune_graph.add_node(node)

        nodes = neptune_graph.get_all_nodes()
        assert len(nodes) == 1
        assert nodes[0]["~id"] == "n1"
        assert "Person" in nodes[0]["~labels"]
        assert nodes[0]["~properties"]["name"] == "Alice"

    def test_add_multiple_nodes(self, neptune_graph):
        neptune_graph.clear_graph()
        nodes = [
            Node(id="a", labels=["City"], properties={"pop": 100}),
            Node(id="b", labels=["City"], properties={"pop": 200}),
            Node(id="c", labels=["City"], properties={"pop": 300}),
        ]
        neptune_graph.add_nodes(nodes)

        result = neptune_graph.get_all_nodes()
        ids = {n["~id"] for n in result}
        assert ids == {"a", "b", "c"}

    def test_add_node_no_properties(self, neptune_graph):
        neptune_graph.clear_graph()
        node = Node(id="bare", labels=["Thing"])
        neptune_graph.add_node(node)

        nodes = neptune_graph.get_all_nodes()
        assert len(nodes) == 1
        assert nodes[0]["~id"] == "bare"
        assert "Thing" in nodes[0]["~labels"]

    def test_add_node_multiple_labels(self, neptune_graph):
        neptune_graph.clear_graph()
        node = Node(id="multi", labels=["Person", "Employee"], properties={"dept": "eng"})
        neptune_graph.add_node(node)

        nodes = neptune_graph.get_all_nodes()
        assert len(nodes) == 1
        labels = set(nodes[0]["~labels"])
        assert "Person" in labels
        assert "Employee" in labels


class TestUpdateNodes:

    def test_update_single_node(self, neptune_graph):
        neptune_graph.clear_graph()
        node = Node(id="u1", labels=["Person"], properties={"name": "Bob"})
        neptune_graph.add_node(node)

        neptune_graph.update_node(
            match_labels="Person",
            ref_name="n",
            node=node,
            properties_set={"n.name": "Robert"},
        )

        nodes = neptune_graph.get_all_nodes()
        assert nodes[0]["~id"] == "u1"
        assert "Person" in nodes[0]["~labels"]
        assert nodes[0]["~properties"]["name"] == "Robert"

    def test_update_multiple_nodes(self, neptune_graph):
        neptune_graph.clear_graph()
        nodes = [
            Node(id="m1", labels=["Item"], properties={"status": "new"}),
            Node(id="m2", labels=["Item"], properties={"status": "new"}),
        ]
        neptune_graph.add_nodes(nodes)

        neptune_graph.update_nodes(
            match_labels="Item",
            ref_name="n",
            nodes=nodes,
            properties_set={"n.status": "updated"},
        )

        result = neptune_graph.get_all_nodes()
        for n in result:
            assert "Item" in n["~labels"]
            assert n["~properties"]["status"] == "updated"


class TestDeleteNodes:

    def test_delete_node(self, neptune_graph):
        neptune_graph.clear_graph()
        node = Node(id="d1", labels=["Temp"])
        neptune_graph.add_node(node)
        assert len(neptune_graph.get_all_nodes()) == 1

        neptune_graph.delete_nodes(node)
        assert len(neptune_graph.get_all_nodes()) == 0

    def test_delete_nonexistent_node_no_effect(self, neptune_graph):
        neptune_graph.clear_graph()
        node = Node(id="keep", labels=["Persist"])
        neptune_graph.add_node(node)

        wrong_node = Node(id="nonexistent", labels=["Persist"])
        neptune_graph.delete_nodes(wrong_node)

        nodes = neptune_graph.get_all_nodes()
        assert len(nodes) == 1
        assert nodes[0]["~id"] == "keep"


class TestAddAndGetEdges:

    def test_add_single_edge(self, neptune_graph):
        neptune_graph.clear_graph()
        src = Node(id="e_src", labels=["Person"])
        dst = Node(id="e_dst", labels=["Person"])
        neptune_graph.add_nodes([src, dst])

        edge = Edge(node_src=src, node_dest=dst, label="KNOWS", properties={"since": "2024"})
        neptune_graph.add_edge(edge)

        edges = neptune_graph.get_all_edges()
        assert len(edges) == 1
        assert edges[0]["~start"] == "e_src"
        assert edges[0]["~end"] == "e_dst"

    def test_add_multiple_edges(self, neptune_graph):
        neptune_graph.clear_graph()
        a = Node(id="ea", labels=["N"])
        b = Node(id="eb", labels=["N"])
        c = Node(id="ec", labels=["N"])
        neptune_graph.add_nodes([a, b, c])

        edges = [
            Edge(node_src=a, node_dest=b, label="R1"),
            Edge(node_src=b, node_dest=c, label="R2"),
        ]
        neptune_graph.add_edges(edges)

        result = neptune_graph.get_all_edges()
        assert len(result) == 2
        edge_pairs = {(e["~start"], e["~end"]) for e in result}
        assert ("ea", "eb") in edge_pairs
        assert ("eb", "ec") in edge_pairs

    def test_edge_with_properties(self, neptune_graph):
        neptune_graph.clear_graph()
        src = Node(id="ps", labels=["X"])
        dst = Node(id="pd", labels=["X"])
        neptune_graph.add_nodes([src, dst])

        edge = Edge(
            node_src=src,
            node_dest=dst,
            label="HAS",
            properties={"weight": 42, "active": True, "tag": "test"},
        )
        neptune_graph.add_edge(edge)

        edges = neptune_graph.get_all_edges()
        assert len(edges) == 1
        props = edges[0]["~properties"]
        assert props["weight"] == 42
        assert props["active"] is True
        assert props["tag"] == "test"


class TestDeleteEdges:

    def test_delete_edge(self, neptune_graph):
        neptune_graph.clear_graph()
        src = Node(id="ds", labels=["N"])
        dst = Node(id="dd", labels=["N"])
        neptune_graph.add_nodes([src, dst])

        edge = Edge(node_src=src, node_dest=dst, label="TEMP")
        neptune_graph.add_edge(edge)
        assert len(neptune_graph.get_all_edges()) == 1

        neptune_graph.delete_edges(edge)
        assert len(neptune_graph.get_all_edges()) == 0
        # Nodes should still exist
        assert len(neptune_graph.get_all_nodes()) == 2


class TestClearGraph:

    def test_clear_removes_everything(self, neptune_graph):
        src = Node(id="cs", labels=["N"])
        dst = Node(id="cd", labels=["N"])
        neptune_graph.add_nodes([src, dst])
        neptune_graph.add_edge(Edge(node_src=src, node_dest=dst, label="R"))

        assert len(neptune_graph.get_all_nodes()) > 0

        neptune_graph.clear_graph()
        assert len(neptune_graph.get_all_nodes()) == 0
        assert len(neptune_graph.get_all_edges()) == 0


class TestExecuteCall:

    def test_raw_cypher_query(self, neptune_graph):
        neptune_graph.clear_graph()
        node = Node(id="raw1", labels=["Test"])
        neptune_graph.add_node(node)

        result = neptune_graph.execute_call("MATCH (n) RETURN count(n) AS cnt")
        assert result[0]["cnt"] == 1

    def test_parameterized_query(self, neptune_graph):
        neptune_graph.clear_graph()
        node = Node(id="param1", labels=["Test"], properties={"val": 99})
        neptune_graph.add_node(node)

        result = neptune_graph.execute_call(
            "MATCH (n) WHERE n.val = $v RETURN n.`~id` AS id",
            {"v": 99},
        )
        assert result[0]["id"] == "param1"
