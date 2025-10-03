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

import pytest

import networkx as nx
from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID

pytestmark = pytest.mark.order("after")

BACKEND = "neptune"

@pytest.fixture(scope="module", autouse=True)
def neptune_graph():
    """Setup Neptune graph for testing"""
    if not NETWORKX_GRAPH_ID:
        pytest.skip('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
    
    g = nx.Graph()
    na_graph = NeptuneGraph.from_config(graph=g)

    """Clear graph before each test"""
    na_graph.clear_graph()
    return na_graph

@pytest.fixture
def digraph():
    """Create a new directed graph"""
    digraph = nx.DiGraph()
    digraph.add_node("Alice")
    digraph.add_node("Bob")
    digraph.add_node("Carl")
    digraph.add_edge("Alice", "Bob")
    digraph.add_edge("Alice", "Carl")

    return digraph

@pytest.fixture
def graph():
    """Create a new undirected graph"""
    graph = nx.Graph()
    graph.add_node("Alice", age=30, role="Engineer")
    graph.add_node("Bob", age=25, role="Designer")
    graph.add_node("Carl", age=35, role="Manager")
    graph.add_edge("Alice", "Bob", weight=5, relationship="colleague")
    graph.add_edge("Alice", "Carl", weight=2, relationship="manager")

    return graph

class TestBFS:
    def test_bfs_edges_directed_graph(self, digraph):
        """Test BFS edges on directed graph with named nodes"""
        r = list(nx.bfs_edges(digraph, "Alice", backend=BACKEND))
        
        assert isinstance(r, list)
        assert len(r) == 2
        assert ["Alice", "Bob"] in r
        assert ["Alice", "Carl"] in r

    def test_bfs_edges_reverse_with_depth_limit(self, digraph):
        """Test BFS edges with reverse=True and depth_limit=1"""
        digraph.add_node("Alice")
        digraph.add_node("Bob")
        digraph.add_node("Carl")
        digraph.add_edge("Alice", "Bob")
        digraph.add_edge("Alice", "Carl")

        r = list(nx.bfs_edges(digraph, "Bob", backend=BACKEND, reverse=True, depth_limit=1))
        
        assert isinstance(r, list)
        assert len(r) == 1
        assert ["Bob", "Alice"] in r

    def test_bfs_layers_multiple_sources(self, digraph):
        """Test BFS layers with multiple sources"""
        r = list(nx.bfs_layers(digraph, ["Alice", "Bob"], backend=BACKEND))

        assert "Alice" in r[0]
        assert "Bob" in r[0]
        assert "Carl" in r[1]

    def test_bfs_edges_path_graph_3_nodes(self):
        """Test BFS edges on 3-node path graph"""
        G = nx.path_graph(3)

        r = list(nx.bfs_edges(G, "0", backend=BACKEND))
        
        assert isinstance(r, list)
        assert len(r) == 2
        assert ["0", "1"] in r
        assert ["1", "2"] in r

    def test_bfs_edges_with_depth_limit_1(self):
        """Test BFS edges with depth_limit=1"""
        G = nx.path_graph(3)

        r = list(nx.bfs_edges(G, source="0", depth_limit=1, backend=BACKEND))
        
        assert len(r) == 1
        assert ["0", "1"] in r

    def test_bfs_edges_from_middle_node(self):
        """Test BFS edges starting from the middle node"""
        G = nx.path_graph(3)

        r = list(nx.bfs_edges(G, source="1", backend=BACKEND))
        
        assert len(r) == 2
        assert ["1", "0"] in r
        assert ["1", "2"] in r

    def test_bfs_edges_reverse_true(self):
        """Test BFS edges with reverse=True"""
        G = nx.path_graph(3)

        r = list(nx.bfs_edges(G, source="1", reverse=True, backend=BACKEND))
        
        assert isinstance(r, list)
        assert len(r) == 2
        assert ["1", "0"] in r
        assert ["1", "2"] in r

    def test_bfs_edges_reverse_false(self):
        """Test BFS edges with reverse=False"""
        G = nx.path_graph(3)

        r = list(nx.bfs_edges(G, source="1", reverse=False, backend=BACKEND))
        
        assert isinstance(r, list)
        assert len(r) == 2
        assert ["1", "0"] in r
        assert ["1", "2"] in r

    def test_bfs_edges_12_node_path(self):
        """Test BFS edges on 12-node path graph"""
        G = nx.path_graph(12)

        r = list(nx.bfs_edges(G, source="6", backend=BACKEND))
        
        assert isinstance(r, list)
        assert len(r) == 11
        
        result_tuples = [tuple(edge) for edge in r]
        # Check forward edges
        for i in [6, 7, 8, 9, 10]:
            assert (str(i), str(i+1)) in result_tuples
        # Check backward edges
        for i in [6, 5, 4, 3, 2, 1]:
            assert (str(i), str(i-1)) in result_tuples

    def test_bfs_edges_with_aws_options(self, graph):
        """Test BFS edges with AWS-specific options"""
        r = list(nx.bfs_edges(graph, source="Alice", backend=BACKEND,
                             vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0))
        
        assert isinstance(r, list)
        assert len(r) == 2
        assert ["Alice", "Bob"] in r
        assert ["Alice", "Carl"] in r

    def test_descendants_at_distance(self, graph):
        """Test descendants_at_distance function"""
        r = nx.descendants_at_distance(graph, backend=BACKEND, source="Alice", distance=1)
        
        assert r == {'Bob', 'Carl'}

    def test_descendants_at_distance_with_aws_options(self, graph):
        """Test descendants_at_distance with AWS-specific options"""
        r = nx.descendants_at_distance(graph, backend=BACKEND, source="Alice", distance=1, 
                                     vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
        
        assert r == {'Bob', 'Carl'}

    def test_bfs_layers_path_graph(self):
        """Test BFS layers on path graph"""
        g = nx.path_graph(5)
        
        result = list(nx.bfs_layers(g, backend=BACKEND, sources=["1", "4"]))
        
        assert set(result[0]) == {'4', '1'}
        assert set(result[1]) == {'2', '3', '0'}

    def test_bfs_layers_with_aws_options(self):
        """Test BFS layers with AWS-specific options"""
        g = nx.path_graph(5)
        
        result = list(nx.bfs_layers(g, backend=BACKEND, sources=["1", "4"], 
                                   edge_labels=["RELATES_TO"], traversal_direction="both", concurrency=0))
        
        assert set(result[0]) == {'4', '1'}
        assert set(result[1]) == {'2', '3', '0'}
