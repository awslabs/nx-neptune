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
from dotenv import load_dotenv
load_dotenv()

import networkx as nx
from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID, Node

pytestmark = pytest.mark.order("after")

@pytest.fixture(scope="module")
def neptune_graph():
    """Setup Neptune graph for testing"""
    if not NETWORKX_GRAPH_ID:
        pytest.skip('Environment Variable "NETWORKX_GRAPH_ID" is not defined')
    
    g = nx.Graph()
    na_graph = NeptuneGraph.from_config(graph=g)
    return na_graph

@pytest.fixture(autouse=True)
def clear_graph(neptune_graph):
    """Clear graph before each test"""
    neptune_graph.clear_graph()

@pytest.fixture
def digraph():
    """Create a new directed graph"""
    return nx.DiGraph()

@pytest.fixture
def test_digraph():
    """Create test directed graph with specific structure"""
    g = nx.DiGraph()
    nodes = ['A', 'B', 'C', 'D', 'E']
    g.add_nodes_from(nodes)
    g.add_edge('A', 'B')
    g.add_edge('B', 'C')
    g.add_edge('C', 'D')
    g.add_edge('D', 'E')
    g.add_edge('E', 'C', weight=1)
    g.add_node("X(DCd)")
    return g

class TestPageRank:
    BACKEND = "neptune"

    def test_pagerank_basic(self, test_digraph):
        """Test basic PageRank"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 6  # 5 connected nodes + 1 isolated
        assert all(isinstance(v, float) for v in r.values())
        assert all(v >= 0 for v in r.values())

    def test_pagerank_with_vertex_label(self, test_digraph):
        """Test PageRank with vertex_label option"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, vertex_label="A")
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_pagerank_with_edge_labels(self, test_digraph):
        """Test PageRank with edge_labels option"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, edge_labels=["RELATES_TO"])
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_pagerank_with_concurrency(self, test_digraph):
        """Test PageRank with concurrency option"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, concurrency=0)
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_pagerank_with_traversal_direction(self, test_digraph):
        """Test PageRank with traversal_direction option"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, traversal_direction="inbound")
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_pagerank_with_edge_weights(self, test_digraph):
        """Test PageRank with edge weight options"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, 
                       edge_weight_type="int", edge_weight_property="weight")
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_pagerank_with_source_nodes_weights(self, test_digraph):
        """Test PageRank with source_nodes and source_weights"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, 
                       source_nodes=["A", "B"], source_weights=[1, 1.5])
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_pagerank_with_personalization(self, test_digraph):
        """Test PageRank with personalization"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, 
                       personalization={"A": 0.1, "B": 100})
        
        assert isinstance(r, dict)
        assert len(r) == 6
        # B should have higher rank due to personalization
        assert r["B"] > r["A"]

    def test_pagerank_mutation(self, test_digraph, neptune_graph):
        """Test PageRank with write_property (mutation)"""
        r = nx.pagerank(test_digraph, backend=self.BACKEND, write_property="rank")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        
        # Verify nodes exist after mutation
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None

    def test_pagerank_empty_graph(self, digraph):
        """Test PageRank on empty graph"""
        r = nx.pagerank(digraph, backend=self.BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 0

    def test_pagerank_single_node(self, digraph):
        """Test PageRank on single node graph"""
        digraph.add_node("A")
        r = nx.pagerank(digraph, backend=self.BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 1
        assert "A" in r
        assert r["A"] == 1.0  # Single node gets all the rank
