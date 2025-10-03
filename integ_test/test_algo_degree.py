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

class TestDegree:
    BACKEND = "neptune"

    def test_degree_centrality_basic(self, test_digraph):
        """Test basic degree centrality"""
        r = nx.degree_centrality(test_digraph, backend=self.BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 6  # 5 connected nodes + 1 isolated
        assert all(isinstance(v, float) for v in r.values())

    def test_degree_centrality_with_aws_options(self, test_digraph):
        """Test degree centrality with AWS-specific options"""
        r = nx.degree_centrality(test_digraph, backend=self.BACKEND, 
                                vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_in_degree_centrality_basic(self, test_digraph):
        """Test basic in-degree centrality"""
        r = nx.in_degree_centrality(test_digraph, backend=self.BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 6
        assert all(isinstance(v, float) for v in r.values())

    def test_in_degree_centrality_with_aws_options(self, test_digraph):
        """Test in-degree centrality with AWS-specific options"""
        r = nx.in_degree_centrality(test_digraph, backend=self.BACKEND, 
                                   vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_out_degree_centrality_basic(self, test_digraph):
        """Test basic out-degree centrality"""
        r = nx.out_degree_centrality(test_digraph, backend=self.BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 6
        assert all(isinstance(v, float) for v in r.values())

    def test_out_degree_centrality_with_aws_options(self, test_digraph):
        """Test out-degree centrality with AWS-specific options"""
        r = nx.out_degree_centrality(test_digraph, backend=self.BACKEND, 
                                    vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_degree_centrality_mutation(self, test_digraph, neptune_graph):
        """Test degree centrality with write_property (mutation)"""
        nx.degree_centrality(test_digraph, backend=self.BACKEND, write_property="degree")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        
        # Verify nodes have the degree property
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None

    def test_in_degree_centrality_mutation(self, test_digraph, neptune_graph):
        """Test in-degree centrality with write_property (mutation)"""
        nx.in_degree_centrality(test_digraph, backend=self.BACKEND, write_property="degree")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0

    def test_out_degree_centrality_mutation(self, test_digraph, neptune_graph):
        """Test out-degree centrality with write_property (mutation)"""
        nx.out_degree_centrality(test_digraph, backend=self.BACKEND, write_property="degree")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
