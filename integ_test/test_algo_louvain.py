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
def graph():
    """Create a new undirected graph"""
    return nx.Graph()

@pytest.fixture
def test_graph():
    """Create test graph with community structure and weights"""
    g = nx.Graph()
    # Create two communities with weighted edges
    # Community 1: A-B-C triangle
    g.add_edge("A", "B", custom_weight=1)
    g.add_edge("B", "C", custom_weight=1)
    g.add_edge("C", "A", custom_weight=1)
    # Community 2: D-E-F triangle
    g.add_edge("D", "E", custom_weight=1)
    g.add_edge("E", "F", custom_weight=1)
    g.add_edge("F", "D", custom_weight=1)
    # Bridge between communities (weaker connection)
    g.add_edge("C", "D", custom_weight=0.5)
    return g

class TestLouvain:
    BACKEND = "neptune"

    def test_louvain_communities_basic(self, test_graph):
        """Test basic Louvain communities"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND)
        
        assert isinstance(result, list)
        assert len(result) > 0
        
        # Verify communities are sets
        for community in result:
            assert isinstance(community, set)
            assert len(community) > 0

    def test_louvain_communities_with_weight(self, test_graph):
        """Test Louvain communities with weight parameter"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 weight="custom_weight")
        
        assert isinstance(result, list)
        assert len(result) > 0

    def test_louvain_communities_with_max_level(self, test_graph):
        """Test Louvain communities with max_level parameter"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 max_level=100)
        
        assert isinstance(result, list)
        assert len(result) > 0

    def test_louvain_communities_with_threshold(self, test_graph):
        """Test Louvain communities with threshold parameter"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 threshold=0.5)
        
        assert isinstance(result, list)
        assert len(result) > 0

    def test_louvain_communities_with_level_tolerance(self, test_graph):
        """Test Louvain communities with level_tolerance parameter"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 level_tolerance=0.5)
        
        assert isinstance(result, list)
        assert len(result) > 0

    def test_louvain_communities_with_max_iterations(self, test_graph):
        """Test Louvain communities with max_iterations parameter"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 max_iterations=100)
        
        assert isinstance(result, list)
        assert len(result) > 0

    def test_louvain_communities_with_concurrency(self, test_graph):
        """Test Louvain communities with concurrency parameter"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 concurrency=1)
        
        assert isinstance(result, list)
        assert len(result) > 0

    def test_louvain_communities_with_edge_labels(self, test_graph):
        """Test Louvain communities with edge_labels parameter"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 edge_labels=["RELATES_TO"])
        
        assert isinstance(result, list)
        assert len(result) > 0

    def test_louvain_communities_mutation(self, test_graph, neptune_graph):
        """Test Louvain communities with write_property (mutation)"""
        result = nx.community.louvain_communities(test_graph, backend=self.BACKEND, 
                                                 write_property="communities")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        
        # Verify nodes exist after mutation
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None

    def test_louvain_communities_empty_graph(self, graph):
        """Test Louvain communities on empty graph"""
        result = nx.community.louvain_communities(graph, backend=self.BACKEND)
        
        assert isinstance(result, list)
        assert len(result) == 0

    def test_louvain_communities_single_node(self, graph):
        """Test Louvain communities on single node graph"""
        graph.add_node("A")
        result = nx.community.louvain_communities(graph, backend=self.BACKEND)
        
        assert isinstance(result, list)
        assert len(result) == 1
        assert {"A"} in result
