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
    """Create test graph with community structure"""
    g = nx.Graph()
    # Create two communities
    # Community 1: A-B-C triangle
    g.add_edge("A", "B")
    g.add_edge("B", "C")
    g.add_edge("C", "A")
    # Community 2: D-E-F triangle
    g.add_edge("D", "E")
    g.add_edge("E", "F")
    g.add_edge("F", "D")
    # Bridge between communities
    g.add_edge("C", "D")
    return g

class TestLPA:
    BACKEND = "neptune"

    def test_label_propagation_communities_basic(self, test_graph):
        """Test basic label propagation communities"""
        result = nx.community.label_propagation_communities(test_graph, backend=self.BACKEND)
        
        communities = list(result)
        assert isinstance(communities, list)
        assert len(communities) > 0
        
        # Verify communities are sets
        for community in communities:
            assert isinstance(community, set)
            assert len(community) > 0

    def test_fast_label_propagation_communities_basic(self, test_graph):
        """Test fast label propagation communities"""
        result = nx.community.fast_label_propagation_communities(test_graph, backend=self.BACKEND)
        
        communities = list(result)
        assert isinstance(communities, list)
        assert len(communities) > 0
        
        for community in communities:
            assert isinstance(community, set)
            assert len(community) > 0

    def test_asyn_lpa_communities_basic(self, test_graph):
        """Test asynchronous label propagation communities"""
        result = nx.community.asyn_lpa_communities(test_graph, backend=self.BACKEND)
        
        communities = list(result)
        assert isinstance(communities, list)
        assert len(communities) > 0
        
        for community in communities:
            assert isinstance(community, set)
            assert len(community) > 0

    def test_label_propagation_communities_mutation(self, test_graph, neptune_graph):
        """Test label propagation communities with write_property (mutation)"""
        result = nx.community.label_propagation_communities(test_graph, backend=self.BACKEND, 
                                                           write_property="communities")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None

    def test_fast_label_propagation_communities_mutation(self, test_graph, neptune_graph):
        """Test fast label propagation communities with write_property (mutation)"""
        result = nx.community.fast_label_propagation_communities(test_graph, backend=self.BACKEND, 
                                                                write_property="communities")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0

    def test_asyn_lpa_communities_mutation(self, test_graph, neptune_graph):
        """Test asynchronous label propagation communities with write_property (mutation)"""
        result = nx.community.asyn_lpa_communities(test_graph, backend=self.BACKEND, 
                                                  write_property="communities")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0

    def test_label_propagation_empty_graph(self, graph):
        """Test label propagation on empty graph"""
        result = nx.community.label_propagation_communities(graph, backend=self.BACKEND)
        
        communities = list(result)
        assert isinstance(communities, list)
        assert len(communities) == 0

    def test_label_propagation_single_node(self, graph):
        """Test label propagation on single node graph"""
        graph.add_node("A")
        result = nx.community.label_propagation_communities(graph, backend=self.BACKEND)
        
        communities = list(result)
        assert isinstance(communities, list)
        assert len(communities) == 1
        assert {"A"} in communities
