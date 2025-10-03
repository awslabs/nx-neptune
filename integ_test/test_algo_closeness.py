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
    """Create test graph with airport-like structure"""
    g = nx.Graph()
    # Create a simple connected graph for testing
    airports = ["YVR", "LAX", "JFK", "ORD", "DFW"]
    g.add_nodes_from(airports)
    # Add edges to create a connected graph
    g.add_edge("YVR", "LAX")
    g.add_edge("LAX", "JFK")
    g.add_edge("JFK", "ORD")
    g.add_edge("ORD", "DFW")
    g.add_edge("DFW", "YVR")  # Create cycle
    g.add_edge("LAX", "ORD")  # Add shortcut
    return g

class TestCloseness:
    BACKEND = "neptune"

    def test_closeness_centrality_basic(self, test_graph):
        """Test basic closeness centrality"""
        result = nx.closeness_centrality(test_graph, backend=self.BACKEND)
        
        assert isinstance(result, dict)
        assert len(result) == 5  # 5 airports
        assert all(isinstance(v, float) for v in result.values())
        assert all(0 <= v <= 1 for v in result.values())

    def test_closeness_centrality_selected_node(self, test_graph):
        """Test closeness centrality for selected node"""
        result = nx.closeness_centrality(test_graph, backend=self.BACKEND, u="YVR")
        
        assert isinstance(result, float)
        assert 0 <= result <= 1

    def test_closeness_centrality_mutation(self, test_graph, neptune_graph):
        """Test closeness centrality with write_property (mutation)"""
        nx.closeness_centrality(test_graph, backend=self.BACKEND, write_property="ccScore")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        
        # Verify nodes exist after mutation
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None

    def test_closeness_centrality_empty_graph(self, graph):
        """Test closeness centrality on empty graph"""
        result = nx.closeness_centrality(graph, backend=self.BACKEND)
        
        assert isinstance(result, dict)
        assert len(result) == 0

    def test_closeness_centrality_single_node(self, graph):
        """Test closeness centrality on single node graph"""
        graph.add_node("A")
        result = nx.closeness_centrality(graph, backend=self.BACKEND)
        
        assert isinstance(result, dict)
        assert len(result) == 1
        assert "A" in result
