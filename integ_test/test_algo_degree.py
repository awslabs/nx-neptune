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
import os

import pytest
from dotenv import load_dotenv
load_dotenv()

import networkx as nx
from nx_neptune import Node
from utils.test_utils import BACKEND, neptune_graph

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

    def test_degree_centrality_basic(self, test_digraph):
        """Test basic degree centrality"""
        r = nx.degree_centrality(test_digraph, backend=BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 6  # 5 connected nodes + 1 isolated
        assert all(isinstance(v, float) for v in r.values())

    def test_degree_centrality_with_aws_options(self, test_digraph):
        """Test degree centrality with AWS-specific options"""
        r = nx.degree_centrality(test_digraph, backend=BACKEND, 
                                vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_in_degree_centrality_basic(self, test_digraph):
        """Test basic in-degree centrality"""
        r = nx.in_degree_centrality(test_digraph, backend=BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 6
        assert all(isinstance(v, float) for v in r.values())

    def test_in_degree_centrality_with_aws_options(self, test_digraph):
        """Test in-degree centrality with AWS-specific options"""
        r = nx.in_degree_centrality(test_digraph, backend=BACKEND, 
                                   vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_out_degree_centrality_basic(self, test_digraph):
        """Test basic out-degree centrality"""
        r = nx.out_degree_centrality(test_digraph, backend=BACKEND)
        
        assert isinstance(r, dict)
        assert len(r) == 6
        assert all(isinstance(v, float) for v in r.values())

    def test_out_degree_centrality_with_aws_options(self, test_digraph):
        """Test out-degree centrality with AWS-specific options"""
        r = nx.out_degree_centrality(test_digraph, backend=BACKEND, 
                                    vertex_label="Node", edge_labels=["RELATES_TO"], concurrency=0)
        
        assert isinstance(r, dict)
        assert len(r) == 6

    def test_degree_centrality_mutation(self, test_digraph, neptune_graph):
        """Test degree centrality with write_property (mutation)"""
        nx.degree_centrality(test_digraph, backend=BACKEND, write_property="degree")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        
        # Verify nodes have the degree property
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None
            assert "degree" in node.properties

    def test_in_degree_centrality_mutation(self, test_digraph, neptune_graph):
        """Test in-degree centrality with write_property (mutation)"""
        nx.in_degree_centrality(test_digraph, backend=BACKEND, write_property="degree")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        # Verify nodes have the degree property
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None
            assert "degree" in node.properties

    def test_out_degree_centrality_mutation(self, test_digraph, neptune_graph):
        """Test out-degree centrality with write_property (mutation)"""
        nx.out_degree_centrality(test_digraph, backend=BACKEND, write_property="degree")
        
        nodes = neptune_graph.get_all_nodes()[:10]
        assert len(nodes) > 0
        # Verify nodes have the degree property
        for item in nodes:
            node = Node.from_neptune_response(item)
            assert node is not None
            assert "degree" in node.properties
