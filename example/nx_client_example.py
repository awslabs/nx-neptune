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
import networkx as nx

from nx_neptune import NeptuneGraph, NETWORKX_GRAPH_ID
from nx_neptune.clients import Node, Edge

""" 
This is an sample script which demonstrate how class NeptuneGraph 
can be used to perform basic CRUD operation against an existing Neptune Analytics graph.
"""

"""Read and load graphId from environment variable. """
if not NETWORKX_GRAPH_ID:
    raise Exception('Environment Variable NETWORKX_GRAPH_ID is not defined')

"""Clear the Neptune Analytics graph"""
nx_graph = nx.Graph()
g = NeptuneGraph.from_config(graph=nx_graph)
g.clear_graph()

"""Populate the dataset by inserting nodes into the graph"""
alice = Node(id='Alice', labels=['Person'], properties={'age': 24})
bob = Node(id='Bob', labels=['Person'], properties={'hair': 'brown'})
edge = Edge(label='FRIEND_WITH', properties={}, node_src=alice, node_dest=bob)

g.add_node(alice)
g.add_node(bob)

g.add_edge(edge)

g.update_node(
    match_labels='Person',
    ref_name='a',
    node=alice,
    properties_set={'a.hair': 'black'})

g.update_nodes(
    match_labels='Person',
    ref_name='a',
    nodes=[alice, bob],
    properties_set={'a.career': 'Flight Instructor'})

"""Update an edge"""
g.update_edges('a', 'r', 'b',
               edge,
               {'a.name': 'Alice', 'b.name': 'Bob'},
               {'r.since': 1997})

""" To demonstrate how to print existing nodes and edges which exist on the graph """

"""List all nodes"""
for item in g.get_all_nodes():
    print(Node.from_neptune_response(item))

"""List all edges"""
for item in g.get_all_edges():
    print(item)
