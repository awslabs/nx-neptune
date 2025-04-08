import networkx as nx
from nx_neptune import NeptuneGraph
from nx_neptune.clients import Node, Edge
import logging
import os

""" 
This is an sample script which demonstrate how class NeptuneGraph 
can be used to perform basic CRUD operation against an existing Neptune Analytics graph.
"""

"""Read and load graphId from environment variable. """
graph_id = os.getenv('GRAPH_ID')
if not graph_id:
    raise Exception('Environment Variable GRAPH_ID is not defined')

"""Clear the Neptune Analytics graph"""
nx_graph = nx.Graph()
g = NeptuneGraph(graph=nx_graph)
g.clear_graph()

"""Populate the dataset by inserting nodes into the graph"""
alice = Node(labels=['Person'], properties={'name': 'Alice'})
bob = Node(labels=['Person'], properties={'name': 'Bob'})
edge = Edge(label='FRIEND_WITH', properties={}, node_src=alice, node_dest=bob)

g.add_node(alice)
g.add_node(bob)

g.add_edge(edge)

g.update_nodes(match_labels='a',
               ref_name='a',
               where_filters={'a.name': 'Alice'},
               properties_set={'a.age': '25'})

"""Update an edge"""
g.update_edges('a', 'r', 'b',
               edge,
               {'a.name': 'Alice', 'b.name': 'Bob'},
               {'r.since': 1997})

""" To demonstrate how to print existing nodes and edges which exist on the graph """

"""List all nodes"""
for item in g.get_all_nodes():
    print(item)

"""List all edges"""
for item in g.get_all_edges():
    print(item)
